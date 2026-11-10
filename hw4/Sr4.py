import socket
import struct
import threading
import uuid
from collections import deque

# Server ports are also the stable IDs used to order connections.
HOST = "127.0.0.1"
SERVER_PORTS = (5000, 5001, 5002, 5003, 5004)
BOOTSTRAP_ADDRESSES = [(HOST, port) for port in SERVER_PORTS]
# Allows five chat clients plus their temporary/current RTT probe connections.
MAX_CLIENTS = 15
MAX_USERNAME_BYTES = 255
MAX_PAYLOAD = 65535
SEEN_MESSAGE_LIMIT = 10000

REQ_INFO = 0
REP_INFO = 1
IDENTIFY = 2
SEND_MSG = 3
ECHO = 4

SERVER_CONNECTION = 0
CLIENT_CONNECTION = 1
CLIENT_MESSAGE = 0
SERVER_FLOOD = 1
ECHO_REQUEST = 0
ECHO_REPLY = 1
MESSAGE_NAMES = {
    REQ_INFO: "INFO REQUEST",
    REP_INFO: "INFO REPLY",
    IDENTIFY: "IDENTIFY",
    SEND_MSG: "MESSAGE",
    ECHO: "ECHO",
}
MESSAGE_SIGNS = {
    REQ_INFO: "!@",
    REP_INFO: "@#",
    IDENTIFY: "#$",
    SEND_MSG: "$!",
    ECHO: "%^",
}

# Socket maps hold active links; known_servers also includes offline peers.
servers_dict = {}
clients_dict = {}
known_servers = {}
# Message IDs stop flooded messages from looping or being delivered twice.
seen_messages = set()
seen_message_order = deque()
# Locks keep shared maps and concurrent socket writes consistent.
state_lock = threading.RLock()
send_locks = {}
own_port = None


def log_message(direction, msg_type, subtype):
    """Print a short marker for visible protocol messages."""
    if msg_type in (REQ_INFO, REP_INFO):
        return
    name = MESSAGE_NAMES.get(msg_type, "UNKNOWN")
    sign = MESSAGE_SIGNS.get(msg_type, "?!")
    print(f"{sign} [{direction} {name}:{subtype}]", flush=True)


def recv_exact(conn, size):
    """Read exactly size bytes, or return None if the peer disconnects."""
    data = bytearray()
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data.extend(chunk)
    return bytes(data)


def recv_message(conn):
    """Read and decode one six-byte-header protocol frame."""
    header = recv_exact(conn, 6)
    if header is None:
        return None
    msg_type, subtype, length, sublen = struct.unpack(">BBHH", header)
    data = recv_exact(conn, length) if length else b""
    if data is None:
        return None
    return msg_type, subtype, sublen, data


def send_message(conn, msg_type, subtype, sublen, data):
    """Encode and send one complete frame without interleaving writes."""
    if len(data) > MAX_PAYLOAD:
        raise ValueError(f"Payload exceeds {MAX_PAYLOAD} bytes")
    frame = struct.pack(">BBHH", msg_type, subtype, len(data), sublen) + data
    with state_lock:
        lock = send_locks.setdefault(conn, threading.Lock())
    with lock:
        conn.sendall(frame)
    log_message("SEND", msg_type, subtype)


def server_addresses_payload():
    """Return known server addresses as null-separated host:port entries."""
    with state_lock:
        addresses = dict(known_servers)
        addresses.update({port: HOST for port in servers_dict})
    return b"\0".join(
        f"{host}:{port}".encode() for port, host in sorted(addresses.items())
    )


def broadcast_server_list(exclude_conn=None):
    """Share the current topology with every peer except the source link."""
    data = server_addresses_payload()
    with state_lock:
        peers = list(servers_dict.values())
    for peer in peers:
        if peer is exclude_conn:
            continue
        try:
            send_message(peer, REP_INFO, 0, 0, data)
        except OSError as error:
            print(f"[NETWORK] Could not update a peer's server list: {error}")
            close_connection(peer)


def handle_info_request(conn, subtype):
    """Reply with either the server list or this server's local usernames."""
    if subtype == 0:
        data = server_addresses_payload()
    elif subtype == 1:
        with state_lock:
            data = b"\0".join(name.encode() for name in sorted(clients_dict))
    else:
        return
    send_message(conn, REP_INFO, subtype, 0, data)


def handle_echo(conn, subtype, sublen, data):
    """Reply to an echo request with its ASCII letters shifted by one."""
    if subtype == ECHO_REQUEST and sublen == 0:
        shifted = bytes(
            byte + 1 if 65 <= byte < 90 or 97 <= byte < 122
            else 65 if byte == 90
            else 97 if byte == 122
            else byte
            for byte in data
        )
        send_message(conn, ECHO, ECHO_REPLY, 0, shifted)


def add_known_servers(data):
    """Add valid addresses from a peer and report whether topology changed."""
    changed = False
    with state_lock:
        for entry in data.split(b"\0"):
            if not entry:
                continue
            try:
                host, port_text = entry.decode().rsplit(":", 1)
                host = host.strip()
                port = int(port_text)
            except (UnicodeDecodeError, ValueError):
                print(f"[NETWORK] Ignoring invalid server address: {entry!r}")
                continue
            if host and 1 <= port <= 65535:
                if port not in known_servers:
                    known_servers[port] = host
                    changed = True
    return changed


def remember_message(message_id):
    """Remember a flood ID and evict old IDs when the cache reaches its limit."""
    with state_lock:
        if message_id in seen_messages:
            return False
        seen_messages.add(message_id)
        seen_message_order.append(message_id)
        if len(seen_message_order) > SEEN_MESSAGE_LIMIT:
            seen_messages.discard(seen_message_order.popleft())
        return True


def deliver_to_local_client(recipient, sender, content):
    """Deliver content locally; return False when the recipient is elsewhere."""
    with state_lock:
        client_conn = clients_dict.get(recipient)
    if client_conn is None:
        return False

    routing_prefix = recipient.encode() + b"\0" + sender.encode()
    try:
        send_message(
            client_conn,
            SEND_MSG,
            CLIENT_MESSAGE,
            len(routing_prefix),
            routing_prefix + content,
        )
    except OSError as error:
        print(f"[CLIENT] Could not deliver to {recipient}: {error}")
        close_connection(client_conn)
        return False
    return True


def broadcast_flood(data, sublen, exclude_conn=None):
    """Forward a message flood to connected peers, excluding its sender."""
    with state_lock:
        peers = list(servers_dict.values())
    for peer in peers:
        if peer is exclude_conn:
            continue
        try:
            send_message(peer, SEND_MSG, SERVER_FLOOD, sublen, data)
        except OSError as error:
            print(f"[NETWORK] Could not forward message to a server: {error}")
            close_connection(peer)


def process_server_flood(conn, sublen, data):
    """Deduplicate a received flood, deliver locally, or relay it onward."""
    fields = data.split(b"\0", 3)
    if len(fields) != 4:
        print("[ROUTING] Ignoring malformed server message.")
        return

    recipient_bytes, sender_bytes, message_id, content = fields
    routing_prefix = recipient_bytes + b"\0" + sender_bytes
    if not message_id or sublen != len(routing_prefix):
        print("[ROUTING] Ignoring server message with invalid routing fields.")
        return
    if not remember_message(message_id):
        return

    recipient = recipient_bytes.decode("utf-8", "replace")
    sender = sender_bytes.decode("utf-8", "replace")
    peer_port = next(
        (port for port, peer in servers_dict.items() if peer is conn),
        "unknown",
    )

    try:
        if deliver_to_local_client(recipient, sender, content):
            print(f"[ROUTING] Delivered {sender} -> {recipient} on this server.")
        else:
            print(f"[ROUTING] Server {peer_port} checked for {recipient}: not local.")
            broadcast_flood(data, sublen, exclude_conn=conn)
    except OSError as error:
        print(f"[ROUTING] Delivery failed for {recipient}: {error}")


def process_client_message(conn, sublen, data):
    """Route a client's message locally or start a deduplicated network flood."""
    if sublen == 0 or sublen > len(data):
        print("[ROUTING] Ignoring malformed client message.")
        return

    with state_lock:
        sender = next(
            (name for name, client_conn in clients_dict.items() if client_conn is conn),
            None,
        )
    if sender is None:
        return

    recipient_bytes = data[:sublen]
    if b"\0" in recipient_bytes:
        print("[ROUTING] Ignoring invalid recipient name.")
        return
    recipient = recipient_bytes.decode("utf-8", "replace")
    content = data[sublen:]
    routing_prefix = recipient.encode() + b"\0" + sender.encode()
    if len(routing_prefix) + len(content) + 34 > MAX_PAYLOAD:
        print("[ROUTING] Message is too large to relay safely.")
        return

    if deliver_to_local_client(recipient, sender, content):
        print(f"[ROUTING] Delivered {sender} -> {recipient} on this server.")
        return

    message_id = uuid.uuid4().hex.encode()
    remember_message(message_id)
    relay_data = routing_prefix + b"\0" + message_id + b"\0" + content
    print(f"[ROUTING] {recipient} is not local; flooding connected servers.")
    broadcast_flood(relay_data, len(routing_prefix))


def identify_connection(conn, subtype, data, address):
    """Register a new server or client after validating its type and identity."""
    if subtype == SERVER_CONNECTION:
        try:
            remote_port = int(data.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            print(f"[NETWORK] Rejecting server with invalid identity from {address}.")
            return False
        if remote_port not in SERVER_PORTS or remote_port == own_port:
            print(f"[NETWORK] Rejecting invalid server port {remote_port}.")
            return False

        with state_lock:
            existing = servers_dict.get(remote_port)
            if existing is not None and existing is not conn:
                print(f"[NETWORK] Duplicate connection from server {remote_port}.")
                return False
            servers_dict[remote_port] = conn
            known_servers[remote_port] = HOST

        print(f"[NETWORK] Connected to server {remote_port} at {address}.")
        send_message(conn, REQ_INFO, 0, 0, b"")
        send_message(conn, REQ_INFO, 1, 0, b"")
        broadcast_server_list(exclude_conn=conn)
        return True

    if subtype == CLIENT_CONNECTION:
        username = data.decode("utf-8", "replace").strip()
        username_bytes = username.encode("utf-8")
        if not username_bytes or len(username_bytes) > MAX_USERNAME_BYTES or b"\0" in username_bytes:
            print(f"[CLIENT] Rejecting invalid username from {address}.")
            return False
        with state_lock:
            if len(clients_dict) >= MAX_CLIENTS or username in clients_dict:
                print(f"[CLIENT] Rejecting unavailable username {username!r}.")
                return False
            clients_dict[username] = conn
        print(f"[CLIENT] {username} connected from {address}.")
        return True

    print(f"[NETWORK] Rejecting unknown connection subtype {subtype}.")
    return False


def remove_connection(conn):
    """Remove a disconnected socket from all active connection maps."""
    with state_lock:
        username = next(
            (name for name, client_conn in clients_dict.items() if client_conn is conn),
            None,
        )
        if username is not None:
            del clients_dict[username]
            print(f"[CLIENT] {username} disconnected.")
        for port, peer in list(servers_dict.items()):
            if peer is conn:
                del servers_dict[port]
                print(f"[NETWORK] Server {port} disconnected.")
        send_locks.pop(conn, None)


def close_connection(conn):
    """Remove a failed socket from shared maps and close it."""
    remove_connection(conn)
    try:
        conn.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    conn.close()


def connection_handler(conn, address, already_identified=False):
    """Read and dispatch frames until the connection closes."""
    identified = already_identified
    try:
        while True:
            message = recv_message(conn)
            if message is None:
                break
            msg_type, subtype, sublen, data = message
            log_message("RECV", msg_type, subtype)

            if msg_type == IDENTIFY and not identified:
                identified = identify_connection(conn, subtype, data, address)
                if not identified:
                    break
            elif not identified:
                print(f"[NETWORK] Message before identification from {address}; closing.")
                break
            elif msg_type == REQ_INFO:
                handle_info_request(conn, subtype)
            elif msg_type == ECHO:
                handle_echo(conn, subtype, sublen, data)
            elif msg_type == REP_INFO:
                if subtype == 0:
                    topology_changed = add_known_servers(data)
                    threading.Thread(
                        target=connect_to_known_servers,
                        args=(data,),
                        daemon=True,
                    ).start()
                    if topology_changed:
                        broadcast_server_list(exclude_conn=conn)
                elif subtype == 1:
                    users = [
                        user.decode("utf-8", "replace")
                        for user in data.split(b"\0")
                        if user
                    ]
                    print(f"[NETWORK] Server {address} reports local clients: {users or 'none'}.")
            elif msg_type == SEND_MSG:
                if subtype == CLIENT_MESSAGE:
                    process_client_message(conn, sublen, data)
                elif subtype == SERVER_FLOOD:
                    process_server_flood(conn, sublen, data)
            else:
                print(f"[NETWORK] Ignoring unsupported message type {msg_type}.")
    except (OSError, ValueError, struct.error) as error:
        print(f"[NETWORK] Connection {address} closed after error: {error}")
    finally:
        remove_connection(conn)
        conn.close()


def accept_connections(server_socket):
    """Accept inbound sockets and give each one a handler thread."""
    while True:
        try:
            conn, address = server_socket.accept()
        except OSError:
            return
        threading.Thread(
            target=connection_handler,
            args=(conn, address),
            daemon=True,
        ).start()


def connect_to_known_servers(data):
    """Connect once to known higher-port servers in deterministic order."""
    addresses = []
    for entry in data.split(b"\0"):
        if not entry:
            continue
        try:
            host, port_text = entry.decode().rsplit(":", 1)
            host = host.strip()
            port = int(port_text)
        except (UnicodeDecodeError, ValueError):
            continue
        if host and port > own_port and port in SERVER_PORTS:
            addresses.append((port, host))

    for port, host in sorted(set(addresses)):
        with state_lock:
            if port in servers_dict:
                continue
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conn.settimeout(2)
            conn.connect((host, port))
            conn.settimeout(None)
            with state_lock:
                if port in servers_dict:
                    conn.close()
                    continue
                servers_dict[port] = conn
                known_servers[port] = host
            threading.Thread(
                target=connection_handler,
                args=(conn, (host, port), True),
                daemon=True,
            ).start()
            send_message(conn, IDENTIFY, SERVER_CONNECTION, 0, str(own_port).encode("ascii"))
            send_message(conn, REQ_INFO, 0, 0, b"")
            send_message(conn, REQ_INFO, 1, 0, b"")
            broadcast_server_list(exclude_conn=conn)
            print(f"[NETWORK] Connected to known server {host}:{port}.")
        except OSError as error:
            conn.close()
            with state_lock:
                if servers_dict.get(port) is conn:
                    del servers_dict[port]
            if not isinstance(error, ConnectionRefusedError):
                print(f"[NETWORK] Could not connect to {host}:{port}: {error}")


def search_bootstrap_servers():
    """Check each bootstrap address once and connect to every available peer."""
    found_server = False
    for host, port in sorted(BOOTSTRAP_ADDRESSES, key=lambda address: (address[1], address[0])):
        if port == own_port:
            continue
        print(f"[CHECKING] Looking for server at {host}:{port}.", flush=True)
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conn.settimeout(2)
            conn.connect((host, port))
            conn.settimeout(None)
            with state_lock:
                if port in servers_dict:
                    conn.close()
                    found_server = True
                    continue
                servers_dict[port] = conn
                known_servers[port] = host
            threading.Thread(
                target=connection_handler,
                args=(conn, (host, port), True),
                daemon=True,
            ).start()
            send_message(conn, IDENTIFY, SERVER_CONNECTION, 0, str(own_port).encode("ascii"))
            send_message(conn, REQ_INFO, 0, 0, b"")
            send_message(conn, REQ_INFO, 1, 0, b"")
            broadcast_server_list(exclude_conn=conn)
            print(f"[SUCCESS] Connected to bootstrap server {host}:{port}.", flush=True)
            found_server = True
        except OSError:
            conn.close()
    if not found_server:
        print("[NETWORK] No existing servers found; waiting for incoming connections.", flush=True)
    return found_server


def main():
    """Select this server's port, listen for peers, and optionally bootstrap."""
    global own_port
    print("[SETUP] Available server indices: 0, 1, 2, 3, 4")
    try:
        index = int(input("Choose your server index: "))
    except ValueError:
        print("[ERROR] Enter a valid index.")
        return
    if not 0 <= index < len(SERVER_PORTS):
        print("[ERROR] Index must be between 0 and 4.")
        return
    own_port = SERVER_PORTS[index]

    search_choice = input("Search for servers? (y/n): ").strip().lower()
    if search_choice not in ("y", "n"):
        print("[ERROR] Enter 'y' to search or 'n' to wait only.")
        return

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server_socket.bind((HOST, own_port))
        server_socket.listen(10)
    except OSError as error:
        server_socket.close()
        print(f"[ERROR] Could not listen on {HOST}:{own_port}: {error}")
        return

    with state_lock:
        known_servers[own_port] = HOST
    print(f"[LISTENING] Server active at {HOST}:{own_port}.", flush=True)
    threading.Thread(target=accept_connections, args=(server_socket,), daemon=True).start()
    if search_choice == "y":
        search_bootstrap_servers()
    else:
        print("[NETWORK] Search skipped; waiting for incoming connections.", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping server.")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()

import socket
import struct
import threading
import time
import uuid

# Frames use a six-byte header, followed by at most 65535 payload bytes.
MAX_PAYLOAD = 65535
PROBE_TIMEOUT = 3

REQ_INFO = 0
REP_INFO = 1
IDENTIFY = 2
SEND_MSG = 3
ECHO = 4
CLIENT_CONNECTION = 1
CLIENT_MESSAGE = 0
ECHO_REQUEST = 0
ECHO_REPLY = 1
ECHO_PROBE = b"hello"
ECHO_RESPONSE = b"ifmmp"
MESSAGE_SIGNS = {
    REQ_INFO: "!@",
    REP_INFO: "@#",
    IDENTIFY: "#$",
    SEND_MSG: "$!",
    ECHO: "%^",
}
MESSAGE_NAMES = {
    REQ_INFO: "INFO REQUEST",
    REP_INFO: "INFO REPLY",
    IDENTIFY: "IDENTIFY",
    SEND_MSG: "MESSAGE",
    ECHO: "ECHO",
}

connection_lock = threading.RLock()
topology_lock = threading.Lock()
topology_changed = threading.Event()
current_connection = None
current_server = None
current_rtt = None
known_servers = {}


def log_message(direction, msg_type, subtype):
    """Print the marker for a sent or received protocol message."""
    sign = MESSAGE_SIGNS.get(msg_type, "?!")
    name = MESSAGE_NAMES.get(msg_type, "UNKNOWN")
    print(f"{sign} [{direction} {name}:{subtype}]", flush=True)


def recv_exact(conn, size):
    """Read exactly size bytes, or return None if the server disconnects."""
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


def send_message(conn, msg_type, subtype, sublen, data, log=True):
    """Encode and send one complete protocol frame."""
    if len(data) > MAX_PAYLOAD:
        raise ValueError(f"Payload exceeds {MAX_PAYLOAD} bytes")
    conn.sendall(struct.pack(">BBHH", msg_type, subtype, len(data), sublen) + data)
    if log:
        log_message("SEND", msg_type, subtype)


def update_server_list(data):
    """Record a server-list update and request a fresh RTT comparison."""
    updated = {}
    for entry in data.split(b"\0"):
        if not entry:
            continue
        try:
            host, port_text = entry.decode().rsplit(":", 1)
            port = int(port_text)
        except (UnicodeDecodeError, ValueError):
            print(f"[DISCOVERY] Ignoring invalid address: {entry!r}")
            continue
        if host.strip() and 1 <= port <= 65535:
            updated[port] = host.strip()

    with topology_lock:
        if updated != known_servers:
            known_servers.clear()
            known_servers.update(updated)
            topology_changed.set()


def listen_for_messages(conn):
    """Print incoming chat messages while the input loop is running."""
    try:
        while True:
            message = recv_message(conn)
            if message is None:
                with connection_lock:
                    is_current = conn is current_connection
                if is_current:
                    print("\n[CLIENT] Server closed the connection.")
                    topology_changed.set()
                return
            msg_type, subtype, sublen, data = message
            log_message("RECV", msg_type, subtype)
            if msg_type == REP_INFO and subtype == 0:
                update_server_list(data)
                continue
            if msg_type != SEND_MSG:
                continue
            if subtype != CLIENT_MESSAGE or sublen > len(data):
                print("\n[CLIENT] Ignoring malformed message.")
                continue

            routing = data[:sublen].split(b"\0", 1)
            sender = routing[1].decode("utf-8", "replace") if len(routing) == 2 else "unknown"
            content = data[sublen:].decode("utf-8", "replace")
            print(f"\n[FROM {sender}]: {content}")
    except (OSError, ValueError, struct.error) as error:
        with connection_lock:
            is_current = conn is current_connection
        if is_current:
            print(f"\n[CLIENT] Receive error: {error}")
            topology_changed.set()
    finally:
        conn.close()


def probe_server(host, port, username, existing_conn=None, identity=None):
    """Connect to a server if needed and return its measured RTT and socket."""
    conn = existing_conn
    try:
        if conn is None:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(PROBE_TIMEOUT)
        if existing_conn is None:
            conn.connect((host, port))
            probe_label = username[:32].replace("\0", "_")
            probe_identity = identity or f"__rtt_{probe_label}_{uuid.uuid4().hex}"
            send_message(conn, IDENTIFY, CLIENT_CONNECTION, 0, probe_identity.encode("utf-8"))

        log_message("SEND", ECHO, ECHO_REQUEST)
        started = time.perf_counter()
        send_message(conn, ECHO, ECHO_REQUEST, 0, ECHO_PROBE, log=False)
        while True:
            response = recv_message(conn)
            if response is None:
                raise ConnectionError("Server closed before echo response")
            msg_type, subtype, sublen, data = response
            log_message("RECV", msg_type, subtype)
            if msg_type == REP_INFO and subtype == 0:
                update_server_list(data)
                continue
            break
        elapsed = time.perf_counter() - started
        if (
            msg_type != ECHO
            or subtype != ECHO_REPLY
            or sublen != 0
            or data != ECHO_RESPONSE
        ):
            raise ValueError("Invalid echo response")
        return elapsed, conn
    except (OSError, ValueError, struct.error) as error:
        print(f"[RTT] {host}:{port} unavailable: {error}")
        if conn is not None:
            conn.close()
        return None


def discover_and_select(host, port, username, initial_conn):
    """Discover server addresses, probe them concurrently, and keep the fastest."""
    initial_conn.settimeout(PROBE_TIMEOUT)
    send_message(initial_conn, REQ_INFO, 0, 0, b"")
    response = recv_message(initial_conn)
    if response is None:
        raise ConnectionError("Server closed before sending its server list")
    msg_type, subtype, _, data = response
    log_message("RECV", msg_type, subtype)
    if msg_type != REP_INFO or subtype != 0:
        raise ValueError("Server did not return the requested server list")

    addresses = {port: host}
    for entry in data.split(b"\0"):
        if not entry:
            continue
        try:
            server_host, server_port = entry.decode().rsplit(":", 1)
            server_host = server_host.strip()
            server_port = int(server_port)
            if server_host and 1 <= server_port <= 65535:
                addresses.setdefault(server_port, server_host)
        except (UnicodeDecodeError, ValueError):
            print(f"[DISCOVERY] Ignoring invalid address: {entry!r}")

    results = []
    results_lock = threading.Lock()
    threads = []
    for server_port, server_host in sorted(addresses.items()):
        existing = initial_conn if server_port == port else None
        thread = threading.Thread(
            target=lambda h=server_host, p=server_port, c=existing: append_probe_result(
                results, results_lock, probe_server(h, p, username, c), p
            ),
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if not results:
        raise ConnectionError("Could not measure RTT to any discovered server")

    best_rtt, best_conn, best_port = min(results, key=lambda result: (result[0], result[2]))
    for _, conn, _ in results:
        if conn is not best_conn:
            conn.close()
    best_conn.settimeout(None)
    print(f"[RTT] Selected {addresses[best_port]}:{best_port} ({best_rtt * 1000:.2f} ms).")
    return best_conn, best_rtt, best_port, addresses


def append_probe_result(results, results_lock, result, port):
    """Store a successful probe result for the selection step."""
    if result is None:
        return
    rtt, conn = result
    with results_lock:
        results.append((rtt, conn, port))


def monitor_server_rtt(username):
    """Recheck discovered servers after topology updates and migrate if faster."""
    global current_connection, current_server, current_rtt
    while True:
        topology_changed.wait()
        topology_changed.clear()
        with topology_lock:
            addresses = dict(known_servers)
        with connection_lock:
            active_server = current_server
            active_rtt = current_rtt
        if active_server is None:
            continue

        measured = []
        threads = []
        results_lock = threading.Lock()
        probe_identity = f"__rtt_{uuid.uuid4().hex}"
        for server_port, server_host in sorted(addresses.items()):
            thread = threading.Thread(
                target=lambda h=server_host, p=server_port: append_probe_result(
                    measured,
                    results_lock,
                    probe_server(h, p, username, identity=probe_identity),
                    p,
                ),
                daemon=True,
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

        if not measured:
            print("[RTT] No discovered servers responded to a probe.")
            continue

        active_samples = [result for result in measured if result[2] == active_server[1]]
        alternatives = [result for result in measured if result[2] != active_server[1]]
        for _, conn, _ in measured:
            conn.close()
        if active_samples:
            active_rtt = min(result[0] for result in active_samples)
            with connection_lock:
                if current_server == active_server:
                    current_rtt = active_rtt
        else:
            active_rtt = None
            with connection_lock:
                if current_server == active_server:
                    current_rtt = None
        if not alternatives:
            continue
        best_rtt, _, best_port = min(alternatives, key=lambda result: (result[0], result[2]))
        if active_rtt is not None and best_rtt >= active_rtt:
            print(f"[RTT] Current server {active_server[0]}:{active_server[1]} remains fastest.")
            continue

        new_host = addresses[best_port]
        replacement = probe_server(
            new_host,
            best_port,
            username,
            identity=username,
        )
        if replacement is None:
            continue
        replacement_rtt, replacement_conn = replacement
        with connection_lock:
            if current_server != active_server or current_connection is None:
                replacement_conn.close()
                continue
            if current_rtt is not None and replacement_rtt >= current_rtt:
                replacement_conn.close()
                continue
            old_conn = current_connection
            current_connection = replacement_conn
            current_server = (new_host, best_port)
            current_rtt = replacement_rtt
        replacement_conn.settimeout(None)
        threading.Thread(
            target=listen_for_messages,
            args=(replacement_conn,),
            daemon=True,
        ).start()
        old_conn.close()
        print(
            f"[RTT] Switched to faster server {new_host}:{best_port} "
            f"({replacement_rtt * 1000:.2f} ms)."
        )


def main():
    """Connect to a server, identify the client, and run the chat input loop."""
    global current_connection, current_server, current_rtt
    host = input("Enter server address (e.g. 127.0.0.1): ").strip()
    try:
        port = int(input("Enter server port: "))
    except ValueError:
        print("Enter a valid server port.")
        return
    username = input("Enter your username: ").strip()
    if not host or not 1 <= port <= 65535 or not username:
        print("Server address, valid port, and username are required.")
        return

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        conn.connect((host, port))
        send_message(conn, IDENTIFY, CLIENT_CONNECTION, 0, username.encode("utf-8"))
        conn, current_rtt, selected_port, addresses = discover_and_select(
            host, port, username, conn
        )
        current_connection = conn
        current_server = (addresses[selected_port], selected_port)
        update_server_list(
            b"\0".join(
                f"{server_host}:{server_port}".encode()
                for server_port, server_host in addresses.items()
            )
        )
    except OSError as error:
        conn.close()
        print(f"Could not connect to {host}:{port}: {error}")
        return
    except (ValueError, struct.error) as error:
        conn.close()
        print(f"Server discovery failed: {error}")
        return

    print(f"[CONNECTED] {current_server[0]}:{current_server[1]} as {username}.")
    threading.Thread(target=listen_for_messages, args=(conn,), daemon=True).start()
    threading.Thread(target=monitor_server_rtt, args=(username,), daemon=True).start()
    print("[READY] Enter <recipient>:<message>, or 'exit' to disconnect.")

    send_lock = threading.Lock()
    try:
        while True:
            try:
                text = input()
            except EOFError:
                break
            if text.strip().lower() == "exit":
                break
            if ":" not in text:
                print("Use <recipient>:<message>.")
                continue

            recipient, content = text.split(":", 1)
            recipient = recipient.strip()
            recipient_bytes = recipient.encode("utf-8")
            content_bytes = content.encode("utf-8")
            if (
                not recipient_bytes
                or b"\0" in recipient_bytes
                or len(recipient_bytes) > 255
                or len(recipient_bytes) + len(content_bytes) + 34 > MAX_PAYLOAD
            ):
                print("Recipient is required and the message must fit in one protocol frame.")
                continue
            with send_lock, connection_lock:
                send_message(
                    current_connection,
                    SEND_MSG,
                    CLIENT_MESSAGE,
                    len(recipient_bytes),
                    recipient_bytes + content_bytes,
                )
    except (OSError, ValueError) as error:
        print(f"[CLIENT] Send error: {error}")
    finally:
        with connection_lock:
            if current_connection is not None:
                current_connection.close()
                current_connection = None


if __name__ == "__main__":
    main()

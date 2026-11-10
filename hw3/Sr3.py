import socket
import threading
import struct

PORTS = [5000, 5001, 5002, 5003, 5004]
HOST = '127.0.0.1'

# Protocol Symbols (Message Types)
REQ_INFO = 0
REP_INFO = 1
IDENTIFY = 2
SEND_MSG = 3
DISCONNECT = 4
REGISTRY_UPDATE = 5
AUTH_RESP = 6
ERROR_MSG = 7
SYS_MSG = 8  # <--- NEW: System Announcements

servers = {}
clients = {}
remote_clients = {}


def read_bytes(conn, size):
    data = b''
    while len(data) < size:
        packet = conn.recv(size - len(data))
        if not packet:
            return None
        data += packet
    return data


def read_msg(conn):
    header = read_bytes(conn, 6)
    if not header:
        return None
    msg_type, subtype, length, sublen = struct.unpack('>BBHH', header)
    data = read_bytes(conn, length) if length > 0 else b''
    return msg_type, subtype, length, sublen, data


def send_msg(conn, msg_type, subtype, sublen, data):
    header = struct.pack('>BBHH', msg_type, subtype, len(data), sublen)
    conn.sendall(header + data)


def get_username_by_socket(target_conn):
    for username, conn in clients.items():
        if conn == target_conn:
            return username
    return "Unknown"


def broadcast_to_servers(msg_type, subtype, data, exclude_conn=None):
    for srv_sock in list(servers.values()):
        if srv_sock != exclude_conn:
            try:
                send_msg(srv_sock, msg_type, subtype, 0, data)
            except:
                pass


def notify_local_clients(message):
    """--- NEW: Broadcasts system messages to all clients connected to this server ---"""
    for conn in list(clients.values()):
        try:
            send_msg(conn, SYS_MSG, 0, 0, message.encode())
        except:
            pass


def print_network_state():
    print("\n=== NETWORK STATE UPDATE ===")
    print(f"Connected Servers: {list(servers.keys()) if servers else 'None'}")
    print("Connected Users:")

    if not clients and not remote_clients:
        print(" - None")

    for user in clients:
        print(f" - {user} (Local)")

    for user, sock in remote_clients.items():
        server_port = "Unknown Port"
        for port, srv_sock in servers.items():
            if srv_sock == sock:
                server_port = f"Port {port}"
                break
        print(f" - {user} (Remote: {server_port})")
    print("============================\n")


# ==========================================
# INTERACTION HANDLERS
# ==========================================

def handle_info_req(conn, subtype):
    if subtype == 0:
        info = b'\0'.join([f"{HOST}:{p}".encode() for p in servers.keys()])
        send_msg(conn, REP_INFO, 0, 0, info)
    elif subtype == 1:
        all_users = list(clients.keys()) + list(remote_clients.keys())
        info = b'\0'.join([name.encode() for name in all_users])
        send_msg(conn, REP_INFO, 1, 0, info)


def handle_info_reply(conn, subtype, data):
    if subtype == 0:
        print(f"~~~> [INFO] Active Servers: {data.decode('utf-8', 'ignore')} <~~~")
    elif subtype == 1:
        if data:
            users = data.split(b'\0')
            for u in users:
                remote_clients[u.decode()] = conn
            print(f"~~~> [INFO] Synced {len(users)} remote users. <~~~")
        print_network_state()


def handle_auth(conn, address, subtype, data):
    if subtype == 1:
        username = data.decode()

        if username in clients or username in remote_clients:
            print(f"!!!> [AUTH FAILED] Username '{username}' already taken. Rejecting {address} <!!!")
            send_msg(conn, AUTH_RESP, 0, 0, b'Username already taken across the network.')
            return False

        clients[username] = conn
        print(f"@@@> [NEW CLIENT] {username} connected from {address} <@@@")

        send_msg(conn, AUTH_RESP, 1, 0, b'Registration successful.')

        # --- NEW: Announce the local connection to everyone ---
        notify_local_clients(f"User '{username}' joined the network.")
        broadcast_to_servers(REGISTRY_UPDATE, 0, username.encode())
        print_network_state()
        return True

    elif subtype == 0:
        remote_port = int(data.decode()) if data else address[1]
        servers[remote_port] = conn
        print(f"@@@> [NEW SERVER] Connected from {address} (Port {remote_port}) <@@@")
        return True


def handle_routing(conn, sublen, data):
    sender_name = get_username_by_socket(conn)

    if sender_name == "Unknown":
        parts = data[:sublen].split(b'\0')
        receiver_name = parts[0].decode()
        if len(parts) >= 2:
            sender_name = parts[1].decode()
        message_content = data[sublen:]
    else:
        receiver_name = data[:sublen].decode()
        message_content = data[sublen:]

    print(f"---> [ROUTING] {sender_name} -> {receiver_name} <---")

    receiver_bytes = receiver_name.encode()
    sender_bytes = sender_name.encode()

    new_routing_prefix = receiver_bytes + b'\0' + sender_bytes
    new_sublen = len(new_routing_prefix)
    new_data = new_routing_prefix + message_content

    if receiver_name in clients:
        send_msg(clients[receiver_name], SEND_MSG, 0, new_sublen, new_data)
    elif receiver_name in remote_clients:
        send_msg(remote_clients[receiver_name], SEND_MSG, 0, new_sublen, new_data)
    else:
        print(f"!!!> [WARNING] User '{receiver_name}' not found. <!!!")
        if sender_name in clients:
            error_text = f"User '{receiver_name}' is not connected."
            send_msg(clients[sender_name], ERROR_MSG, 0, 0, error_text.encode())


def handle_disconnect(conn):
    username = get_username_by_socket(conn)
    if username != "Unknown":
        del clients[username]
        print(f"<<<> [DISCONNECTED] Local client '{username}' left. <<<>")

        # --- NEW: Announce the disconnection to everyone ---
        notify_local_clients(f"User '{username}' left the network.")
        broadcast_to_servers(REGISTRY_UPDATE, 1, username.encode())
        print_network_state()
    else:
        for port, srv_sock in list(servers.items()):
            if srv_sock == conn:
                del servers[port]
                removed_users = [u for u, c in remote_clients.items() if c == conn]
                for u in removed_users:
                    del remote_clients[u]
                    # --- NEW: Announce remote users dropped because their server died ---
                    notify_local_clients(f"User '{u}' left the network.")

                print(
                    f"<<<> [DISCONNECTED] Server at port {port} left. Removed {len(removed_users)} remote users. <<<>")
                print_network_state()
                break


def handle_registry_update(conn, subtype, data):
    username = data.decode()
    if subtype == 0:
        if username not in remote_clients:
            remote_clients[username] = conn
            print(f"===> [REGISTRY] Added remote user '{username}' <===")

            # --- NEW: Announce remote connection to local clients ---
            notify_local_clients(f"User '{username}' joined the network.")
            broadcast_to_servers(REGISTRY_UPDATE, subtype, data, exclude_conn=conn)

    elif subtype == 1:
        if username in remote_clients:
            del remote_clients[username]
            print(f"===> [REGISTRY] Removed remote user '{username}' <===")

            # --- NEW: Announce remote disconnection to local clients ---
            notify_local_clients(f"User '{username}' left the network.")
            broadcast_to_servers(REGISTRY_UPDATE, subtype, data, exclude_conn=conn)

    print_network_state()


# ==========================================
# CONNECTION MANAGEMENT
# ==========================================

def manage_traffic(conn, address):
    with conn:
        while True:
            msg = read_msg(conn)
            if not msg:
                break

            msg_type, subtype, length, sublen, data = msg

            if msg_type == REQ_INFO:
                handle_info_req(conn, subtype)
            elif msg_type == REP_INFO:
                handle_info_reply(conn, subtype, data)
            elif msg_type == IDENTIFY:
                success = handle_auth(conn, address, subtype, data)
                if not success:
                    break
            elif msg_type == SEND_MSG:
                handle_routing(conn, sublen, data)
            elif msg_type == DISCONNECT:
                handle_disconnect(conn)
                break
            elif msg_type == REGISTRY_UPDATE:
                handle_registry_update(conn, subtype, data)


def accept_conns(server_socket):
    while True:
        try:
            conn, address = server_socket.accept()
            threading.Thread(target=manage_traffic, args=(conn, address), daemon=True).start()
        except Exception:
            break


def link_servers(own_port):
    connected = False
    for port in sorted(PORTS):
        if port == own_port:
            continue
        if not connected:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.settimeout(2)
                client.connect((HOST, port))
                client.settimeout(None)

                print(f"***> [LINKED] Connected to server at {port} <***")
                servers[port] = client

                send_msg(client, IDENTIFY, 0, 0, str(own_port).encode())
                send_msg(client, REQ_INFO, 0, 0, b'')
                send_msg(client, REQ_INFO, 1, 0, b'')

                threading.Thread(target=manage_traffic, args=(client, (HOST, port)), daemon=True).start()
                connected = True
            except (ConnectionRefusedError, OSError):
                pass
    if not connected:
        print("!!!> [NETWORK] No other servers found. Starting a new network. <!!!")


def main():
    print("===> Available port indices: 0, 1, 2, 3, 4 <===")
    try:
        index = int(input("Choose your server index: "))
        own_port = PORTS[index] if 0 <= index < len(PORTS) else None
        if own_port is None:
            print("!!!> Index must be between 0 and 4. <!!!")
            return
    except ValueError:
        print("!!!> Please enter a valid number. <!!!")
        return

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, own_port))
    server.listen(5)
    print(f"^^^> [LISTENING] Server active at {HOST}:{own_port} <^^^\n")

    threading.Thread(target=accept_conns, args=(server,), daemon=True).start()
    link_servers(own_port)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n!!!> [SHUTDOWN] Stopping server. <!!!")
        server.close()


if __name__ == '__main__':
    main()
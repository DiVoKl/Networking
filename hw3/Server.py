import socket
import threading
import struct

PORTS = [5000, 5001, 5002, 5003, 5004]
HOST = '127.0.0.1'

# Dictionaries to store active connections
servers_dict = {}  # {port: socket}
clients_dict = {}  # {username: socket}


def recv_exact(conn, size):
    """Reads an exact number of bytes from the socket"""
    data = b''
    while len(data) < size:
        packet = conn.recv(size - len(data))
        if not packet:
            return None
        data += packet
    return data


def receive_message(conn):
    """Receives a message according to the custom protocol"""
    header = recv_exact(conn, 6)
    if not header:
        return None

    # Unpacking the 6-byte header: type (1), subtype (1), len (2), sublen (2)
    msg_type, subtype, length, sublen = struct.unpack('>BBHH', header)

    data = b''
    if length > 0:
        data = recv_exact(conn, length)

    return msg_type, subtype, length, sublen, data


def send_message(conn, msg_type, subtype, sublen, data):
    """Sends a message according to the custom protocol"""
    length = len(data)
    header = struct.pack('>BBHH', msg_type, subtype, length, sublen)
    conn.sendall(header + data)


def handle_connection(connection, address, is_server_conn=False):
    """Handles incoming and outgoing connection traffic"""
    with connection:
        while True:
            msg = receive_message(connection)
            if not msg:
                break

            msg_type, subtype, length, sublen, data = msg

            # Type 0: Request connection information
            if msg_type == 0:
                if subtype == 0:  # Request servers
                    servers_info = b'\0'.join([f"{HOST}:{p}".encode() for p in servers_dict.keys()])
                    send_message(connection, 1, 0, 0, servers_info)
                elif subtype == 1:  # Request clients
                    clients_info = b'\0'.join([name.encode() for name in clients_dict.keys()])
                    send_message(connection, 1, 1, 0, clients_info)

            # Type 1: Information reply
            elif msg_type == 1:
                print(f"~~~> [INFO] Received info reply (subtype {subtype}): {data.decode('utf-8', 'ignore')} <~~~")

            # Type 2: Identification
            elif msg_type == 2:
                if subtype == 1:  # Client connected
                    username = data.decode()
                    clients_dict[username] = connection
                    print(f"@@@> [NEW CLIENT] {username} connected from {address} <@@@")
                elif subtype == 0:  # Server connected
                    print(f"@@@> [NEW SERVER] Server connected from {address} <@@@")

            # Type 3: Message routing
            elif msg_type == 3:
                # data structure: receiver_name + message_content
                receiver_name = data[:sublen].decode()
                message_content = data[sublen:]

                print(f"---> [MESSAGE] Routing to {receiver_name}: {message_content.decode('utf-8', 'ignore')} <---")

                if receiver_name in clients_dict:
                    # Receiver is connected to this server, pass it directly
                    send_message(clients_dict[receiver_name], 3, 0, sublen, data)
                else:
                    # Broadcast to all other servers if receiver is not local
                    for srv_port, srv_sock in servers_dict.items():
                        if srv_sock != connection:
                            try:
                                send_message(srv_sock, 3, 0, sublen, data)
                            except:
                                pass


def accept_connections(server_socket):
    while True:
        try:
            connection, address = server_socket.accept()
            threading.Thread(target=handle_connection, args=(connection, address), daemon=True).start()
        except Exception as e:
            break


def contact_other_servers(own_port):
    """Deterministic connection to the lowest available server port"""
    connected = False
    for port in sorted(PORTS):
        if port == own_port:
            continue

        if not connected:
            try:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.settimeout(2)
                client.connect((HOST, port))
                print(f"***> [SUCCESS] Connected to server at {port} <***")
                servers_dict[port] = client

                # Identify as a server
                send_message(client, 2, 0, 0, b'')

                # Request list of servers
                send_message(client, 0, 0, 0, b'')

                threading.Thread(target=handle_connection, args=(client, (HOST, port), True), daemon=True).start()
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
    print(f"^^^> [LISTENING] Server listening at {HOST}:{own_port} <^^^\n")

    threading.Thread(target=accept_connections, args=(server,), daemon=True).start()

    contact_other_servers(own_port)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n!!!> [SHUTDOWN] Stopping server. <!!!")
        server.close()


if __name__ == '__main__':
    main()
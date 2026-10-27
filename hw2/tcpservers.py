import socket
import threading

# 1. Array of 5 ports
PORTS = [5000, 5001, 5002, 5003, 5004]
HOST = '127.0.0.1'


def handle_client(connection, address):
    with connection:
        print(f'[SERVICING] Active connection from client at {address}', flush=True)
        try:
            data = connection.recv(1024)
            if data == b'Hello':
                connection.sendall(b'World')
                print(f'[REPLY] Sent "World" to {address}', flush=True)
        except Exception as e:
            print(f'[ERROR] Client handling error: {e}', flush=True)


def accept_connections(server_socket):
    while True:
        try:
            connection, address = server_socket.accept()
            print(f'[INCOMING] Created socket for connection with {address}', flush=True)
            threading.Thread(
                target=handle_client,
                args=(connection, address),
                daemon=True
            ).start()
        except Exception as e:
            break


def contact_other_servers(own_port):
    for port in PORTS:
        if port == own_port:
            continue  # Do not connect to ourselves

        print(f'[CHECKING] Checking port {port}...', flush=True)
        print(f'[OUTGOING] Creating client socket to connect with target ({HOST}, {port})', flush=True)

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.settimeout(2)
                client.connect((HOST, port))
                print(f'[SUCCESS] Successfully connected socket to ({HOST}, {port})', flush=True)

                client.sendall(b'Hello')
                response = client.recv(1024)

                if response == b'World':
                    print(f'[RESPONSE] Port {port}: received "World"', flush=True)
                    print(f'[CHECK UPDATE] Port {port} check: SUCCEEDED\n', flush=True)
                else:
                    print(f'[RESPONSE] Port {port}: unexpected reply {response!r}', flush=True)
                    print(f'[CHECK UPDATE] Port {port} check: FAILED (Unexpected response)\n', flush=True)

        except ConnectionRefusedError:
            print(f'[RESULT] Port {port}: no server running (Connection Refused)', flush=True)
            print(f'[CHECK UPDATE] Port {port} check: FAILED (No server)\n', flush=True)
        except OSError as error:
            print(f'[RESULT] Port {port}: connection failed ({error})', flush=True)
            print(f'[CHECK UPDATE] Port {port} check: FAILED ({error})\n', flush=True)


def main():
    print('Available port indices: 0, 1, 2, 3, 4')
    try:
        index = int(input('Choose your server index: '))
        own_port = PORTS[index] if 0 <= index < len(PORTS) else None
        if own_port is None:
            print('Index must be between 0 and 4.')
            return
    except ValueError:
        print('Please enter a valid number.')
        return

    print(f'[SETUP] Creating listening port on {HOST}:{own_port}...', flush=True)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    server.bind((HOST, own_port))
    server.listen(5)
    print(f'[LISTENING] Server port created and listening at {HOST}:{own_port}\n', flush=True)

    # Launch incoming listener thread
    threading.Thread(target=accept_connections, args=(server,), daemon=True).start()

    contact_other_servers(own_port)

    print('[STATUS] Finished checking all ports. Listening for incoming connections. (Press Ctrl+C to exit)',
          flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print('\n[SHUTDOWN] Stopping server.')
        server.close()


if __name__ == '__main__':
    main()
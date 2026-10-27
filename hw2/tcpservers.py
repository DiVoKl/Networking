import socket
import threading

# Each running copy chooses one of these ports (indices 0 through 4).
PORTS = [5000, 5001, 5002, 5003, 5004]
HOST = '127.0.0.1'


def handle_client(connection, address):
    """Reply World to a received Hello."""
    with connection:
        print(f'Incoming connection from {address}', flush=True)
        data = connection.recv(1024)
        if data == b'Hello':
            connection.sendall(b'World')
            print(f'Replied World to {address}', flush=True)


def accept_connections(server_socket):
    """Accept incoming clients while main thread makes outgoing connections."""
    while True:
        connection, address = server_socket.accept()
        threading.Thread(
            target=handle_client,
            args=(connection, address),
            daemon=True
        ).start()


def contact_other_servers(own_port):
    for port in PORTS:
        if port == own_port:
            continue  # Do not connect to ourselves.

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                # Allow reuse of our server's source port for outgoing sockets.
                client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if hasattr(socket, "SO_REUSEPORT"):
                    client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                client.bind((HOST, own_port))
                client.settimeout(2)
                client.connect((HOST, port))
                client.sendall(b'Hello')
                response = client.recv(1024)
                if response == b'World':
                    print(f'Port {port}: received World - finished', flush=True)
                else:
                    print(f'Port {port}: unexpected reply {response!r}', flush=True)
        except ConnectionRefusedError:
            print(f'Port {port}: no server running', flush=True)
        except OSError as error:
            print(f'Port {port}: connection failed ({error})', flush=True)


def main():
    print('Available port indices: 0, 1, 2, 3, 4')
    try:
        index = int(input('Choose your server index: '))
        own_port = PORTS[index] if 0 <= index < len(PORTS) else None
        if own_port is None:
            print('Index must be between 0 and 4.')
            return
    except ValueError:
        print('Please enter a number.')
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server.bind((HOST, own_port))
        server.listen(5)
        print(f'Listening at {HOST}:{own_port}', flush=True)

        threading.Thread(target=accept_connections, args=(server,), daemon=True).start()
        contact_other_servers(own_port)
        print('Waiting for incoming connections. Press Ctrl+C to stop.', flush=True)
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            print('\nStopping server.')


if __name__ == '__main__':
    main()

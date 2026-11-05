import socket
import threading
import sys

# => Array of 5 ports (Step 1)
PORTS = [5000, 5001, 5002, 5003, 5004]

def handle_incoming_connection(conn, addr):
    # -> Handle incoming client connection (Step 5)
    print(f"-> [{addr}] New connection established.")
    try:
        # > Wait for "Hello" message
        data = conn.recv(1024).decode('utf-8')
        if data == "Hello":
            print(f"-> [{addr}] Received: Hello")
            # > Respond with "World"
            conn.send("World".encode('utf-8'))
            print(f"-> [{addr}] Sent: World")
    except Exception as e:
        print(f"-> [{addr}] Connection error: {e}")
    finally:
        conn.close()

def accept_connections(server_socket):
    # -> Thread assigned to listen and accept incoming connections (Step 3)
    while True:
        try:
            conn, addr = server_socket.accept()
            # > Spawn a new thread for each accepted connection (Step 5)
            client_thread = threading.Thread(target=handle_incoming_connection, args=(conn, addr))
            client_thread.daemon = True
            client_thread.start()
        except Exception as e:
            print(f"-> Accept error: {e}")
            break

def connect_to_other_servers(my_port):
    # -> Loop through and try connecting to all other server ports (Step 4)
    for port in PORTS:
        if port == my_port:
            continue  # > Do not connect to self

        try:
            # > Attempt connection to local IP 127.0.0.1
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)  # Short timeout to avoid blocking
            sock.connect(('127.0.0.1', port))

            # > Send "Hello" on successful connection
            sock.send("Hello".encode('utf-8'))
            response = sock.recv(1024).decode('utf-8')

            # > Wait for "World" response and print completion
            if response == "World":
                print(f"-> Successfully connected to port {port}. Received: {response}")
            sock.close()

        except ConnectionRefusedError:
            # > Catch ConnectionRefusedError if no listener exists on the other side (Step 4)
            print(f"-> Connection to port {port} failed: ConnectionRefusedError (No server listening)")
        except socket.timeout:
            print(f"-> Connection to port {port} failed: Timeout")
        except Exception as e:
            print(f"-> Connection to port {port} failed: {e}")

def main():
    # => Prompt user for index and pick port from array (Steps 1 & 2)
    try:
        index = int(input("=> Enter server index (0-4): "))
        if index < 0 or index > 4:
            raise ValueError
    except ValueError:
        print("=> Invalid index! Enter a number between 0 and 4.")
        sys.exit(1)

    my_port = PORTS[index]
    print(f"=> Starting server on port {my_port} (Index {index})")

    # => Create socket and start listening on port (Step 3)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', my_port))
    server_socket.listen(5)

    # => Create a new thread dedicated to waiting for accept (Step 3)
    accept_thread = threading.Thread(target=accept_connections, args=(server_socket,))
    accept_thread.daemon = True
    accept_thread.start()

    # => Attempt connection to all other ports (Step 4)
    connect_to_other_servers(my_port)

    # => Keep the main thread active
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\n=> Shutting down server...")
        server_socket.close()

if __name__ == "__main__":
    main()
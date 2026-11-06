import socket
import threading
import struct


def recv_exact(conn, size):
    data = b''
    while len(data) < size:
        packet = conn.recv(size - len(data))
        if not packet:
            return None
        data += packet
    return data


def receive_messages(client_socket):
    """Listens for incoming messages routed from the server"""
    while True:
        try:
            header = recv_exact(client_socket, 6)
            if not header:
                break
            msg_type, subtype, length, sublen = struct.unpack('>BBHH', header)

            data = b''
            if length > 0:
                data = recv_exact(client_socket, length)

            if msg_type == 3:
                # Extracting message content from data
                message_content = data[sublen:].decode()
                print(f"\n>>> [INCOMING MESSAGE]: {message_content} <<<")

        except Exception as e:
            print("!!!> Connection closed. <!!!")
            break


def send_message(conn, msg_type, subtype, sublen, data):
    length = len(data)
    header = struct.pack('>BBHH', msg_type, subtype, length, sublen)
    conn.sendall(header + data)


def main():
    host = input("Enter server IP (e.g., 127.0.0.1): ")
    port = int(input("Enter server Port (e.g., 5000): "))
    username = input("Enter your username: ")

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((host, port))
        print(f"***> Connected to {host}:{port} <***")

        # 1. Send identification message
        # type=2, subtype=1 (Client), sublen=0
        send_message(client, 2, 1, 0, username.encode())

        # Start listening thread
        threading.Thread(target=receive_messages, args=(client,), daemon=True).start()

        # 2. Main loop for sending messages
        print("===> To send a message type: <receiver_name>:<message> <===")
        while True:
            user_input = input()
            if ":" in user_input:
                receiver, msg = user_input.split(":", 1)
                receiver_bytes = receiver.encode()
                msg_bytes = msg.encode()

                # Payload construction: receiver_name + message
                data = receiver_bytes + msg_bytes
                sublen = len(receiver_bytes)

                # Send message: type=3, subtype=0
                send_message(client, 3, 0, sublen, data)
            else:
                print("!!!> Invalid format. Use <receiver_name>:<message> <!!!")

    except Exception as e:
        print(f"!!!> Failed to connect: {e} <!!!")
    finally:
        client.close()


if __name__ == '__main__':
    main()
import socket
import threading
import struct

# Protocol Symbols (Message Types)
IDENTIFY = 2
SEND_MSG = 3
DISCONNECT = 4
AUTH_RESP = 6
ERROR_MSG = 7
SYS_MSG = 8  # <--- NEW


def read_bytes(conn, size):
    data = b''
    while len(data) < size:
        packet = conn.recv(size - len(data))
        if not packet:
            return None
        data += packet
    return data


def listen_for_msgs(client_socket):
    while True:
        try:
            header = read_bytes(client_socket, 6)
            if not header:
                break
            msg_type, subtype, length, sublen = struct.unpack('>BBHH', header)
            data = read_bytes(client_socket, length) if length > 0 else b''

            if msg_type == SEND_MSG:
                routing_prefix = data[:sublen]
                message_content = data[sublen:].decode()

                parts = routing_prefix.split(b'\0')
                if len(parts) >= 2:
                    sender = parts[1].decode()
                    print(f"\n>>> [FROM {sender}]: {message_content} <<<")
                else:
                    print(f"\n>>> [INCOMING]: {message_content} <<<")

            elif msg_type == ERROR_MSG:
                print(f"\n!!!> [ERROR]: {data.decode()} <!!!")

            # --- NEW: Catch and display System Updates ---
            elif msg_type == SYS_MSG:
                print(f"\n***> [SYSTEM]: {data.decode()} <***")

        except Exception:
            print("\n!!!> Connection closed by server. Press enter to exit. <!!!")
            break


def send_msg(conn, msg_type, subtype, sublen, data):
    header = struct.pack('>BBHH', msg_type, subtype, len(data), sublen)
    conn.sendall(header + data)


def main():
    client = None

    # --- CONNECTION & AUTHENTICATION LOOP ---
    while True:
        print("\n===> Enter Server Details <===")
        host = input("Enter server IP (e.g., 127.0.0.1): ")
        try:
            port = int(input("Enter server Port (e.g., 5000): "))
        except ValueError:
            print("!!!> Invalid port number. Try again. <!!!")
            continue

        username = input("Enter your username: ")

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            client.connect((host, port))
            send_msg(client, IDENTIFY, 1, 0, username.encode())

            header = read_bytes(client, 6)
            if not header:
                print("!!!> Connection closed by server before authentication. <!!!")
                client.close()
                continue

            msg_type, subtype, length, sublen = struct.unpack('>BBHH', header)
            data = read_bytes(client, length) if length > 0 else b''

            if msg_type == AUTH_RESP:
                if subtype == 0:
                    print(f"!!!> Registration Failed: {data.decode()} <!!!")
                    print("!!!> Please choose a different username and try again. <!!!")
                    client.close()
                    continue
                elif subtype == 1:
                    print(f"***> Connected to {host}:{port}. {data.decode()} <***")
                    break

        except Exception as e:
            print(f"!!!> Connection error: {e} <!!!")
            print("!!!> Retrying connection setup... <!!!")
            if client:
                client.close()
            continue

    # --- CHAT LOOP ---
    threading.Thread(target=listen_for_msgs, args=(client,), daemon=True).start()

    print("===> To send a message type: <receiver_name>:<message> <===")
    print("===> To disconnect, type: exit <===")

    try:
        while True:
            user_input = input()

            if user_input.strip().lower() == 'exit':
                print("<<<> Sending disconnect signal to server... <<<>")
                send_msg(client, DISCONNECT, 0, 0, b'')
                break

            if ":" in user_input:
                receiver, msg = user_input.split(":", 1)
                receiver_bytes = receiver.encode()
                msg_bytes = msg.encode()

                data = receiver_bytes + msg_bytes
                sublen = len(receiver_bytes)

                send_msg(client, SEND_MSG, 0, sublen, data)
            else:
                print("!!!> Invalid format. Use <receiver_name>:<message> <!!!")
    finally:
        client.close()


if __name__ == '__main__':
    main()
import socket
import struct
import threading

# Frames use a six-byte header, followed by at most 65535 payload bytes.
MAX_PAYLOAD = 65535

IDENTIFY = 2
SEND_MSG = 3
CLIENT_CONNECTION = 1
CLIENT_MESSAGE = 0
MESSAGE_SIGNS = {
    IDENTIFY: "#$",
    SEND_MSG: "$!",
}


def log_message(direction, msg_type, subtype):
    """Print the marker for a sent or received protocol message."""
    sign = MESSAGE_SIGNS.get(msg_type, "?!")
    print(f"{sign} [{direction} TYPE {msg_type}:{subtype}]", flush=True)


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


def send_message(conn, msg_type, subtype, sublen, data):
    """Encode and send one complete protocol frame."""
    if len(data) > MAX_PAYLOAD:
        raise ValueError(f"Payload exceeds {MAX_PAYLOAD} bytes")
    conn.sendall(struct.pack(">BBHH", msg_type, subtype, len(data), sublen) + data)
    log_message("SEND", msg_type, subtype)


def listen_for_messages(conn):
    """Print incoming chat messages while the input loop is running."""
    try:
        while True:
            message = recv_message(conn)
            if message is None:
                print("\n[CLIENT] Server closed the connection.")
                return
            msg_type, subtype, sublen, data = message
            log_message("RECV", msg_type, subtype)
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
        print(f"\n[CLIENT] Receive error: {error}")


def main():
    """Connect to a server, identify the client, and run the chat input loop."""
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
    except OSError as error:
        conn.close()
        print(f"Could not connect to {host}:{port}: {error}")
        return

    print(f"[CONNECTED] {host}:{port} as {username}.")
    threading.Thread(target=listen_for_messages, args=(conn,), daemon=True).start()
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
            with send_lock:
                send_message(
                    conn,
                    SEND_MSG,
                    CLIENT_MESSAGE,
                    len(recipient_bytes),
                    recipient_bytes + content_bytes,
                )
    except (OSError, ValueError) as error:
        print(f"[CLIENT] Send error: {error}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

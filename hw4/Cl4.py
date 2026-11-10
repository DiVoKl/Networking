import socket
import struct
import threading
import time

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
    IDENTIFY: "#$",
    SEND_MSG: "$!",
    ECHO: "%^",
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


def send_message(conn, msg_type, subtype, sublen, data, log=True):
    """Encode and send one complete protocol frame."""
    if len(data) > MAX_PAYLOAD:
        raise ValueError(f"Payload exceeds {MAX_PAYLOAD} bytes")
    conn.sendall(struct.pack(">BBHH", msg_type, subtype, len(data), sublen) + data)
    if log:
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
    finally:
        conn.close()


def probe_server(host, port, username, existing_conn=None):
    """Connect to a server if needed and return its measured RTT and socket."""
    conn = existing_conn
    try:
        if conn is None:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(PROBE_TIMEOUT)
        if existing_conn is None:
            conn.connect((host, port))
            send_message(conn, IDENTIFY, CLIENT_CONNECTION, 0, username.encode("utf-8"))

        log_message("SEND", ECHO, ECHO_REQUEST)
        started = time.perf_counter()
        send_message(conn, ECHO, ECHO_REQUEST, 0, ECHO_PROBE, log=False)
        response = recv_message(conn)
        elapsed = time.perf_counter() - started
        if response is None:
            raise ConnectionError("Server closed before echo response")
        msg_type, subtype, sublen, data = response
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
    return best_conn


def append_probe_result(results, results_lock, result, port):
    """Store a successful probe result for the selection step."""
    if result is None:
        return
    rtt, conn = result
    with results_lock:
        results.append((rtt, conn, port))


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
        conn = discover_and_select(host, port, username, conn)
    except OSError as error:
        conn.close()
        print(f"Could not connect to {host}:{port}: {error}")
        return
    except (ValueError, struct.error) as error:
        conn.close()
        print(f"Server discovery failed: {error}")
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

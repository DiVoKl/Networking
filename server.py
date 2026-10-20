import socket

UDP_IP = '0.0.0.0'
UDP_PORT = 9999  # Server runs on port 9999
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
sock.bind((UDP_IP, UDP_PORT))

users = {}  # Dictionary: key = name, value = (port - 1)
known_addrs = set()  # Set to track known sender addresses

print("Server is listening...")

while True:
    data, addr = sock.recvfrom(1024)
    msg = data.decode()

    # Check if sender is known
    if addr not in known_addrs:
        # Registration: save key(name) -> value(port - 1)
        users[msg] = addr[1] - 1
        known_addrs.add(addr)
        print(f"Registered user: {msg} with port: {users[msg]}")
    else:
        # Message format: "recipient_name content"
        parts = msg.split(' ', 1)
        dest_name = parts[0]
        content = parts[1] if len(parts) > 1 else ""

        # Look up recipient in dictionary
        if dest_name in users:
            dest_port = users[dest_name]
            # Forward message to recipient port
            sock.sendto(content.encode(), (addr[0], dest_port))
        else:
            # Send error message if recipient does not exist
            sock.sendto("אין כזה משתמש".encode(), addr)
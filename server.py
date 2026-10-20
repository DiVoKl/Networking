import socket
UDP_IP = '0.0.0.0'  # set ip
UDP_PORT = 9999     # set port
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0) # creates IPv4 UDP socket
sock.bind((UDP_IP, UDP_PORT)) # binds the socket to the specified IP and port
while True:
    data, addr = sock.recvfrom(1024) #waits to recieve data and sender address
    print('received message:', data.decode()) # Decodes the byte data into a string and prints it
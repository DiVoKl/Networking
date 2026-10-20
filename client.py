from threading import Thread # Imports Thread for background tasks
import socket # Imports socket module
import sys # Imports sys for terminal input
server_addr = ('127.0.0.1' , 9999) # Sets server IP address & server port
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0) # Creates IPv4 UDP socket
my_name = input('Enter your name: ') # Prompts for user name
sock.sendto(my_name.encode(), server_addr) # Sends name to server
def output_recvfrom(sock): # Defines receive function
    while True: # Continuous listening loop
        data, _ = sock.recvfrom(1024) # Receives up to 1024 bytes
        if not data: break # Exits if no data
        print(data.decode()) # Prints decoded message
    x = Thread(target=output_recvfrom, args=(sock, )) # Creates receive thread
    x.start() # Starts thread
    for line in sys.stdin: # Reads terminal input
        sock.sendto(line.strip().encode(), server_addr) # Sends typed message
    sock.close() # Closes socket
    x.join() # Waits for thread
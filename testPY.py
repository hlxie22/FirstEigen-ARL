from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
print('Done')
'''
import socket
import time

HOST = "localhost"
PORT = 8080
 
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

message = "[CLIENT] Test request"
print(message)
message = message.encode("utf-8")

#time.sleep(3)
sock.send(message)

#print(sock.recv(2**0).decode('utf-8'))
'''
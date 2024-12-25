
#this is for continous streaming of the data from sentences.txt file 
import socket
import time
import random
import string

# This is to generate random k-mer sentences
def ge_kmer_sen_ko(num_kmers, k=3):
    return ''.join(''.join(random.choice(string.ascii_lowercase) for _ in range(k)) for _ in range(num_kmers))

#sending the data to the port (connection)
HOST = "localhost"
PORT = 9999
NUM_KMERS = 10
INTERVAL = 10  

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Streaming on {HOST}:{PORT}")
    conn_ko, addr_ko = s.accept()
    with conn_ko:
        print(f"Connection established with {addr_ko}")
        while True:
            sentence = ge_kmer_sen_ko(NUM_KMERS)
            conn_ko.sendall((sentence + "\n").encode("utf-8"))
            print(f"Sent: {sentence}")
            time.sleep(INTERVAL)

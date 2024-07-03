import socket
import time
import threading


def main():
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(5)
    print("server listening")

    while True:
        client, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client, ))
        client_handler.start()

def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024)
        if data == b'*1\r\n$4\r\nPING\r\n':
            client_socket.send(b"+PONG\r\n")


if __name__ == "__main__":
    main()

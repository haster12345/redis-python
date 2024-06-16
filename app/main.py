import socket
import time


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(5)
    print("server listening")

    while True:
        print("check 1")
        client, addr = server_socket.accept()
        print("check 2")
        while True:
            data = client.recv(1024)
            print(data)
            if data == b'*1\r\n$4\r\nPING\r\n':
                client.send(b"+PONG\r\n")

        # client.close()

    # server_socket.close()


if __name__ == "__main__":
    main()

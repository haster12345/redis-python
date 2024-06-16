import socket


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.accept() # wait for client
    client, addr = server_socket.accept()
    print("check1")
    data = client.recv(1024)
    if data == "PING":
        print(1)
        client.send(b"+PONG\r\n")

    client.close()
    server_socket.close()


if __name__ == "__main__":
    main()

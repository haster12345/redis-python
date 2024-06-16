import socket


def main():
    print("Logs from your program will appear here!")

    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # sock.bind(("localhost", 6379))
    server_socket.listen(1)

    print("check1")
    client, addr = server_socket.accept()
    print("check2")
    data = client.recv(1024)
    if data == "PING":
        print("data recieved")
        client.send(b"+PONG\r\n")

    client.close()
    server_socket.close()


if __name__ == "__main__":
    main()

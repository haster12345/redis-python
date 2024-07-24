import socket
import time
import threading
from typing import List


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
    store = {}
    while True:
        data = client_socket.recv(1024)
        data_parse = parse_resp(data)

        print(data_parse)

        if not data_parse:
            return

        if data_parse[0].lower() == 'ping':
            client_socket.send(b"+PONG\r\n")

        elif data_parse[0].lower() == 'echo':
            print("$%s\r\n%s\r\n" % (len(data_parse[1]), data_parse[1]))
            client_socket.send(
                b"$%s\r\n%s\r\n" %
                (
                    str.encode(f"{len(data_parse[1])}"),
                    str.encode(data_parse[1])
                )
            )

        elif data_parse[0].lower() == 'set':
            try:
                if data_parse[3].lower() == 'px':
                    exp_time = time.time_ns() + int(data_parse[4]) * 10 ** 6
                    print(exp_time)
                    store[data_parse[1]] = (data_parse[2], exp_time)
                    client_socket.send(b"+OK\r\n")
                else:
                   assert False, "Invalid Argument"

            except IndexError:
                store[data_parse[1]] = (data_parse[2], )
                client_socket.send(b"+OK\r\n")

        elif data_parse[0].lower() == 'get':
            value = get_value(key=data_parse[1], store=store)

            if value == "$-1\r\n":
                client_socket.send(b"$-1\r\n")

            else:
                client_socket.send(
                        b"$%s\r\n%s\r\n" %
                        (
                            str.encode(f"{len(value)}"),
                            str.encode(value)
                        )
                    )
        else:
            assert False, "Invalid Command"


def parse_resp(data):
        str_data = data.decode('utf-8')
        if data == '*1\r\n$4\r\nPING\r\n':
           return ['PING']

        if str_data[0] == '*':
            parsed_array_out = parse_array(str_data)
            return parsed_array_out

        elif str_data[0] == '$':
            parsed_str = parse_bulk_str(str_data)
            return parsed_str
        else:
            assert "Not Implemented"


def parse_array(data):
    data_split = data.split('\r\n')
    n_elements = int(data_split[0][1:])
    return [data_split[2*(i+1)] for i in range(n_elements)]


def get_value(key, store):
    try:
        if len(store[key]) == 1:
            return str(store[key][0])
        else:
            if time.time_ns() > store[key][1]:
                print('delete time: ',time.time_ns())
                store.pop(key)
                return "$-1\r\n"
            else:
                return str(store[key][0])
    except KeyError:
        return "$-1\r\n"


def parse_bulk_str(data):
    pass

if __name__ == "__main__":
    main()

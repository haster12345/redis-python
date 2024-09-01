import socket
from typing import Any
import secrets
import argparse
import time
import threading


def handshake(host, port):
    client = socket.create_connection((host, int(port)))
    client.send(b"*1\r\n$4\r\nPING\r\n")
    addr = host
    print(f"Connected to Master {addr}")
    return


def main(port: int, replicaoff: str):
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    server_socket.listen(5)
    print("server listening")

    if not server_socket:
        return

    if replicaoff:
        replica_host, replica_port = replicaoff.split(" ")
        assert bool(replica_host and replica_port), "both replica host and replica port must be given"
        handshake(replica_host, replica_port)

    while True:
        client, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client, replicaoff))
        client_handler.start()


def ping(socket: socket.socket):
    socket.send(b"+PONG\r\n")
    return


def echo(socket: socket.socket, data:str):
    socket.send(
        b"$%s\r\n%s\r\n" %
        (
            str.encode(f"{len(data)}"),
            str.encode(data)
        )
    )
    return


def info_replication(socket:socket.socket, replicaoff):
    if not replicaoff:
        alpnum_string = secrets.token_hex(40)
        role_resp = "role:master"
        replid_resp = "master_replid:%s" % alpnum_string
        repk_resp = "master_repl_offset:0"

        value = "\r".join([role_resp, replid_resp, repk_resp])
        total_lenght = len(value.encode())

        socket.send(
            str.encode(
                f"${total_lenght}\r\n{value}\r\n"
            )
        )

    else:
        socket.send(b"$10\r\nrole:slave\r\n")


def handle_client(client_socket, replicaoff):
    store = {}
    while True:
        data = client_socket.recv(1024)
        data_parse = parse_resp(data)

        if not data_parse:
            return

        if data_parse[0].lower() == 'ping':
            ping(client_socket)

        elif data_parse[0].lower() == 'echo':
            echo(client_socket, data_parse[1])

        elif data_parse[0].lower() == 'set':
            try:
                if data_parse[3].lower() == 'px':
                    exp_time = time.time_ns() + int(data_parse[4]) * 10 ** 6
                    store[data_parse[1]] = (data_parse[2], exp_time)
                    client_socket.send(b"+OK\r\n")
                else:
                    assert False, "Invalid Argument"

            except IndexError:
                store[data_parse[1]] = (data_parse[2],)
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

        elif data_parse[0].lower() == "info":
            if data_parse[1].lower() == "replication":
                info_replication(client_socket, replicaoff)

        else:
            break


def parse_resp(data):
    str_data = data.decode('utf-8')
    if data == '*1\r\n$4\r\nPING\r\n':
        return ['PING']

    elif str_data[0] == '*':
        parsed_array_out = parse_array(str_data)
        return parsed_array_out

    elif str_data[0] == '$':
        parsed_str = parse_bulk_str(str_data)
        return parsed_str

    else:
        assert "Unreachable"


def parse_array(data):
    data_split = data.split('\r\n')
    n_elements = int(data_split[0][1:])
    return [data_split[2 * (i + 1)] for i in range(n_elements)]


def get_value(key, store):
    try:
        if len(store[key]) == 1:
            return str(store[key][0])
        else:
            if time.time_ns() > store[key][1]:
                store.pop(key)
                return "$-1\r\n"
            else:
                return str(store[key][0])
    except KeyError:
        return "$-1\r\n"


def parse_bulk_str(data):
    pass


if __name__ == "__main__":
    port = 6379
    replicaoff = ""
    parser = argparse.ArgumentParser(
        description="Python REDIS"
    )
    parser.add_argument('--port', default=port, type=int)
    parser.add_argument('--replicaoff', default=replicaoff, type=str)
    args = parser.parse_args()

    main(port=args.port, replicaoff=args.replicaoff)

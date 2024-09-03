import socket
import secrets
import argparse
import time
import threading


def handshake(host, port, master_port):
    port = int(port)
    client = socket.create_connection((host, port))
    client.send(b"*1\r\n$4\r\nPING\r\n")
    addr = host
    print(f"Connected to Master {addr}")
    while True:
        data = client.recv(1024)
        if not data:
            continue
        elif data == b'+PONG\r\n':
            client.send(
                b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n"
                % str.encode(f"{master_port}")
            )
            client.send(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            resp_1 = client.recv(1024)
            resp_2 = client.recv(1024)
            if (resp_1 and resp_2) == b"+OK\r\n":
                client.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
                break
    return


def ping(socket: socket.socket):
    socket.send(b"+PONG\r\n")
    return


def echo(socket: socket.socket, data: str):
    socket.send(
        b"$%s\r\n%s\r\n" %
        (str.encode(f"{len(data)}"), str.encode(data))
    )
    return


def info_replication(socket: socket.socket, replicaoff):
    if not replicaoff:
        alpnum_string = secrets.token_hex(40)
        role_resp = "role:master"
        replid_resp = "master_replid:%s" % alpnum_string
        repk_resp = "master_repl_offset:0"

        value = "\r".join([role_resp, replid_resp, repk_resp])
        total_length = len(value.encode())

        socket.send(
            str.encode(f"${total_length}\r\n{value}\r\n")
        )

    else:
        socket.send(b"$10\r\nrole:slave\r\n")


class Store:
    def __init__(self, replicas: list):
        self.replicas = replicas

    store = {}

    def set_value(self, key: str, value: str, client: socket.socket, argument=None, px="", is_rep=False):
        if argument:
            if argument.lower() == 'px':
                exp_time = time.time_ns() + int(px) * 10 ** 6
                self.store[key] = (value, exp_time)
                if not is_rep:
                    client.send(b"+OK\r\n")
            else:
                assert False, "Invalid Argument"
        else:
            self.store[key] = (value,)
            if not is_rep:
                client.send(b"+OK\r\n")

        for replica_client in self.replicas:
            out_st = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
            replica_client.send(str.encode(out_st))

        return

    def get_value(self, key):
        try:
            if len(self.store[key]) == 1:
                return str(self.store[key][0])
            else:
                if time.time_ns() > self.store[key][1]:
                    self.store.pop(key)
                    return "$-1\r\n"
                else:
                    return str(self.store[key][0])
        except KeyError:
            return "$-1\r\n"

    def get(self, key: str, client: socket.socket):
        value = self.get_value(key=key)
        if value == "$-1\r\n":
            client.send(b"$-1\r\n")
        else:
            client.send(
                b"$%s\r\n%s\r\n" %
                (str.encode(f"{len(value)}"), str.encode(value))
            )
        return


def handle_client(client_socket: socket.socket, replicaoff, addr, store: Store):
    _store = store
    while True:
        data = client_socket.recv(1024)
        data_parse = parse_resp(data)
        print(data_parse)

        if not data_parse:
            return

        if data_parse[0].lower() == 'ping':
            ping(client_socket)

        elif data_parse[0].lower() == 'echo':
            echo(client_socket, data_parse[1])

        elif data_parse[0].lower() == 'set':
            try:
                _store.set_value(key=data_parse[1], value=data_parse[2], client=client_socket, argument=data_parse[3],
                                 px=data_parse[4], is_rep=bool(replicaoff))
            except IndexError:
                _store.set_value(key=data_parse[1], value=data_parse[2], client=client_socket, is_rep=bool(replicaoff))

        elif data_parse[0].lower() == 'get':
            _store.get(key=data_parse[1], client=client_socket)

        elif data_parse[0].lower() == "info":
            if data_parse[1].lower() == "replication":
                info_replication(client_socket, replicaoff)

        elif data_parse[0].lower() == "replconf":
            if data_parse[1] == "listening-port":
                rec_handshake_listening_port(client_socket, data_parse[2])
            if data_parse[1] == "capa":
                rec_handshake_capa(client_socket, data_parse[2])

        elif data_parse[0].lower() == "psync":
            _store.replicas.append(client_socket)
            psync(client_socket)

        else:
            break


def psync(client: socket.socket):
    rep_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    client.send(
        b"+FULLRESYNC %s 0\r\n" % str.encode(rep_id)
    )
    empty_rdb_hex = ("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26"
                     "d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

    rdb_content = bytes.fromhex(empty_rdb_hex)
    rdb_data = f"${len(rdb_content)}\r\n".encode()
    client.send(rdb_data + rdb_content)
    return 0


def rec_handshake_listening_port(socket: socket.socket, port):
    socket.send(b"+OK\r\n")
    return


def rec_handshake_capa(socket: socket.socket, capa):
    socket.send(b"+OK\r\n")
    return


def parse_resp(data):
    str_data = data.decode('utf-8')

    if not str_data:
        return []

    elif data == '*1\r\n$4\r\nPING\r\n':
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


def parse_bulk_str(data):
    pass


def main(port: int, replicaoff: str):
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    server_socket.listen(5)
    print("server listening")

    if not server_socket:
        return

    if replicaoff:
        replica_host, replica_port = replicaoff.split(" ")
        assert bool(replica_host and replica_port), "Both replica host and replica port must be given"
        handshake(replica_host, replica_port, port)

    store = Store(replicas=[])

    while True:
        client, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client, replicaoff, addr, store))
        client_handler.start()


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

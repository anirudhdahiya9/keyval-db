import shlex
import zmq
import argparse
from utils import CommandParser
import sys


class ClientSession:
    def __init__(self, args):
        self.__known_commands = {'SELECT', 'DESELECT', 'GET', 'SET', 'EXPIRE', 'TTL', 'DEL', 'ZADD', 'ZRANK', 'ZRANGE',
                                 'EXIT'}
        self.__parsers = {}
        self.__init_parsers()
        self.server_host = args.server_host
        self.server_port = args.server_port
        self.__context = None
        self.__socket = None

    def __init_parsers(self):
        for command in self.__known_commands:
            self.__parsers[command] = CommandParser(command)

    def __validate_cmd(self, cmd):
        command = shlex.split(cmd, comments=True)
        if command == [] or command[0] not in self.__known_commands:
            print(f'Unrecognized Command')
            print(f'The known commands are:')
            print(' '.join(self.__known_commands))
            return None, None
        else:
            parsed_args = self.__parsers[command[0]].parse(command[1:])
            if parsed_args is not None:
                return command, parsed_args
            else:
                return None, None

    def connect(self):
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.REQ)
        self.__socket.connect(f"tcp://{self.server_host}:{self.server_port}")

    def __process_command(self, user_input):
        self.__socket.send_string(user_input)
        message = self.__socket.recv_string()
        return message

    def shell(self):
        if not self.__socket:
            self.connect()
        prompt = 'Redis> '

        while True:
            user_input = input(prompt)
            validated_cmd, parsed_args = self.__validate_cmd(user_input)
            if validated_cmd is None:
                continue
            if validated_cmd=='EXIT':
                sys.exit(0)

            output = self.__process_command(user_input)
            if output:
                print(str(output))


def main(args):
    session = ClientSession(args)
    session.shell()


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Redis Client: A python implementation for simple Redis-like '
                                                 'database engine.')
    parser.add_argument('--server_host', default='localhost', help='Host Address for the server.')
    parser.add_argument('--server_port', default=5698, type=int, help='Host Port for the server.')

    args = parser.parse_args()
    main(args)

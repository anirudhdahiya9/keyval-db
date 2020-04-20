from main import CommandParser
import shlex
import zmq


class ClientSession:
    def __init__(self, port):
        self.__known_commands = {'SELECT', 'DESELECT', 'GET', 'SET', 'EXPIRE', 'TTL', 'DEL', 'ZADD', 'ZRANK', 'ZRANGE'}
        self.__parsers = {}
        self.__init_parsers()
        self.server_port = port
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
        self.__socket.connect("tcp://localhost:%s" % self.server_port)

    def __process_command(self, user_input):
        self.__socket.send_string(user_input)
        message = self.__socket.recv_string()
        return message

    def shell(self):
        prompt = 'Redis> '

        while True:
            user_input = input(prompt)
            validated_cmd, parsed_args = self.__validate_cmd(user_input)
            if validated_cmd is None:
                continue

            output = self.__process_command(user_input)
            if output:
                print(str(output))


def main(args):
    portnum = 8234
    session = ClientSession(portnum)
    session.connect()
    session.shell()


if __name__=="__main__":
    #USE PARSER TO GET SERVER PORT, IP IF YOU WANT
    main(None)

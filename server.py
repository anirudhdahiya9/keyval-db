from main import *
import zmq
import argparse


class ServerSession:

    def __init__(self, args):
        self.__port = args.port
        self.__session = Session(args)

    def serve_request(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % self.__port)

        while True:
            message = socket.recv_string()
            print("Received request: ", message)
            validated_cmd, parsed_args = self.__session.validate_cmd(message)
            if validated_cmd is None:
                continue
            output = self.__session.process_command(validated_cmd[0], parsed_args)

            socket.send_string(str(output))


def main(args):
    session = ServerSession(args)
    session.serve_request()


if __name__=="__main__":
    parser = argparse.ArgumentParser(description='A python implementation for simple Redis-like database engine.')
    parser.add_argument('--database_path', type=str, default='databases')
    parser.add_argument('--log_path', type=str, default='logs')
    parser.add_argument('--RDB_persistence', type=bool, default=True, help="True if RDB persistence needed.")
    parser.add_argument('--RDB_timeout', default=30, type=int, help="Save dataset state every x minutes")
    parser.add_argument('--AOF_persistence', type=bool, default=True, help="True if AOF persistence needed.")
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--port', default=8234, type=int, help='port to serve at')
    main_args = parser.parse_args()

    main(main_args)


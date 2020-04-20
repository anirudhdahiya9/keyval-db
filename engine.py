import argparse
import logging
import shlex
import time
import _pickle as pickle
from multiprocessing import Process, Lock
import os
import shutil
from sortedcontainers import SortedSet
import sys
from datastructures import MySortedSet, Value
from utils import CommandParser
from database import Database


def rdb_serialize(cur_database, dump_path, lock):
    """
    Function call to serialize database snapshot
    Args:
        cur_database: copy of data snapshot
        dump_path: full path to file where data to be serialized
        lock: multiprocessing.Lock object to prevent race conditions while serializing.
    """

    lock.acquire()
    with open(dump_path+'.new', 'wb') as f:
        pickle.dump(cur_database, f)
    shutil.copy(dump_path+'.new', dump_path)
    os.remove(dump_path+'.new')
    lock.release()


# TODO: To enable multiple server sessions, add a check if any other session using the same dataset
# TODO: Make singleton class of database
def init_database(name, log_path, dump_path):
    log_path = os.path.join(log_path, name) + '.log'
    dump_path = os.path.join(dump_path, name) + '.rdb'
    database = Database.get_instance(name, log_path, dump_path)
    return database


class Session:
    """
    Session object for the Redis Server Instance.
    Takes in various arguments from main function
    Exposes a shell to access server functionality
    """
    def __init__(self, main_args):

        self.persistence_timeout = None
        self.__known_commands = {'SELECT', 'DESELECT', 'GET', 'SET', 'EXPIRE', 'TTL', 'DEL', 'ZADD', 'ZRANK', 'ZRANGE',
                                 'EXIT'}
        self.__cur_database = None

        self.__command_processors = {
            'GET': self.__cmd_get,
            'SET': self.__cmd_set,
            'EXPIRE': self.__cmd_expire,
            'SELECT': self.__cmd_select,
            'DESELECT': self.__cmd_deselect,
            'TTL': self.__cmd_ttl,
            'DEL': self.__cmd_del,
            'ZADD': self.__cmd_zadd,
            'ZRANK': self.__cmd_zrank,
            'ZRANGE': self.__cmd_zrange,
            'EXIT': self.__cmd_exit
        }

        self.__parsers = {}
        self.__init_parsers()

        self.__log_path = main_args.log_path
        self.__dump_path = main_args.database_path

        self.last_save = time.time()

        self.RDB_persistence = main_args.RDB_persistence
        if main_args.debug:
            self.RDB_timeout = main_args.RDB_timeout
        else:
            self.RDB_timeout = main_args.RDB_timeout*60.0
        self.AOF_persistence = main_args.AOF_persistence

        self.lock = Lock()

    def __init_parsers(self):
        for command in self.__known_commands:
            self.__parsers[command] = CommandParser(command)

    def __cmd_exit(self, args):
        sys.exit(0)

    def __cmd_zrank(self, args):
        try:
            return self.__cur_database.zrank(args.key, args.member)
        except KeyError:
            return '(nil)'
        except Exception as e:
            return f"Error: {e}"

    def __cmd_zrange(self, args):
        try:
            return self.__cur_database.zrange(args.key, args)
        except KeyError:
            return '(nil)'
        except Exception as e:
            return f"Error: {e}"

    def __cmd_zadd(self, args):
        return self.__cur_database.zadd(args.key, args)

    def __cmd_del(self, args):
        for key in args.keys:
            self.__cur_database.delete(key)

    def __cmd_ttl(self, args):
        return self.__cur_database.ttl(args.key)

    def __cmd_get(self, args):
        try:
            return self.__cur_database.get(args.key)
        except KeyError:
            return '(nil)'
        except Exception as e:
            return f"Error: {e}"

    def __cmd_set(self, args):
        if self.__cur_database:
            try:
                return self.__cur_database.set(args.key, args.value, args)
            except KeyError:
                return '(nil)'
            except Exception as e:
                return f"Error: {e}"

        else:
            return 'Error: No dataset currently loaded.'

    def __cmd_expire(self, args):
        try:
            return self.__cur_database.expire(args.key, args.seconds)
        except KeyError:
            return '(nil)'
        except Exception as e:
            return f"Error: {e}"

    def __cmd_select(self, args):
        if self.__cur_database is not None:
            return f'Error: dataset `{self.__cur_database.name}` currently in use, cannot use multiple datasets.'
        else:
            self.restore(args.db_name)
            return f"Loaded Dataset `{self.__cur_database.name}`"

    def __cmd_deselect(self, args):
        if self.__cur_database is None:
            return 'Error: No database currently loaded'
        else:
            self.__rdb_routine()
            self.__cur_database = None

    def process_command(self, cmd, parsed_args):
        return self.__command_processors[cmd](parsed_args)

    def validate_cmd(self, cmd):
        command = shlex.split(cmd, comments=True)
        if command == [] or command[0] not in self.__known_commands:
            ret_val = f'Unrecognized Command\n' + f'The known commands are:\n' + ' '.join(self.__known_commands)
            return None, ret_val
        else:
            if self.__cur_database is None and command[0] not in ('EXIT', 'SELECT'):
                ret_val = f"Select a database first before running operations."
                return None, ret_val
            parsed_args = self.__parsers[command[0]].parse(command[1:])
            if parsed_args is not None:
                return command, parsed_args
            else:
                return None, None

    def __rdb_routine(self):

        if self.__cur_database is None:
            return

        # Store old logs for safety, renew current logs from scratch
        self.__cur_database.backup_logs()

        # Hand over data to child process, note that this is still suboptimal
        child = Process(target=rdb_serialize, args=(self.__cur_database.data, self.__cur_database.dump_path, self.lock))
        child.start()

        os.remove(self.__cur_database.log_path+'.bkp')
        self.last_save = time.time()
        if self.debug:
            print(f'RDB started at {self.last_save}')

    def restore(self, name):

        if name+'.rdb' in os.listdir(self.__dump_path):
            rdb_data = pickle.load(open(os.path.join(self.__dump_path, name+'.rdb'), 'rb'))
        else:
            rdb_data = {}

        self.__cur_database = init_database(name, self.__log_path, self.__dump_path)
        self.__cur_database.data = rdb_data
        self.__cur_database.backup_logs()

        if name + '.log.bkp' in os.listdir(self.__log_path):
            with open(os.path.join(self.__log_path, name+'.log.bkp')) as f:
                for line in f:
                    command = ' '.join(line.strip().split()[3:])
                    validated_cmd, parsed_args = self.validate_cmd(command)
                    if validated_cmd is None:
                        continue
                    _ = self.process_command(validated_cmd[0], parsed_args)
            os.remove(self.__cur_database.log_path+'.bkp')
        else:
            print('Error: Could not load previous log file.')

        return

    def shell(self):
        prompt = 'Redis> '

        while True:
            user_input = input(prompt)
            validated_cmd, parsed_args = self.validate_cmd(user_input)
            if validated_cmd is None:
                print(parsed_args)
                continue

            output = self.process_command(validated_cmd[0], parsed_args)
            if output or output == 0:
                print(output)

            if time.time() - self.last_save >= self.RDB_timeout:
                self.__rdb_routine()

    def debug(self):
        prompt = 'Redis> '

        with open('test_commands.txt') as f:
            lines = f.read().strip().split('\n')
        for line in lines:
            print(prompt, line)
            user_input = line
            validated_cmd, parsed_args = self.validate_cmd(user_input)
            if validated_cmd is None:
                continue

            output = self.process_command(validated_cmd[0], parsed_args)
            if output:
                print(output)

        self.__rdb_routine()
        print('hello')


def validate_args(args):
    if not os.path.exists(args.log_path):
        os.makedirs(args.log_path)
    if not os.path.exists(args.database_path):
        os.makedirs(args.database_path)

def serve_shell(args):
    validate_args(args)
    session = Session(args)
    session.shell()
    # session.debug()

def connect_server(args):
    validate_args(args)
    session = Session(args)
    return session


# TODO: Create Logging mechanism for server, polish db logging
# TODO: Journalling
# IDEA: Not all logs required, only log the state changing logs for database
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='A python implementation for simple Redis-like database engine.')
    parser.add_argument('--database_path', type=str, default='databases')
    parser.add_argument('--log_path', type=str, default='logs')
    parser.add_argument('--RDB_persistence', type=bool, default=True, help="True if RDB persistence needed.")
    parser.add_argument('--RDB_timeout', default=30, type=int, help="Save dataset state every x minutes")
    parser.add_argument('--AOF_persistence', type=bool, default=True, help="True if AOF persistence needed.")
    parser.add_argument('--debug', action='store_true')

    main_args = parser.parse_args()

    serve_shell(main_args)

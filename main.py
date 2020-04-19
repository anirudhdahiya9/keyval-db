import argparse
import logging
import shlex
import time
import _pickle as pickle
from multiprocessing import Process
import os
import shutil
import copy
import daemon
from multiprocessing import Lock


def rdb_serialize(cur_database, dump_path, lock):
    lock.acquire()
    with open(dump_path+'.new', 'wb') as f:
        pickle.dump(cur_database, f)
    shutil.copy(dump_path+'.new', dump_path)
    os.remove(dump_path+'.new')
    lock.release()


class Database:
    def __init__(self, name, log_path, dump_path):
        self.name = name
        self.log_path = log_path
        self.dump_path = dump_path
        self.data = {}
        self.logger = None
        self.file_handler = None
        self.setup_logger()

    def setup_logger(self):
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        self.file_handler = logging.FileHandler(self.log_path)
        log_format = logging.Formatter('%(asctime)s %(name)s %(message)s')
        self.file_handler.setFormatter(log_format)
        self.logger.addHandler(self.file_handler)

    def __check_life(self, key):
        val_obj = self.data[key]
        if val_obj.timeout and time.time() > val_obj.timeout:
            self.logger.info(f'DEL {key}')
            del self.data[key]
            return False
        else:
            return True

    def __check_active(self, key):
        if key in self.data and self.__check_life(key):
            return True
        else:
            return False

    def get(self, key):
        #self.logger.info(f'GET {key}')

        if self.__check_active(key):
            return self.data[key].val
        else:
            return '(nil)'

    def set(self, key, val, args):

        active = self.__check_active(key)

        if args.NX and active:
            return
        elif args.XX and not active:
            return

        timeout = None
        if args.KEEPTTL and active:
            timeout = self.data[key].timeout
        else:
            if args.EX:
                timeout = time.time() + args.EX
            elif args.PX:
                timeout = time.time() + 0.001*args.PX

        self.logger.info(f'SET {key} {val} {timeout}')
        self.data[key] = Value(val, timeout)

    def expire(self, key, age):
        if self.__check_active(key):
            timeout = time.time() + int(age)
            #self.logger.info(f'EXPIRE {key} {age} {timeout}')
            self.data[key].timeout = timeout

    def ttl(self, key):
        if self.__check_active(key):
            timeout = self.data[key].timeout
            if timeout:
                return int(timeout - time.time())
            else:
                return '-1'
        else:
            return '-2'

    def delete(self, key):
        try:
            self.logger.info(f'DEL {key}')
            del self.data[key]
        except KeyError:
            pass

    def serialize(self):
        with open(self.dump_path+'.pkl', 'wb') as f:
            pickle.dump(self.data, f)

    def deserialize(self):
        with open(self.dump_path+'.pkl', 'rb') as f:
            self.data = pickle.load(f)

    def backup_logs(self):
        if self.file_handler:
            self.file_handler.close()
            self.logger.removeHandler(self.file_handler)
            shutil.copyfile(self.log_path, self.log_path+'.bkp')
            os.remove(self.log_path)

            self.file_handler = logging.FileHandler(self.log_path)
            log_format = logging.Formatter('%(asctime)s %(name)s %(message)s')
            self.file_handler.setFormatter(log_format)
            self.logger.addHandler(self.file_handler)


class Value:
    def __init__(self, value=None, timeout=None):
        self.val = value
        self.timeout = timeout
        self.type = None


class CommandParser(argparse.ArgumentParser):
    def __init__(self, command):

        super().__init__(prog=command)

        if command == 'SELECT':
            self.add_argument('db_name', help="Identifier for the database")

        elif command == 'DESELECT':
            pass

        elif command == 'GET':
            self.description = "Get the value of key. If the key does not exist the special value nil is returned. An" \
                               " error is returned if the value stored at key is not a string, because GET only " \
                               "handles string values. "
            self.add_argument('key', help="Identifier for the key")

        elif command == 'SET':
            self.description = "Set key to hold the string value.  If key already holds a value, it is overwritten, " \
                               "regardless of its type. Any previous time to live associated with the key is " \
                               "discarded on successful SET operation. "

            self.add_argument('key', type=str, help="Identifier for the key")
            self.add_argument('value', type=str)

            ex_group = self.add_mutually_exclusive_group()
            ex_group.add_argument('-EX', type=int, help='Set the specified expire time, in seconds.')
            ex_group.add_argument('-PX', type=int, help='Set the specified expire time, in milliseconds.')

            ol_group = self.add_mutually_exclusive_group()
            ol_group.add_argument('-NX', action='store_true', help='Only set the key if it does not already exist.')
            ol_group.add_argument('-XX', action='store_true', help='Only set the key if it already exists.')

            self.add_argument('-KEEPTTL', action='store_true', help='Retain the time to live associated with the key.')

        elif command == 'EXPIRE':
            self.description = "Set a timeout on key. After the timeout has expired, the key will automatically be " \
                               "deleted. A key with an associated timeout is often said to be volatile in Redis " \
                               "terminology. "

            self.add_argument('key', help="Identifier for the key.")
            self.add_argument('seconds', help="Time(Seconds) for the key to expire in.")

        elif command == 'TTL':
            self.description = "Returns the remaining time to live of a key that has a timeout. This introspection " \
                               "capability allows a Redis client to check how many seconds a given key will continue " \
                               "to be part of the dataset. "
            self.add_argument('key', help="Identifier for the key.")

        elif command == 'DEL':
            self.description = "Removes the specified keys. A key is ignored if it does not exist."
            self.add_argument('keys', nargs='+', help='Identifier for the key.')


    def error(self, message):
        print(message)
        self.print_usage()
        raise Exception

    def parse(self, cmd_args):
        try:
            parsed_args = self.parse_args(cmd_args)
            return parsed_args
        except:
            #print(self.print_help())
            return None


# TODO: To enable multiple server sessions, add a check if any other session using the same dataset
def init_database(name, log_path, dump_path):
    log_path = os.path.join(log_path, name) + '.log'
    dump_path = os.path.join(dump_path, name) + '.rdb'
    database = Database(name, log_path, dump_path)
    return database


class Session:
    def __init__(self, main_args):
        # known_commands = {'SELECT', 'DESELECT', 'DUMP', 'GET', 'SET', 'EXPIRE', 'ZADD', 'ZRANK', 'ZRANGE'}

        self.persistence_timeout = None
        self.__known_commands = {'SELECT', 'DESELECT', 'GET', 'SET', 'EXPIRE', 'TTL', 'DEL', 'ZADD'}
        self.__cur_database = None

        self.__command_processors = {
            'GET': self.__cmd_get,
            'SET': self.__cmd_set,
            'EXPIRE': self.__cmd_expire,
            'SELECT': self.__cmd_select,
            'DESELECT': self.__cmd_deselect,
            'TTL': self.__cmd_ttl,
            'DEL': self.__cmd_del,
            'ZADD': self.__cmd_zadd
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

    def __cmd_zadd(self, args):
        pass

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
            print(f"Error: {e}")

    def __cmd_set(self, args):
        if self.__cur_database:
            try:
                self.__cur_database.set(args.key, args.value, args)
            except KeyError:
                return '(nil)'
            except Exception as e:
                return f"Error: {e}"

        else:
            return 'Error: No dataset currently loaded.'

    def __cmd_expire(self, args):
        try:
            self.__cur_database.expire(args.key, args.seconds)
        except KeyError:
            return '(nil)'
        except Exception as e:
            print(f"Error: {e}")

    def __cmd_select(self, args):
        if self.__cur_database is not None:
            print(f'Error: dataset `{self.__cur_database.name}` currently in use, cannot use multiple datasets.')
        else:
            self.__cur_database = init_database(args.db_name, self.__log_path, self.__dump_path)
            print(f"Loaded Dataset `{self.__cur_database.name}`")

    def __cmd_deselect(self, args):
        if self.__cur_database is None:
            print('Error: No database currently loaded')
        else:
            self.__cur_database.serialize()
            self.__cur_database = None

    def __process_command(self, cmd, parsed_args):
        return self.__command_processors[cmd](parsed_args)

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

    def __rdb_routine(self):

        if self.__cur_database is None:
            return

        # Store old logs for safety, renew current logs from scratch
        self.__cur_database.backup_logs()

        # Hand over data to child process, note that this is still suboptimal
        child = Process(target=rdb_serialize, args=(self.__cur_database.data, self.__cur_database.dump_path, self.lock))
        child.start()

        self.last_save = time.time()
        if self.debug:
            print(f'RDB started at {self.last_save}')

    def restore(self, name):

        if name+'.rdb' in os.listdir(self.__dump_path):
            rdb_data = pickle.load(os.path.join(self.__dump_path, name+'.rdb'))
        else:
            rdb_data = {}

        self.__cur_database = init_database(name, self.__log_path, self.__dump_path)
        self.__cur_database.data = rdb_data
        self.__cur_database.backup_logs()

        if name + '.log.bkp' in os.listdir(self.__log_path):
            with open(os.path.join(self.__log_path, name+'.log.bkp')) as f:
                for line in f:
                    command = ' '.join(line.strip().split()[2:])
                    validated_cmd, parsed_args = self.__validate_cmd(command)
                    if validated_cmd is None:
                        continue
                    _ = self.__process_command(validated_cmd[0], parsed_args)
        else:
            print('Error: Could not load previous log file.')

        return


    def shell(self):
        prompt = 'Redis> '

        while True:
            user_input = input(prompt)
            validated_cmd, parsed_args = self.__validate_cmd(user_input)
            if validated_cmd is None:
                continue

            output = self.__process_command(validated_cmd[0], parsed_args)
            if output:
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
            validated_cmd, parsed_args = self.__validate_cmd(user_input)
            if validated_cmd is None:
                continue

            output = self.__process_command(validated_cmd[0], parsed_args)
            if output:
                print(output)

        self.__rdb_routine()


        print('hello')


def main(args):
    session = Session(args)
    session.shell()
    # session.debug()


def validate_args(args):
    if not os.path.exists(args.log_path):
        print('log_path does not exist')
        os.makedirs(args.log_path)
    if not os.path.exists(args.database_path):
        os.makedirs(args.database_path)

# TODO: Create Logging mechanism for server, polish db logging
# TODO: Create persistance mechanism
# TODO: Create Loading from prev dump, journalling
# TODO: Check if expired keys
# TODO: Add comments
# IDEA: Not all logs required, only log the state changing logs for database

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='A python implementation for simple Redis-like database engine.')
    parser.add_argument('--mode', type=str, required=True)
    parser.add_argument('--database_path', type=str, default='databases')
    parser.add_argument('--log_path', type=str, default='logs')
    parser.add_argument('--RDB_persistence', type=bool, default=True, help="True if RDB persistence needed.")
    parser.add_argument('--RDB_timeout', default=30, type=int, help="Save dataset state every x minutes")
    parser.add_argument('--AOF_persistence', type=bool, default=True, help="True if AOF persistence needed.")
    parser.add_argument('--debug', action='store_true')

    main_args = parser.parse_args()

    validate_args(main_args)

    main(main_args)
import os
import shutil
import logging
from multiprocessing import Lock
import time
from datastructures import Value, MySortedSet

db_map = {}
db_lock = Lock()


class Database:
    """
    Database class, holds key `str` value `Value` pairs
    name: name identifier to select, log and dump path
    log_path, dump_path: full paths to log and dump files
    """
    def __init__(self, name, log_path, dump_path):
        self.name = name
        self.log_path = log_path
        self.dump_path = dump_path
        self.data = {}
        self.logger = None
        self.file_handler = None
        self.setup_logger()

    @staticmethod
    def get_instance(name, log_path, dump_path):
        if name not in db_map.keys():
            db_lock.acquire()
            if name not in db_map.keys():
                db_map[name] = Database(name, log_path, dump_path)
            db_lock.release()
        return db_map[name]

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

        if self.__check_active(key):
            return self.data[key].val
        else:
            return '(nil)'

    def set(self, key, val, args):

        active = self.__check_active(key)

        if args.NX and active:
            return '(nil)'
        elif args.XX and not active:
            return '(nil)'

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
        return 'OK'

    def expire(self, key, age):
        if self.__check_active(key):
            timeout = time.time() + int(age)
            self.logger.info(f'EXPIRE {key} {timeout}')
            self.data[key].timeout = timeout
            return 1
        else:
            return 0

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

    def zadd(self, key, args):

        active = self.__check_active(key)

        if active and type(self.data[key].val) != MySortedSet:
            return f"ERR: Value at {key} is not a MySortedSet object."

        if args.NX and active:
            return '(nil)'
        elif args.XX and not active:
            return '(nil)'

        if active:
            if args.INCR:
                ret_val = self.data[key].val.incr_update(args.score_member)
            else:
                ret_val = self.data[key].val.update(args.score_member, args.CH)
        else:
            self.data[key] = Value(MySortedSet())
            ret_val = self.data[key].val.update(args.score_member)

        return ret_val

    def zrank(self, key, member):
        active = self.__check_active(key)

        if not active:
            return '(nil)'
        if active and type(self.data[key].val) != MySortedSet:
            return f"ERR: Value at {key} is not a MySortedSet object."

        return self.data[key].val.rank(member)

    def zrange(self, key, args):
        active = self.__check_active(key)
        if not active:
            return []
        if active and type(self.data[key].val) != MySortedSet:
            return f"ERR: Value at {key} is not a MySortedSet object."

        return self.data[key].val.range(args.start, args.stop, args.WITHSCORES)

    def backup_logs(self):
        # Useful during snapshot serialization, backs up existing log file to name.log.bkp,
        # Reloads the Filehandler to restart logging from scratch in new file
        if self.file_handler:
            self.file_handler.close()
            self.logger.removeHandler(self.file_handler)
            shutil.copyfile(self.log_path, self.log_path+'.bkp')
            os.remove(self.log_path)

            self.file_handler = logging.FileHandler(self.log_path)
            log_format = logging.Formatter('%(asctime)s %(name)s %(message)s')
            self.file_handler.setFormatter(log_format)
            self.logger.addHandler(self.file_handler)

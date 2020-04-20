import argparse


class CommandParser(argparse.ArgumentParser):
    """
    Parser to parse redis commands, loads the parameters in argument namespace objects
    """
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

        elif command == "ZADD":
            self.description = "Adds all the specified members with the specified scores to the sorted set" \
                                "stored at key."

            self.add_argument('key', help="Identifier for the key.")

            ol_group = self.add_mutually_exclusive_group()
            ol_group.add_argument('-NX', action='store_true', help='Only set the key if it does not already exist.')
            ol_group.add_argument('-XX', action='store_true', help='Only set the key if it already exists.')

            self.add_argument('-CH', action='store_true', help='Modify the return value from the number of new '
                                                               'elements added, to the total number of elements '
                                                               'changed (CH is an abbreviation of changed).')

            self.add_argument('-INCR', action='store_true', help='When this option is specified ZADD acts like '
                                                                 'ZINCRBY. Only one score-element pair can be '
                                                                 'specified in this mode.')

            self.add_argument('score_member_pairs', nargs='+', help='Pairs of scores and member identifiers')

        elif command == 'ZRANK':
            self.description = "Returns the rank of member in the sorted set stored at key, with the scores ordered " \
                               "from low to high. The rank (or index) is 0-based, which means that the member with " \
                               "the lowest score has rank 0. "

            self.add_argument('key', help="Identifier for the key.")
            self.add_argument('member', help="Identifier for the sortedset member.")

        elif command == 'ZRANGE':
            self.description = "Returns the specified range of elements in the sorted set stored at key."
            self.add_argument('key', help="Identifier for the key.")
            self.add_argument('start', type=int, help="Starting Index")
            self.add_argument('stop', type=int, help="Last Index")
            self.add_argument('-WITHSCORES', action='store_true', help="Display scores of members")

        elif command == 'EXIT':
            pass

    def error(self, message):
        # Custom Error function to avoid sys exit on parsing errors
        print(message)
        self.print_usage()
        raise Exception

    def __fetch_pair_list(self, arglist):
        # Parses sequence of score key pairs for ZADD command, called from parse function
        if len(arglist) % 2 == 1:
            self.error(f"Score member should be in pairs.")

        pairs_list = []
        for i in range(0, len(arglist)-1, 2):
            try:
                score = float(arglist[i])
            except ValueError:
                self.error(f"Score values should be int or float, not string")
            member = arglist[i+1]
            pairs_list.append((score, member))
        return pairs_list

    def parse(self, cmd_args):
        try:
            parsed_args = self.parse_args(cmd_args)
            if self.prog == 'ZADD':
                parsed_args.score_member = self.__fetch_pair_list(parsed_args.score_member_pairs)

            return parsed_args
        except:
            # print(self.print_help())
            return None

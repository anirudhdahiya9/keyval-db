from sortedcontainers import SortedSet


class MySortedSet:
    """Custom class to abstract redis sorted sets"""
    def __init__(self):
        self.members = SortedSet(key=self.sortedset_key)
        self.scoremap = {}

    def sortedset_key(self, x):
        return self.scoremap[x]

    def update(self, iterable, ch_flag=False):
        # Emulates set update
        scores, members = zip(*iterable)
        exist_count = 0
        for ikey, key in enumerate(members):
            if key in self.scoremap:
                if ch_flag:
                    if self.scoremap[key] == scores[ikey]:
                        exist_count += 1
                else:
                    exist_count += 1
            self.scoremap[key] = scores[ikey]

        for member in members:
            try:
                self.members.remove(member)
            except KeyError:
                continue
            except ValueError:
                continue
        self.members.update(members)

        return len(members) - exist_count

    def incr_update(self, iterable):
        # Increments the scores for already existing keys
        scores, members = zip(*iterable)
        for ikey, key in enumerate(members):
            if key in self.scoremap:
                self.scoremap[key] += scores[ikey]
            else:
                self.scoremap[key] = scores[ikey]
        for member in members:
            try:
                self.members.remove(member)
            except KeyError:
                continue
            except ValueError:
                continue
        self.members.update(members)
        return self.scoremap[members[-1]]

    def rank(self, member):
        try:
            return self.members.index(member)
        except KeyError:
            return '(nil)'

    def range(self, start, end, withscores):
        range_members = self.members[start:end]
        if withscores:
            range_scores = [self.scoremap[member] for member in range_members]
            return list(zip(range_members, range_scores))
        else:
            return range_members


class Value:
    # Holds the value objects and timeouts for Database values
    # timeout: time.time() + age
    def __init__(self, value=None, timeout=None):
        self.val = value
        self.timeout = timeout
        self.type = None

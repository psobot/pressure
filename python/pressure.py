import os
import redis
import socket


class QueueAlreadyExistsError(Exception):
    pass


class QueueDoesNotExistError(Exception):
    pass


class QueueClosedError(Exception):
    pass


class QueueFullError(Exception):
    pass


class QueueInUseError(Exception):
    pass


def requiresQueueToExist(fn):
    def wrapped(self=None, *args, **kwargs):
        if not self.exists():
            raise QueueDoesNotExistError()
        return fn(self, *args, **kwargs)
    return wrapped


class PressureQueue(object):
    def __init__(self, name, prefix='__pressure__', **redis_kwargs):
        self._db = redis.Redis(**redis_kwargs)
        self.name = name
        self.prefix = prefix

        self.keys = {"queue": ":".join([prefix, name])}
        for key in ['bound', 'producer', 'consumer',
                    'producer_free', 'consumer_free',
                    'stats:produced_messages',
                    'stats:produced_bytes',
                    'stats:consumed_messages',
                    'stats:consumed_bytes',
                    'not_full', 'closed']:
            self.keys[key] = ":".join([prefix, name, key])

        self.bound = self._db.get(self.keys['bound'])
        if self.bound is not None:
            self.bound = int(self.bound)
            self._exists = True
        else:
            self._exists = False
        self._closed = self._db.exists(self.keys['closed'])

        self.client_uid = "_pid".join([socket.gethostname(), str(os.getpid())])

    def create(self, bound=None):
        int_bound = int(bound) if bound is not None else 0

        self._exists = not self._db.setnx(self.keys['bound'], int_bound)
        if self._exists:
            raise QueueAlreadyExistsError()

        self._exists = True
        self.bound = bound
        assert self._db.lpush(self.keys['producer_free'], 0) == 1
        assert self._db.lpush(self.keys['consumer_free'], 0) == 1
        assert self._db.lpush(self.keys['not_full'], 0) == 1

    @requiresQueueToExist
    def exists(self):
        self._exists = self._db.exists(self.keys['bound'])
        return self._exists

    @requiresQueueToExist
    def qsize(self):
        return self._db.llen(self.keys['queue'])

    @requiresQueueToExist
    def closed(self):
        if self._closed:
            return True
        else:
            self._closed = self._db.exists(self.keys['closed'])
            return self._closed

    def get(self, block=True, timeout=0):
        if block:
            return self.__get_blocking(timeout=timeout)
        else:
            return self.get_nowait()

    @requiresQueueToExist
    def __get_blocking(self, timeout=0):
        self._db.brpop([self.keys['consumer_free']], 0)
        try:
            self._db.set(self.keys['consumer'], self.client_uid)

            self._closed = self._db.exists(self.keys['closed'])
            if self._closed:
                empty = not self._db.exists(self.keys['queue'])
                if empty:
                    raise QueueClosedError()
                else:
                    result = self._db.brpop([self.keys['queue']], 0)
                    return result[1]

            else:
                result = self._db.brpop([self.keys['queue'],
                                         self.keys['closed']], 0)
                if result[0] == self.keys['closed']:
                    self._closed = True
                    raise QueueClosedError()
                else:
                    self._db.lpush(self.keys['not_full'], 0)
                    self._db.ltrim(self.keys['not_full'], 0, 0)

                    self._db.incr(self.keys['stats:consumed_messages'])
                    self._db.incr(
                        self.keys['stats:consumed_bytes'],
                        len(result[1])
                    )
                    return result[1]
        finally:
            self._db.lpush(self.keys['consumer_free'], 0)

    @requiresQueueToExist
    def get_nowait(self):
        res = self._db.rpop([self.keys['consumer_free']], 0)
        if res is None:
            raise QueueInUseError()
        try:
            self._db.set(self.keys['consumer'], self.client_uid)
            result = self._db.rpop(self.keys['queue'])

            self._db.lpush(self.keys['not_full'], 0)
            self._db.ltrim(self.keys['not_full'], 0, 0)

            if result is not None:
                self._db.incr(self.keys['stats:consumed_messages'])
                self._db.incr(
                    self.keys['stats:consumed_bytes'],
                    len(result)
                )
            elif self._db.exists(self.keys['closed']):
                self._closed = True
                raise QueueClosedError()
            return result
        finally:
            self._db.lpush(self.keys['consumer_free'], 0)

    @requiresQueueToExist
    def put(self, bytes):
        self._db.brpop([self.keys['producer_free']], 0)
        try:
            self._db.set(self.keys['producer'], self.client_uid)

            self._closed = self._db.exists(self.keys['closed'])
            if self._closed:
                raise QueueClosedError()

            if self.bound is not None:
                self._db.brpop([self.keys['not_full']], 0)

            new_length = self._db.lpush(self.keys['queue'], bytes)
            if self.bound is not None and new_length < self.bound:
                self._db.lpush(self.keys['not_full'], 0)
                self._db.ltrim(self.keys['not_full'], 0, 0)

            self._db.incr(self.keys['stats:produced_messages'])
            self._db.incr(self.keys['stats:produced_bytes'], len(bytes))
        finally:
            self._db.lpush(self.keys['producer_free'], 0)

    @requiresQueueToExist
    def put_nowait(self, bytes):
        res = self._db.rpop([self.keys['producer_free']], 0)
        if res is None:
            raise QueueInUseError()
        try:
            self._db.set(self.keys['producer'], self.client_uid)

            self._closed = self._db.exists(self.keys['closed'])
            if self._closed:
                raise QueueClosedError()

            if self.bound is not None:
                res = self._db.rpop(self.keys['not_full'])
                if res is None:
                    raise QueueFullError()

            new_length = self._db.lpush(self.keys['queue'], bytes)
            if self.bound is not None and new_length < self.bound:
                self._db.lpush(self.keys['not_full'], 0)
                self._db.ltrim(self.keys['not_full'], 0, 0)

            self._db.incr(self.keys['stats:produced_messages'])
            self._db.incr(self.keys['stats:produced_bytes'], len(bytes))
        finally:
            self._db.lpush(self.keys['producer_free'], 0)

    @requiresQueueToExist
    def close(self):
        self._db.brpop([self.keys['producer_free']], 0)
        try:
            self._db.set(self.keys['producer'], self.client_uid)

            self._closed = self._db.exists(self.keys['closed'])
            if self._closed:
                raise QueueClosedError()

            self._db.lpush(self.keys['closed'], 0, 0)
        finally:
            self._db.lpush(self.keys['producer_free'], 0)

    @requiresQueueToExist
    def delete(self):
        self._db.delete(self.keys['bound'])
        self._db.lpush(self.keys['not_full'], 0)
        self._db.lpush(self.keys['closed'], 0, 0)

        self._db.brpop([self.keys['producer_free']], 0)
        self._db.delete(self.keys['producer'], self.keys['producer_free'])

        self._db.brpop([self.keys['consumer_free']], 0)
        self._db.delete(self.keys['consumer'], self.keys['consumer_free'])

        self._db.delete(
            self.keys['not_full'],
            self.keys['closed'],
            self.keys['stats:produced_messages'],
            self.keys['stats:produced_bytes'],
            self.keys['stats:consumed_messages'],
            self.keys['stats:consumed_bytes'],
            self.keys['queue'],
        )

        self._exists = False

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.get()
        except QueueClosedError:
            raise StopIteration()

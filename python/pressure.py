import os
import redis
import socket


class TimeoutError(Exception):
    pass


class QueueAlreadyExistsError(Exception):
    pass


class QueueDoesNotExistError(Exception):
    pass


class QueueClosedError(Exception):
    pass


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
            self.exists = True
        else:
            self.exists = False
        self.closed = self._db.exists(self.keys['closed'])

        self.client_uid = "_pid".join([socket.gethostname(), str(os.getpid())])

    def create(self, bound=None):
        int_bound = int(bound) if bound is not None else 0

        self.exists = not self._db.setnx(self.keys['bound'], int_bound)
        if self.exists:
            raise QueueAlreadyExistsError()

        self.exists = True
        self.bound = bound
        assert self._db.lpush(self.keys['producer_free'], 0) == 1
        assert self._db.lpush(self.keys['consumer_free'], 0) == 1
        assert self._db.lpush(self.keys['not_full'], 0) == 1

    def get(self):
        if not self._db.exists(self.keys['bound']):
            self.exists = False
            raise QueueDoesNotExistError()

        self._db.brpop([self.keys['consumer_free']], 0)
        try:
            self._db.set(self.keys['consumer'], self.client_uid)

            self.closed = self._db.exists(self.keys['closed'])
            if self.closed:
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
                    self.closed = True
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

    def put(self, bytes):
        if not self._db.exists(self.keys['bound']):
            self.exists = False
            raise QueueDoesNotExistError()

        self._db.brpop([self.keys['producer_free']], 0)
        try:
            self._db.set(self.keys['producer'], self.client_uid)

            self.closed = self._db.exists(self.keys['closed'])
            if self.closed:
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

    def close(self):
        if not self._db.exists(self.keys['bound']):
            self.exists = False
            raise QueueDoesNotExistError()

        self._db.brpop([self.keys['producer_free']], 0)
        try:
            self._db.set(self.keys['producer'], self.client_uid)

            self.closed = self._db.exists(self.keys['closed'])
            if self.closed:
                raise QueueClosedError()

            self._db.lpush(self.keys['closed'], 0, 0)
        finally:
            self._db.lpush(self.keys['producer_free'], 0)

    def delete(self):
        if not self._db.exists(self.keys['bound']):
            self.exists = False
            raise QueueDoesNotExistError()

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

        self.exists = False

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.get()
        except QueueClosedError:
            raise StopIteration()

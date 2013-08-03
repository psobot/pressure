import time
import redis


class TimeoutError(Exception):
    pass


class Queue(object):
    """
    Simple synchronized Queue class with Redis Backend.
    Based on work by Peter Hoffmann at:
        http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html
    Synchronous, blocking behaviour and generator syntax added by @psobot
    """

    def __init__(self, name, maxsize=0, namespace='queue', **redis_kwargs):
        """Default connection parameters: host='localhost', port=6379, db=0"""
        self._db = redis.Redis(**redis_kwargs)
        self.key = '%s:%s' % (namespace, name)
        self.full = self.key + ':sem'
        self.maxsize = maxsize + 1 if maxsize != 0 else 0
        self._kill = False

        #   Initialize the full to open.
        if self.maxsize > 0:
            self._not_full()

    def qsize(self):
        """Return the approximate size of the queue."""
        return self._db.llen(self.key)

    def _sem_val(self):
        return self._db.llen(self.full)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def unblock(self):
        "Unblock the current .put call, pushing an extra item into the queue."
        self._kill = True
        self._not_full()

    def _wait_for_not_full(self, timeout):
        self._db.blpop(self.full, timeout=timeout)

    def _not_full(self):
        if not self._db.llen(self.full):
            self._db.rpush(self.full, "")

    def put(self, item, block=True, timeout=None):
        """Put item into the queue. This will block if the queue is full."""
        start = time.time()
        timeout_exceeded = (timeout is not None
                            and (time.time() - start) > timeout)
        while (block and not self._kill
               and self.maxsize > 0
               and self.qsize() >= self.maxsize - 1
               and not timeout_exceeded):
            self._wait_for_not_full(timeout=timeout)
            timeout_exceeded = (timeout is not None
                                and (time.time() - start) > timeout)
        if timeout_exceeded and self.qsize() >= self.maxsize - 1:
            raise TimeoutError()
        self._db.lpush(self.key, item)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self._db.brpop(self.key, timeout=timeout)
            if item:
                item = item[1]
        else:
            item = self._db.rpop(self.key)

        self._not_full()
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

    def peek_nowait(self):
        items = self._db.lrange(self.key, -1, -1)
        return items[0] if items else None

    def peek_reverse_nowait(self):
        items = self._db.lrange(self.key, 0, 0)
        return items[0] if items else None

    def __iter__(self):
        return self

    def next(self):
        return self.get()

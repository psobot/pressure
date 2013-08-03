import rqueue


class ReliableRedisQueue(rqueue.Queue):
    """
    Like RedisQueue, but ensures that each element in the queue is processed
    without exceptions. After calling .get(), you must call .confirm(elem) to
    signal that the element has been processed.

    Alternatively, the built-in generator syntax automatically calls confirm
    when the next element is accessed.
    """

    PROCESSING = ':processing'

    def __init__(self, *args, **kwargs):
        rqueue.Queue.__init__(self, *args, **kwargs)
        self.element = None

    def qsize(self):
        """Return the approximate size of the queue."""
        return (self._db.llen(self.key)
                + self._db.llen(self.key + self.PROCESSING))

    #   TODO: Expand this to allow multiple "processing" items?
    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        item = self._db.rpop(self.key + self.PROCESSING)
        if item:
            self._db.rpush(self.key + self.PROCESSING, item)
            return item

        if block:
            item = self._db.brpoplpush(self.key,
                                       self.key + self.PROCESSING,
                                       timeout=timeout)
        else:
            item = self._db.rpop(self.key)

        return item

    def confirm(self, element):
        if element is not None:
            self._db.lrem(self.key + self.PROCESSING, element)
        if self.qsize() < self.maxsize:
            self._not_full()

    def next(self):
        self.confirm(self.element)
        self.element = self.get()
        return self.element

import rqueue
import threading
from Queue import Queue as PythonQueue


class BufferedRedisQueue(PythonQueue):
    def __init__(self, name, buffer_size=None, **kwargs):
        self.raw = rqueue.Queue(name, **kwargs)
        self._listener = threading.Thread(target=self.listen)
        self._listener.setDaemon(True)
        self._listener.start()
        PythonQueue.__init__(self, buffer_size)

    def listen(self):
        try:
            while True:
                self.put(self.raw.get())
        except:
            pass

    @property
    def buffered(self):
        return self.qsize()

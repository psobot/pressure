import rqueue
import buffered
import reliable

import sys
import logging
import traceback
import contextlib
import subprocess

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.INFO)

REDIS_TEST_PORT = 9999


def test_rqueue_nonblocking_same():
    strings = ["hello", "goodbye"]
    q = rqueue.Queue('test', port=REDIS_TEST_PORT)
    for string in strings:
        q.put(string)
    for string in strings:
        assert q.get() == string


def test_rqueue_nonblocking_different():
    strings = ["hello", "goodbye"]
    q1 = rqueue.Queue('test', port=REDIS_TEST_PORT)
    q2 = rqueue.Queue('test', port=REDIS_TEST_PORT)
    for string in strings:
        q1.put(string)
    for string in strings:
        assert q2.get() == string


def test_rqueue_blocking_empty_timeout():
    q = rqueue.Queue('test', port=REDIS_TEST_PORT)
    assert q.get(timeout=1) is None


def test_rqueue_blocking_full():
    for size in xrange(1, 101, 10):
        q = rqueue.Queue('test_%d' % size,
                         maxsize=size,
                         port=REDIS_TEST_PORT)
        for i in xrange(0, size):
            q.put("herp_%d" % i)
        try:
            q.put("full", timeout=1)

            # That put line should throw an exception.
            assert False
        except rqueue.TimeoutError:
            pass


@contextlib.contextmanager
def redis_instance(port):
    """
    A little context manager that spawns a redis server on the given
    port and tears it down once the block is done.

    Currently does not handle any errors. If Redis fails to start,
    this will hang indefinitely.
    """
    ready_string = "The server is now ready to accept connections"
    p = subprocess.Popen(
        ['redis-server', '--port', str(port), '--save', '""'],
        stdout=subprocess.PIPE,
    )
    accumulate = []
    while not ready_string in "".join(accumulate):
        accumulate.append(p.stdout.read(16))
    yield
    p.kill()


def test():
    log.info("Starting test suite.")
    tests = [
        test_rqueue_nonblocking_same,
        test_rqueue_nonblocking_different,
        test_rqueue_blocking_empty_timeout,
        test_rqueue_blocking_full
    ]
    for method in tests:
        with redis_instance(REDIS_TEST_PORT):
            try:
                method()
                log.info("[x] Test \"%s\" successful.", method.__name__)
            except:
                log.error("[ ] Test \"%s\" failed!", method.__name__)
                log.error(traceback.format_exc())
    log.info("Tests done.")

if __name__ == "__main__":
    test()

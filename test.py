import rqueue
import buffered
import reliable

import os
import sys
import time
import logging
import traceback
import contextlib
import subprocess
from glob import glob

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.INFO)

REDIS_TEST_PORT = 9999
REDIS_SLAVE_TEST_PORT = 9998


def test_rqueue_nonblocking_same():
    strings = ["hello", "goodbye"]
    q = rqueue.Queue('test', port=REDIS_TEST_PORT)
    for string in strings:
        q.put(string)
    for string in strings:
        assert q.get(block=False) == string


def test_rqueue_nonblocking_different():
    strings = ["hello", "goodbye"]
    q1 = rqueue.Queue('test', port=REDIS_TEST_PORT)
    q2 = rqueue.Queue('test', port=REDIS_TEST_PORT)
    for string in strings:
        q1.put(string)
    for string in strings:
        assert q2.get(block=False) == string


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


def test_slave_rqueue_nonblocking_different():
    strings = ["hello", "goodbye"]
    q1 = rqueue.Queue('test', port=REDIS_TEST_PORT)
    q2 = rqueue.Queue('test', port=REDIS_SLAVE_TEST_PORT)
    for string in strings:
        q1.put(string)
    for string in strings:
        assert q2.get(timeout=1) == string


def test_slave_rqueue_blocking_full():
    for size in xrange(1, 101, 10):
        q1 = rqueue.Queue('test_%d' % size,
                          maxsize=size,
                          port=REDIS_TEST_PORT)
        q2 = rqueue.Queue('test_%d' % size,
                          maxsize=size,
                          port=REDIS_SLAVE_TEST_PORT)
        for i in xrange(0, size):
            q1.put("herp_%d" % i)

        #   Account for propagation between master and slave.
        time.sleep(0.1)
        try:
            q2.put("full", timeout=1)

            # That put line should throw an exception.
            assert False
        except rqueue.TimeoutError:
            pass


@contextlib.contextmanager
def redis_instance(port, slaveof=None):
    """
    A little context manager that spawns a redis server on the given
    port and tears it down once the block is done.

    Currently does not handle any errors. If Redis fails to start,
    this will hang indefinitely.
    """
    ready_string = "The server is now ready to accept connections"
    args = ['redis-server', '--port', str(int(port)), '--save', '""']
    if slaveof is not None:
        args += ['--slaveof', '127.0.0.1', str(int(slaveof))]
        args += ['--slave-read-only', 'no']
    else:
        for f in glob(os.path.join(os.getcwd(), "*.rdb")):
            log.debug("Removing RDB file %s", f)
            os.remove(f)
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    accumulate = []
    while not ready_string in "".join(accumulate):
        accumulate.append(p.stdout.read(16))
    yield
    if slaveof is None:
        for f in glob(os.path.join(os.getcwd(), "*.rdb")):
            log.debug("Removing RDB file %s.", f)
            os.remove(f)
    p.kill()


def test():
    log.info("Starting test suite.")
    tests = [
        test_rqueue_nonblocking_same,
        test_rqueue_nonblocking_different,
        test_rqueue_blocking_empty_timeout,
        test_rqueue_blocking_full
    ]
    slave_tests = [
        test_slave_rqueue_nonblocking_different,
        test_slave_rqueue_blocking_full,
    ]
    for method in tests:
        with redis_instance(REDIS_TEST_PORT):
            try:
                log.info("    Test \"%s\" beginning...", method.__name__)
                method()
                log.info("[x] Test \"%s\" successful.", method.__name__)
            except:
                log.error("[ ] Test \"%s\" failed!", method.__name__)
                log.error(traceback.format_exc())
    for method in slave_tests:
        with redis_instance(REDIS_TEST_PORT):
            with redis_instance(REDIS_SLAVE_TEST_PORT, REDIS_TEST_PORT):
                try:
                    log.info("    Test \"%s\" beginning...", method.__name__)
                    method()
                    log.info("[x] Test \"%s\" successful.", method.__name__)
                except:
                    log.error("[ ] Test \"%s\" failed!", method.__name__)
                    log.error(traceback.format_exc())
    log.info("Tests done.")

if __name__ == "__main__":
    test()

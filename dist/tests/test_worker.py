from dist import Worker, MDStore
from dist.worker import loads, dumps
import trollius as asyncio
from contextlib import contextmanager
import zmq


context = zmq.Context()


@contextmanager
def dealer(addr):
    socket = context.socket(zmq.DEALER)
    socket.connect(addr)
    try:
        yield socket
    finally:
        socket.close()


@contextmanager
def mdstore():
    mds = MDStore('*', 8008)
    mds.start()

    try:
        yield mds
    finally:
        mds.close()


@contextmanager
def worker(metadata_addr):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    w = Worker('127.0.0.1', 3483, '*', metadata_addr, loop=loop)
    w.start()

    try:
        yield w
    finally:
        w.close()

def test_Worker():
    with mdstore() as mds:
        with worker(metadata_addr='tcp://127.0.0.1:%d' % mds.port) as w:
            with dealer(w.address) as sock:
                msg = {'op': 'ping'}
                sock.send(dumps(msg))
                result = sock.recv()
                assert result == b'pong'

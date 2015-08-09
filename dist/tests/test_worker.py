from dist import Worker, MDStore
from dist.worker import loads, dumps
from dist.utils import delay
import trollius as asyncio
from trollius import From, Return
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
def Loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()


@contextmanager
def worker(metadata_addr, loop=None, start=True):
    if loop is None:
        loop = asyncio.get_event_loop_policy().new_event_loop()
    w = Worker('127.0.0.1', 3483, '*', metadata_addr, loop=loop)
    if start:
        w.start()

    try:
        yield w
    finally:
        if w.status != 'closed':
            w.close()

def test_Worker():
    with mdstore() as mds, Loop() as loop:
        with worker(metadata_addr='tcp://127.0.0.1:%d' % mds.port, start=False,
                    loop=loop) as w:
            with dealer(w.address) as sock:

                @asyncio.coroutine
                def f():
                    msg = {'op': 'ping'}
                    for i in range(3):
                        sock.send(dumps(msg))
                        result = yield From(delay(loop, sock.recv))
                        assert result == b'pong'
                    yield From(w.close())

                loop.run_until_complete(asyncio.gather(w.start(), f()))

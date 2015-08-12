from dist import Worker, MDStore
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

port = [8012]

@contextmanager
def mdstore(loop=None):
    if loop is None:
        loop = asyncio.get_event_loop_policy().new_event_loop()
    port[0] += 1
    mds = MDStore('127.0.0.1', port[0], '*')

    try:
        yield mds
    finally:
        if mds.status != 'closed':
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
def worker(metadata_port, port=3598, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop_policy().new_event_loop()
    w = Worker('127.0.0.1', port, '*', 'tcp://127.0.0.1:%d' % metadata_port,
               loop=loop)

    try:
        yield w
    finally:
        if w.status != 'closed':
            w.close()


@contextmanager
def everything():
    with Loop() as loop:
        with mdstore() as mds:
            with worker(metadata_port=mds.port, loop=loop) as w:
                with dealer(w.address) as sock:
                    yield loop, mds, w, sock

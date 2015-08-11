from dist import Worker, MDStore
from dist.worker import loads, dumps
from dist.utils import delay
import trollius as asyncio
from trollius import From, Return
from contextlib import contextmanager
from operator import add
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
def mdstore():
    port[0] += 1
    mds = MDStore('*', port[0])
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

def test_Worker():
    with mdstore() as mds, Loop() as loop:
        with worker(metadata_port=mds.port, loop=loop) as w:
            with dealer(w.address) as sock:

                @asyncio.coroutine
                def f():
                    msg = {'op': 'ping'}
                    for i in range(3):
                        sock.send(dumps(msg))
                        result = yield From(delay(loop, sock.recv))
                        print(result)
                        assert result == b'pong'

                    w.close()

                loop.run_until_complete(asyncio.gather(w.go(), f()))


def test_get_data():
    with everything() as (loop, mds, w, sock):
        w.data['x'] = 123
        @asyncio.coroutine
        def f():
            msg = {'op': 'get-data', 'keys': ['x']}
            for i in range(3):
                sock.send(dumps(msg))
                result = yield From(delay(loop, sock.recv))
                assert loads(result) == {'x': 123}
            w.close()

        loop.run_until_complete(asyncio.gather(w.go(), f()))


def test_compute():
    with everything() as (loop, mds, w, sock):
        w.data['x'] = 123
        mds.who_has['x'].add(w.address)
        mds.has_what[w.address].add('x')

        @asyncio.coroutine
        def f():
            msg = {'op': 'compute',
                   'key': 'y',
                   'function': add,
                   'args': ('x', 10),
                   'needed': ['x'],
                   'reply': True}
            for i in range(3):
                sock.send(dumps(msg))
                result = yield From(delay(loop, sock.recv))
                assert loads(result) == {'op': 'computation-finished',
                                         'key': 'y'}

            w.close()

        loop.run_until_complete(asyncio.gather(w.go(), f()))


def test_remote_gather():
    with Loop() as loop, mdstore() as mds, worker(metadata_port=mds.port, port=1234, loop=loop) as a, worker(metadata_port=mds.port, port=4321, loop=loop) as b, dealer(a.address) as sock:

        # Put 'x' in b's data.  Register with metadata store
        b.data['x'] = 123
        mds.who_has['x'].add(b.address)
        mds.has_what[b.address].add('x')

        @asyncio.coroutine
        def f():
            msg = {'op': 'compute',
                   'key': 'y',
                   'function': add,
                   'args': ('x', 10),
                   'needed': ['x'],
                   'reply': True}

            sock.send(dumps(msg))  # send to a, will need to get from b
            result = yield From(delay(loop, sock.recv))
            assert loads(result) == {'op': 'computation-finished',
                                     'key': 'y'}

            a.close()
            b.close()

        loop.run_until_complete(asyncio.gather(a.go(), b.go(), f()))
        assert a.data['y'] == 10 + 123

from dist import Worker, MDStore
from dist.worker import loads, dumps
from dist.utils import delay
from dist.utils_test import dealer, mdstore, Loop, worker, context, everything
import trollius as asyncio
from trollius import From, Return
from operator import add
from time import sleep


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
        assert mds.who_has['y'] == set([a.address])
        assert 'y' in mds.has_what[a.address]


def test_no_data_found():
    with everything() as (loop, mds, w, sock):
        @asyncio.coroutine
        def f():
            msg = {'op': 'compute',
                   'key': 'y',
                   'function': add,
                   'args': ('asdf', 10),
                   'needed': ['asdf'],
                   'reply': True}
            sock.send(dumps(msg))
            result = yield From(delay(loop, sock.recv))
            result = loads(result)
            assert result['op'] == 'computation-failed'
            assert isinstance(result['error'], KeyError)
            assert 'asdf' in str(result['error'])

            w.close()

        loop.run_until_complete(asyncio.gather(w.go(), f()))


"""
def test_worker_data_management():
    with everything() as (loop, mds, w, sock):
        @asyncio.coroutine
        def f():
            msg = {'op': 'put-data',
                   'keys': ['x', 'y'],
                   'values': [1, 2],
                   'reply': True}
            sock.send(dumps(msg))
            result = yield From(delay(loop, sock.recv))
            result = loads(result)
            assert result['op'] == 'put-ack'
            assert w.data == {'x': 1, 'y': 2}

            w.close()

        loop.run_until_complete(asyncio.gather(w.go(), f()))

        for i in range(100):
            sleep(0.01)
            if mds.who_has['x']:
                break

        assert mds.who_has['x'] == set([w.address])
        assert mds.who_has['y'] == set([w.address])
        assert mds.has_what[w.address] == set(['x', 'y'])
"""

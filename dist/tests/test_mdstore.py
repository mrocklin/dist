from dist import MDStore
from dist.utils_test import mdstore, dealer, Loop
from dist.core import dumps, loads, delay
import trollius as asyncio
from trollius import From


def test_mdstore():
    with Loop() as loop:
        with mdstore() as mds:
            with dealer(mds.address) as sock:
                @asyncio.coroutine
                def f():
                    msg = {'op': 'register', 'address': 'hank',
                            'keys': ['x', 'y'], 'reply': True}
                    sock.send(dumps(msg))
                    ack = yield From(delay(loop, sock.recv))
                    assert ack == b'OK'
                    assert 'hank' in mds.who_has['x']
                    assert 'hank' in mds.who_has['y']
                    assert mds.has_what['hank'] == set(['x', 'y'])

                    msg = {'op': 'who-has', 'keys': ['x']}
                    sock.send(dumps(msg))
                    result = yield From(delay(loop, sock.recv))
                    assert loads(result) == {'x': set(['hank'])}

                    msg = {'op': 'list', 'number': 0}
                    sock.send(dumps(msg))
                    result = yield From(delay(loop, sock.recv))
                    assert loads(result) == set(['hank'])

                    msg = {'op': 'unregister', 'address': 'hank', 'keys': ['x'],
                           'reply': True}
                    sock.send(dumps(msg))
                    result = yield From(delay(loop, sock.recv))
                    assert mds.who_has['x'] == set()
                    assert mds.who_has['y'] == set(['hank'])
                    assert mds.has_what['hank'] == set(['y'])

                    mds.close()

                loop.run_until_complete(asyncio.gather(mds.go(), f()))

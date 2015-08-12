import trollius as asyncio
from trollius import From

from dist import Center
from dist.utils_test import center, dealer, Loop
from dist.core import dumps, loads, delay


def test_center():
    with Loop() as loop:
        with center(loop) as c:
            with dealer(c.address) as sock:
                @asyncio.coroutine
                def f():
                    msg = {'op': 'register', 'address': 'hank',
                            'keys': ['x', 'y'], 'reply': True}
                    sock.send(dumps(msg))
                    ack = yield From(delay(loop, sock.recv))
                    assert ack == b'OK'
                    assert 'hank' in c.who_has['x']
                    assert 'hank' in c.who_has['y']
                    assert c.has_what['hank'] == set(['x', 'y'])

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
                    assert c.who_has['x'] == set()
                    assert c.who_has['y'] == set(['hank'])
                    assert c.has_what['hank'] == set(['y'])

                    c.close()

                loop.run_until_complete(asyncio.gather(c.go(), f()))

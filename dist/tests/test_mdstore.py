from dist import MDStore
from dist.mdstore import dumps, loads
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
    mds = MDStore('*', 8003)
    mds.start()

    try:
        yield mds
    finally:
        mds.close()


def test_mdstore():
    with mdstore() as mds:
        with dealer('tcp://127.0.0.1:%d' % mds.port) as sock:
            msg = {'op': 'register', 'address': 'hank',
                    'keys': ['x', 'y'], 'reply': True}
            sock.send(dumps(msg))
            ack = loads(sock.recv())
            assert ack == b'OK'
            assert 'hank' in mds.who_has['x']
            assert 'hank' in mds.who_has['y']
            assert mds.has_what['hank'] == set(['x', 'y'])

            msg = {'op': 'who-has', 'keys': ['x']}
            sock.send(dumps(msg))
            result = sock.recv()
            assert loads(result) == {'x': set(['hank'])}

            msg = {'op': 'list', 'number': 0}
            sock.send(dumps(msg))
            result = sock.recv()
            assert loads(result) == set(['hank'])

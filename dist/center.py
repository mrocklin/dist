from collections import defaultdict
from dill import dumps, loads
from threading import Thread
import zmq


import trollius as asyncio
from trollius import From, Return

from .core import (comm, control, pingpong, send, delay, dealer_send_recv,
        context)

class Center(object):
    """ A central point of contact

    >>> c = Center(ip='192.168.0.45', port=8000, bind_ip='*')
    >>> c.loop.run_until_complete(c.go) # doctest: +SKIP

    To close cause this coroutine to be run in the same event loop

    >>> c.close()  # doctest: +SKIP
    """
    def __init__(self, ip, port, bind_ip,
                 context=context, loop=None, start=False):
        self.ip = ip
        self.port = port
        self.bind_ip = bind_ip
        self.context = context
        self.loop = loop or asyncio.get_event_loop()

        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)

        self.metadata_q = asyncio.Queue(loop=self.loop)
        self.control_q = asyncio.Queue(loop=self.loop)
        self.send_q = asyncio.Queue(loop=self.loop)
        self.pingpong_q = asyncio.Queue(loop=self.loop)
        self.outgoing_q = asyncio.Queue(loop=self.loop)
        self.signal_q = asyncio.Queue(loop=self.loop)

        self.status = 'running'

        if start:
            self.start()

    @asyncio.coroutine
    def go(self):
        coroutines = [
                control(self.control_q, {'who-has': self.metadata_q,
                                         'register': self.metadata_q,
                                         'unregister': self.metadata_q,
                                         'list': self.metadata_q,
                                         'ping': self.pingpong_q,
                                         'send': self.send_q}),
                send(self.send_q, self.outgoing_q, self.signal_q),
                pingpong(self.pingpong_q, self.send_q),
                comm(self.ip, self.port, self.bind_ip, self.signal_q,
                     self.control_q, self.outgoing_q, self.loop, self.context),
                metadata(self.metadata_q, self.send_q,
                         self.who_has, self.has_what)
            ]

        first, other = yield From(asyncio.wait(coroutines,
                                return_when=asyncio.FIRST_COMPLETED))

        print("Closing")

        yield From(asyncio.gather(*other))

    @property
    def address(self):
        return 'tcp://%s:%d' % (self.ip, self.port)

    def close(self):
        self.signal_q.put_nowait(b'close')
        self.status = 'closing'

    def start(self):
        self.loop.run_until_complete(self.go())


@asyncio.coroutine
def metadata(metadata_q, send_q, who_has, has_what):
    print("Metadata boots up")
    while True:
        addr, msg = yield From(metadata_q.get())
        if msg == b'close':
            break
        elif msg['op'] == 'who-has':
            result = {k: who_has[k] for k in msg['keys']}
            send_q.put_nowait((addr, result))
        elif msg['op'] == 'register':
            print("Register: %s" % str(msg))
            has_what[msg['address']].update(msg['keys'])
            for key in msg['keys']:
                who_has[key].add(msg['address'])
            if msg.get('reply'):
                send_q.put_nowait((addr, b'OK'))
        elif msg['op'] == 'unregister':
            print("UnRegister: %s" % str(msg))
            for key in msg['keys']:
                if key in has_what[msg['address']]:
                    has_what[msg['address']].remove(key)
                try:
                    who_has[key].remove(msg['address'])
                except KeyError:
                    pass
            if msg.get('reply'):
                send_q.put_nowait((addr, b'OK'))
        elif msg['op'] == 'list':
            result = set(has_what)
            send_q.put_nowait((addr, result))

    raise Return("MetaData Done")

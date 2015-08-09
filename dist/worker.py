import trollius as asyncio
from trollius import From, Return
from toolz import merge, get
from dill import dumps, loads
from threading import Thread
from .utils import delay
import zmq

context = zmq.Context()


@asyncio.coroutine
def dealer_send_recv(loop, addr, data):
    socket = context.socket(zmq.DEALER)
    socket.connect(addr)
    if not isinstance(data, bytes):
        data = dumps(data)
    yield From(delay(loop, socket.send, data))
    result = yield From(delay(loop, socket.recv))
    socket.close()  # TODO: LRU sockets
    result = loads(result)
    raise Return(result)


def keys_to_data(o, data):
    if isinstance(o, (tuple, list)):
        result = []
        for arg in o:
            try:
                result.append(data[arg])
            except (TypeError, KeyError):
                result.append(arg)
        result = type(o)(result)

    if isinstance(o, dict):
        for k, v in o.items():
            result = {}
            try:
                result[key] = data[arg]
            except (TypeError, KeyError):
                result[key] = arg
    return result


@asyncio.coroutine
def get_data(loop, keys, local_data, metadata_addr, update=False):
    local = {k: local_data[k] for k in needed if k in local_data},
    missing = [k for k in needed if k not in local_data]

    msg = {'op': 'who-has', 'keys': other}
    who_has = yield From(dealer_send_recv(loop, metadata_attr, msg))

    coroutines = [dealer_send_recv(loop, random.choice(who_has[k]),
                                   {'op': 'get-data', 'keys': [k]})
                                for k in missing]

    other = yield From(asyncio.gather(*coroutines))
    other = merge(other)

    if update:
        local_data.update(other)

    result = merge(local, other)

    raise Return(result)


@asyncio.coroutine
def compute(loop, msg, local_data, metadata_addr, store=True):
        key, func, args, kwargs, needed = \
                get(msg, 'key', 'func', 'args', 'kwargs', 'needed')

        data = yield From(get_data(loop, keys, local_data, metadata_addr))

        args2 = keys_to_data(args, data)
        kwargs2 = keys_to_data(kwargs, data)

        result = yield From(delay(self.loop, func, *args2, **kwargs2))

        if store:
            local_data[key] = result

        raise Return(result)


class Worker(object):
    def __init__(self, ip, port, bind_ip,
                 metadata_addr, loop=None):
        self.ip = ip
        self.port = port
        self.bind_ip = bind_ip
        self.metadata_addr = metadata_addr
        self.data = dict()
        self.loop = loop or asyncio.get_event_loop()
        self.work_q = asyncio.Queue(loop=self.loop)
        self.send_q = asyncio.Queue(loop=self.loop)

    @property
    def address(self):
        return 'tcp://%s:%d' % (self.ip, self.port)

    @asyncio.coroutine
    def listen(self):
        while True:
            try:
                socks = yield From(delay(self.loop, self.poller.poll))
                socks = dict(socks)
            except KeyboardInterrupt:
                break
            print("Communication received")
            if self._local_router in socks:
                addr, bytes = self._local_router.recv_multipart()
            elif self.router in socks:
                result = yield From(delay(self.loop, self.router.recv_multipart))
                addr, bytes = result

            msg = loads(bytes)
            print("msg: %s" % str(msg))
            if msg == 'close':
                break
            if msg['op'] == 'compute':
                work_q.put_nowait(msg)
            if msg['op'] == 'get-data':
                data = {k: self.data[k] for k in msg['keys']
                                         if k in self.data}
                self.send_q.put_nowait((addr, data))
            if msg['op'] == 'ping':
                self.send_q.put_nowait((addr, b'pong'))
            else:
                raise NotImplementedError("Bad Message: %s" % msg)

    @asyncio.coroutine
    def compute(self):
        while True:
            addr, msg = yield From(self.work_q.get())
            print('compute', msg)
            if msg == 'close':
                break

            result = yield From(compute(self.loop, msg, self.data, self.metadata_addr))
            out = {'op': 'computation-finished',
                   'key': msg['key']}
            self.send_q.put_nowait((addr, out))

    @asyncio.coroutine
    def reply(self):
        while True:
            addr, msg = yield From(self.send_q.get())
            print('reply', msg)
            if msg == 'close':
                break
            if not isinstance(msg, bytes):
                msg = dumps(msg)
            yield From(delay(self.loop, self.router.send_multipart, [addr, msg]))

    def close(self):
        self._local_dealer.send(dumps('close'))
        self.send_q.put_nowait((None, 'close'))
        self.work_q.put_nowait((None, 'close'))
        self._thread.join()
        self.router.close()
        self._local_router.close()
        self._local_dealer.close()

    def start(self, thread=True):
        self.router = context.socket(zmq.ROUTER)
        self.router.bind('tcp://%s:%d' % (self.bind_ip, self.port))
        print("Binding router to %s" % 'tcp://%s:%d' % (self.bind_ip,
            self.port))

        self._local_router = context.socket(zmq.ROUTER)
        port = self._local_router.bind_to_random_port('tcp://127.0.0.1')
        self._local_dealer = context.socket(zmq.DEALER)
        self._local_dealer.connect('tcp://127.0.0.1:%d' % port)

        self.poller = zmq.Poller()
        self.poller.register(self.router, zmq.POLLIN)
        self.poller.register(self._local_router, zmq.POLLIN)

        cor = asyncio.gather(*[asyncio.Task(cor, loop=self.loop)
                               for cor in [self.listen(),
                                           self.compute(),
                                           self.reply()]])

        if thread:
            self._thread = Thread(target=self.loop.run_until_complete,
                                  args=(cor,))
            self._thread.start()
        else:
            self.loop.run_until_complete(cor)

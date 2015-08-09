import trollius as asyncio
from trollius import From, Return
from concurrent.futures import ThreadPoolExecutor
from toolz import merge, get
from dill import dumps, loads
import zmq

executor = ThreadPoolExecutor(1)
context = zmq.Context()


def delay(loop, func, *args, **kwargs):
    """ Run function in separate thread, turn into coroutine """
    future = executor.submit(func, *args2, **kwargs2)
    return asyncio.futures.wrap_future(future, loop=loop)


@asyncio.coroutine
def dealer_send_recv(loop, addr, data):
    socket = context.socket(zmq.DEALER)
    socket.connect(addr)
    yield From(delay(loop, socket.send, data))
    data = yield From(delay(loop, socket.recv))
    socket.close()  # TODO: LRU sockets
    raise Return(data)


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
def get_data(keys, local_data, metadata_addr, update=False):
    local = {k: local_data[k] for k in needed if k in local_data},
    missing = [k for k in needed if k not in local_data]

    msg = {'op': 'who-has', 'data': other}
    who_has = yield From(dealer_send_recv(metadata_attr, msg))

    coroutines = [dealer_send_recv(random.choice(who_has[k]),
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

        data = yield From(get_data(keys, local_data, metadata_addr))

        args2 = keys_to_data(args, data)
        kwargs2 = keys_to_data(kwargs, data)

        result = yield From(delay(self.loop, func, *args2, **kwargs2))

        if store:
            local_data[key] = result

        raise Return(result)


class Worker(object):
    def __init__(self, address, bind, metadata_addr, loop=None):
        self.router = context.socket(zmq.ROUTER)
        self.router.bind(bind)
        self.metadata_addr = metadata_addr
        self.data = dict()
        self.loop = loop or asyncio.get_event_loop()
        self.work_q = asyncio.Queue(loop=self.loop)
        self.send_q = asyncio.Queue(loop=self.loop)


    @asyncio.coroutine
    def listen(self):
        while True:
            addr, bytes = yield From(delay(self.loop, self.router.recv_multipart))
            msg = loads(bytes)
            if msg['op'] == 'close':
                break
            if msg['op'] == 'compute':
                work_q.put(msg)
            if msg['op'] == 'get-data':
                data = {k: self.data[k] for k in msg['keys']
                                         if k in self.data}
                send_q.put((addr, data))
            if msg['op'] == 'ping':
                send_q.put((addr, b'pong'))

    @asyncio.coroutine
    def compute(self):
        while True:
            addr, msg = self.work_q.get()
            if msg['op'] == 'close':
                break

            result = yield From(compute(self.loop, msg, self.data, self.metadata_addr))
            out = {'op': 'computation-finished',
                   'key': msg['key']}
            self.send_q.put((addr, out))

    @asyncio.coroutine
    def reply(self):
        while True:
            addr, msg = self.send_q.get()
            if msg['op'] == 'close':
                break
            if not isinstance(msg, bytes):
                msg = dumps(msg)
            yield From(delay(self.loop, self.router.send_multipart, [addr, msg]))

    def close(self):
        self.send_q.put((None, 'close'))
        self.work_q.put((None, 'close'))
        self.listen_q.put((None, 'close'))
        self.router.close(linger=1)

    def run(self):
        self.loop.run_until_complete(asyncio.gather(self.listen(),
                                                    self.compute(),
                                                    self.reply()))

import trollius as asyncio
from trollius import From, Return, Task
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
    """

    >>> keys_to_data(('x', 'y'), {'x': 1})
    (1, 'y')
    >>> keys_to_data({'a': 'x', 'b': 'y'}, {'x': 1})
    {'a': 1, 'b': 'y'}
    """
    if isinstance(o, (tuple, list)):
        result = []
        for arg in o:
            try:
                result.append(data[arg])
            except (TypeError, KeyError):
                result.append(arg)
        result = type(o)(result)

    if isinstance(o, dict):
        result = {}
        for k, v in o.items():
            try:
                result[k] = data[v]
            except (TypeError, KeyError):
                result[k] = v
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
            get(['key', 'function', 'args', 'kwargs', 'needed'], msg)

    data = yield From(get_data(loop, needed, local_data, metadata_addr))


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
        self.control_q = asyncio.Queue(loop=self.loop)
        self.signal_q = asyncio.Queue(loop=self.loop)
        self.status = None

    @property
    def address(self):
        return 'tcp://%s:%d' % (self.ip, self.port)

    @asyncio.coroutine
    def control(self):
        print("Control boots up")
        while True:
            addr, msg = yield From(self.control_q.get())
            if msg == b'close':
                break
            elif msg['op'] == 'compute':
                self.work_q.put_nowait((addr, msg))
            elif msg['op'] == 'get-data':
                data = {k: self.data[k] for k in msg['keys']
                                         if k in self.data}
                self.send(addr, data)
            elif msg['op'] == 'ping':
                self.send(addr, b'pong')
            else:
                raise NotImplementedError("Bad Message: %s" % msg)
        raise Return("Done listening")

    def send(self, addr, msg):
        if not isinstance(msg, bytes):
            msg = dumps(msg)
        self.send_q.put_nowait((addr, msg))
        self.signal_q.put_nowait('interrupt')

    @asyncio.coroutine
    def compute(self):
        while True:
            addr, msg = yield From(self.work_q.get())
            if msg == 'close':
                break

            result = yield From(compute(self.loop, msg, self.data, self.metadata_addr))
            out = {'op': 'computation-finished',
                   'key': msg['key']}
            self.send(addr, out)

    @asyncio.coroutine
    def comm(self):
        wait_signal = Task(self.signal_q.get(), loop=self.loop)
        while True:
            wait_router = delay(self.loop, self.router.recv_multipart)
            [first], [other] = yield From(
                    asyncio.wait([wait_router, wait_signal],
                                 return_when=asyncio.FIRST_COMPLETED))

            if first is wait_signal:        # Interrupt socket recv
                self._dealer.send(b'break')
                addr, data = yield From(wait_router)  # should be fast
                assert data == b'break'

            while not self.send_q.empty():  # Flow data out
                addr, msg = self.send_q.get_nowait()
                if not isinstance(msg, bytes):
                    msg = dumps(msg)
                self.router.send_multipart([addr, msg])
                print("Message sent: %s" % str(msg))

            if first is wait_signal:        # Handle internal messages
                msg = wait_signal.result()
                if msg == b'close':
                    break
                elif msg == b'interrupt':
                    wait_signal = Task(self.signal_q.get(), loop=self.loop)
                    continue
            elif first is wait_router:      # Handle external messages
                addr, byts = wait_router.result()
                msg = loads(byts)
                print("Communication received: %s" % str(msg))
                self.control_q.put_nowait((addr, msg))

    @asyncio.coroutine
    def close(self):
        self.work_q.put_nowait((None, b'close'))
        self.control_q.put_nowait((None, b'close'))
        self.signal_q.put_nowait(b'close')

        yield From(asyncio.sleep(0.01))

        self.router.close(linger=1)
        self._dealer.close(linger=1)
        self.status = 'closed'

    def setup_sockets(self):
        self.status = 'running'
        self.router = context.socket(zmq.ROUTER)
        self.router.bind('tcp://%s:%d' % (self.bind_ip, self.port))
        self._dealer = context.socket(zmq.DEALER)
        self._dealer.connect('tcp://127.0.0.1:%d' % self.port)
        print("Binding router to %s" %
                ('tcp://%s:%d' % (self.bind_ip, self.port)))


    @asyncio.coroutine
    def start(self):
        self.setup_sockets()

        cor = asyncio.gather(self.control(),
                             self.compute(),
                             self.comm())
        yield From(cor)

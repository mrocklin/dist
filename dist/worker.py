""" Distributed worker network

A worker responds to requests from the outside world via ZMQ sockets.
It serves a dictionary of local data, processes and stores the results of
arbitrary function calls, collecting data from peers as necessary.

It depends on an externally set up MDStore and on its peers.

Internally a worker maintains a bit of state:

    data: dictionary of data

And processing occurs on four coroutines

    comm: Manage communication with the outside world via ZeroMQ
    control: Dispatch incoming messages to the right process accordingly
    work: Execute functions and store the results.
          Communicate to peers as necessary
    send: Prepare data to be sent out over comm

These processes/coroutines interact over shared queues between each other and
over ZeroMQ sockets with the outside world.

            |       ------ send              |
            |      /         ^               |
            |      |         |\________      |    -> Worker
            |      .         |         \     |   /
   Router <-|--> comm --> control --> work <-|--->MDStore
            |                                |   \           .
            |                                |    -> Worker
            |          One Worker            |
"""

import trollius as asyncio
from trollius import From, Return, Task
from toolz import merge, get
from dill import dumps, loads
import random
from .utils import delay
import zmq

context = zmq.Context()


class Worker(object):
    """ A single Worker in a distributed network

    >>> w = Worker(ip='192.168.0.45', port=8000, bind_ip='*',
    ...            metadata_addr='192.168.0.13')
    >>> w.loop.run_until_complete(w.go) # doctest: +SKIP

    To close cause this coroutine to be run in the event loop

    >>> w.close()  # doctest: +SKIP
    """
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
        self.outgoing_q = asyncio.Queue(loop=self.loop)
        self.control_q = asyncio.Queue(loop=self.loop)
        self.signal_q = asyncio.Queue(loop=self.loop)

        self.status = 'running'

    @asyncio.coroutine
    def go(self):
        coroutines = [
            work(self.work_q, self.send_q, self.data, self.metadata_addr,
                 self.address, self.loop),
            control(self.control_q, self.work_q, self.send_q, self.data),
            send(self.send_q, self.outgoing_q, self.signal_q),
            comm(self.ip, self.port, self.bind_ip, self.signal_q, self.control_q,
                 self.outgoing_q, self.loop, context)]

        yield From(asyncio.wait(coroutines,
                                return_when=asyncio.FIRST_COMPLETED))
        self.close()

        print("Closing")

        yield From(asyncio.gather(*coroutines))

    @property
    def address(self):
        return 'tcp://%s:%d' % (self.ip, self.port)

    def close(self):
        self.signal_q.put_nowait(b'close')
        self.status = 'closing'


@asyncio.coroutine
def comm(ip, port, bind_ip, signal_q, control_q, outgoing_q, loop=None,
        context=None):
    """ Communications coroutine

    Input Channels:
        ZMQ router: from outside world
        signal_q: to break waits on the router
        outgoing_q: data that needs to be sent out on the router

    Output Channels:
        ZMQ router: to the outside world
        control_q: put messages from outside world here for handling

    Interacts with:
        send, control
    """
    loop = loop or asyncio.get_event_loop()
    context = context or zmq.Context()

    router = context.socket(zmq.ROUTER)
    router.bind('tcp://%s:%d' % (bind_ip, port))

    dealer = context.socket(zmq.DEALER)
    dealer.connect('tcp://127.0.0.1:%d' % port)

    wait_signal = Task(signal_q.get(), loop=loop)

    while True:
        wait_router = delay(loop, router.recv_multipart)
        [first], [other] = yield From(asyncio.wait([wait_router, wait_signal],
                                      return_when=asyncio.FIRST_COMPLETED))

        if first is wait_signal:        # Interrupt socket recv
            dealer.send(b'break')
            addr, data = yield From(wait_router)  # should be fast
            assert data == b'break'

        while not outgoing_q.empty():  # Flow data out
            addr, msg = outgoing_q.get_nowait()
            router.send_multipart([addr, msg])
            print("Message sent")

        if first is wait_signal:        # Handle internal messages
            msg = wait_signal.result()
            if msg == b'close':
                control_q.put_nowait((None, b'close'))
                break
            elif msg == b'interrupt':
                wait_signal = Task(signal_q.get(), loop=loop)
                continue
        elif first is wait_router:      # Handle external messages
            addr, byts = wait_router.result()
            msg = loads(byts)
            print("Communication received: %s" % str(msg))
            control_q.put_nowait((addr, msg))

    router.close(1)
    dealer.close(1)

    raise Return("Done communicating")


@asyncio.coroutine
def control(control_q, work_q, send_q, data):
    """ Control coroutine, general dispatching

    Input Channels:
        control_q: Mailbox for any messages that come in from comm

    Output Channels:
        work_q: jobs for the worker
        send_q: people ask us to send them data
    """
    print("Control boots up")
    while True:
        addr, msg = yield From(control_q.get())
        if msg == b'close':
            work_q.put_nowait((addr, msg))
            send_q.put_nowait((addr, msg))
            break
        elif msg['op'] == 'compute':
            work_q.put_nowait((addr, msg))
        elif msg['op'] == 'get-data':
            data = {k: data[k] for k in msg['keys']
                                if k in data}
            send_q.put_nowait((addr, data))
        elif msg['op'] == 'ping':
            send_q.put_nowait((addr, b'pong'))
        else:
            raise NotImplementedError("Bad Message: %s" % msg)
    raise Return("Done listening")


@asyncio.coroutine
def send(send_q, outgoing_q, signal_q):
    """ Prep outgoing data before sending out on the wire

    In particular the router is currently doing a blocking recv.  We need to
    load an interrupt onto the signal queue as we load up the message onto the
    outgoing queue

    Input Channels:
        send_q:  Messages of (addr, obj) pairs

    Output Channels:
        outgoing_q:  Messages of (addr, bytes) pairs
        signal_q:  An interrupt signal to break the current block on the socket
    """
    print("Send boots up")
    while True:
        addr, msg = yield From(send_q.get())
        if msg == b'close':
            break

        if not isinstance(msg, bytes):
            msg = dumps(msg)
        outgoing_q.put_nowait((addr, msg))
        signal_q.put_nowait('interrupt')

    raise Return("Done sending")


@asyncio.coroutine
def work(work_q, send_q, data, metadata_addr, address, loop=None):
    """ Work coroutine

    Input Channels:
        work_q: Main mailbox, get work requests from control
        metadata_addr: This directly communicates with the MDStore via ZMQ
        data: A dictionary of local data.  This manages some state.

    Output Channels:
        send_q: Send acknowledgements of task finished (or failed) to requeter
        data:  A (possibly modified) dictionary of local data.
    """
    print("Worker boots up")
    loop = loop or asyncio.get_event_loop()
    while True:
        addr, msg = yield From(work_q.get())
        if msg == 'close':
            break

        key, func, args, kwargs, needed = \
                get(['key', 'function', 'args', 'kwargs', 'needed'], msg, None)

        try:
            d = yield From(get_data(loop, needed, data, metadata_addr))
        except KeyError as e:
            out = {'op': 'computation-failed',
                   'key': msg['key'],
                   'error': e}
        else:
            args2 = keys_to_data(args or (), d)
            kwargs2 = keys_to_data(kwargs or {}, d)

            # result = yield From(delay(loop, func, *args2, **kwargs2))
            result = func(*args2, **kwargs2)

            data[key] = result

            # Register ourselves with the metadata store
            req = {'op': 'register', 'keys': [key], 'address': address,
                    'reply': True}
            response = yield From(dealer_send_recv(loop, metadata_addr, req))
            assert response == b'OK'

            out = {'op': 'computation-finished',
                   'key': msg['key']}

        send_q.put_nowait((addr, out))

    raise Return("Done working")


@asyncio.coroutine
def get_data(loop, keys, data, metadata_addr, update=False):
    local = {k: data[k] for k in keys if k in data}
    missing = [k for k in keys if k not in local] if keys else []

    while missing:  # Are we missing anything?
        # Ask who has the keys we want
        msg = {'op': 'who-has', 'keys': missing}
        who_has = yield From(dealer_send_recv(loop, metadata_addr, msg))
        lost = set(missing) - set(k for k, v in who_has.items() if v)
        if lost:
            raise KeyError("Missing keys {%s}" % ', '.join(map(str, lost)))
        print("Collecting %s" % who_has)

        # get those keys from remote sources
        coroutines = [dealer_send_recv(loop, random.choice(list(who_has[k])),
                                       {'op': 'get-data', 'keys': [k]})
                                    for k in missing]
        other = yield From(asyncio.gather(*coroutines))

        # Merge in to local and make sure we aren't still missing anything
        local.update(merge(other))
        missing = [k for k in keys if k not in local]

    if update:
        data.update(local)

    raise Return(local)


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

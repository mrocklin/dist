from concurrent.futures import ThreadPoolExecutor
import random

from dill import dumps, loads
from toolz import merge, get
import trollius as asyncio
from trollius import From, Return, Task
import zmq

context = zmq.Context()


@asyncio.coroutine
def comm(ip, port, bind_ip, signal_q, control_q, outgoing_q,
         loop=None, context=None):
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

        if first is wait_signal:       # Handle internal messages
            msg = wait_signal.result()
            if msg == b'close':
                control_q.put_nowait((None, b'close'))
                break
            elif msg == b'interrupt':
                wait_signal = Task(signal_q.get(), loop=loop)
                continue
        elif first is wait_router:     # Handle external messages
            addr, byts = wait_router.result()
            msg = loads(byts)
            print("Communication received: %s" % str(msg))
            control_q.put_nowait((addr, msg))

    router.close(linger=2)
    dealer.close(linger=2)

    raise Return("Comm done")


@asyncio.coroutine
def heartbeat(heartbeat_q, send_q):
    """ A simple heartbeat coroutine

    Input Channels:
        heartbeat_q: should have messages of the form
                     (address, {'op': 'ping'}

    Output Channels:
        send_q: send out messages of the form
                (address, b'pong')
    """
    print("Heartbeat boots up")
    while True:
        addr, msg = yield From(heartbeat_q.get())
        if msg == b'close':
            break

        send_q.put_nowait((addr, b'pong'))

    raise Return("Heartbeat done")


@asyncio.coroutine
def control(control_q, out_qs):
    """ Control coroutine, general dispatch

    Input Channels:
        control_q: Mailbox for any messages that come in from comm

    Output Channels:
        out_qs: a dictionary of operator: queue pairs
                {'compute': compute_q, 'heartbeat': heartbeat_q}

    """
    print("Control boots up")
    while True:
        addr, msg = yield From(control_q.get())
        if msg == b'close':
            for q in set(out_qs.values()):
                q.put_nowait((addr, b'close'))
            break
        try:
            q = out_qs[msg['op']]
        except KeyError:
            raise NotImplementedError("Don't know how to route: %s" % msg)
        q.put_nowait((addr, msg))

    raise Return("Control done")


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

        print("Enque outgoing message: %s" % str(msg))
        if not isinstance(msg, bytes):
            msg = dumps(msg)
        outgoing_q.put_nowait((addr, msg))
        signal_q.put_nowait('interrupt')

    raise Return("Send done")


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


executor = ThreadPoolExecutor(20)


def delay(loop, func, *args, **kwargs):
    """ Run function in separate thread, turn into coroutine """
    future = executor.submit(func, *args, **kwargs)
    return asyncio.futures.wrap_future(future, loop=loop)

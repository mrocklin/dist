from collections import defaultdict
from dill import dumps, loads
from threading import Thread
import zmq


context = zmq.Context()


class MDStore(object):
    def __init__(self, ip, port, bind_ip='*', loop=None):
        self.ip = ip
        self.port = int(port)
        self.bind_ip = ip
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)

    @property
    def address(self):
        return 'tcp://%s:%d' % (self.ip, self.port)

    def listen(self):
        while True:
            try:
                socks = dict(self.poller.poll())
            except KeyboardInterrupt:
                break
            if self._local_router in socks:
                addr, bytes = self._local_router.recv_multipart()
            elif self.router in socks:
                addr, bytes = self.router.recv_multipart()
            msg = loads(bytes)
            print("MDStore received: %s" % str(msg))
            if msg['op'] == 'close':
                break
            elif msg['op'] == 'who-has':
                result = {k: self.who_has[k] for k in msg['keys']}
                self.send(addr, result)
            elif msg['op'] == 'register':
                self.has_what[msg['address']].update(msg['keys'])
                print("Register: %s" % str(msg))
                for key in msg['keys']:
                    self.who_has[key].add(msg['address'])
                if msg.get('reply'):
                    self.send(addr, b'OK')
            elif msg['op'] == 'unregister':
                for key in msg['keys']:
                    if key in self.has_what[msg['address']]:
                        self.has_what[msg['address']].remove(key)
                    try:
                        self.who_has[key].remove(msg['address'])
                    except KeyError:
                        pass
                print("UnRegister: %s" % str(msg))
                if msg.get('reply'):
                    self.send(addr, b'OK')
            elif msg['op'] == 'list':
                result = set(self.has_what)
                self.send(addr, result)

    def send(self, addr, msg):
        msg = dumps(msg)
        self.router.send_multipart([addr, msg])

    def start(self, separate_thread=True):
        self.poller = zmq.Poller()

        self.router = context.socket(zmq.ROUTER)
        self.router.bind('tcp://%s:%d' % (self.bind_ip, self.port))
        self.poller.register(self.router, zmq.POLLIN)

        self._local_router = context.socket(zmq.ROUTER)
        port = self._local_router.bind_to_random_port('tcp://127.0.0.1')
        self._local_dealer = context.socket(zmq.DEALER)
        self._local_dealer.connect('tcp://127.0.0.1:%d' % port)

        self.poller.register(self._local_router, zmq.POLLIN)

        if separate_thread:
            self._thread = Thread(target=self.listen)
            self._thread.start()
        else:
            self.listen()

    def close(self):
        self._local_dealer.send(dumps({'op': 'close'}))
        self._thread.join()
        self.router.close()
        self._local_router.close()
        self._local_dealer.close()

from dist.worker import Worker
import trollius as asyncio

def test_Worker():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    w = Worker('tcp://127.0.0.1:8000', 'tcp://127.0.0.1:8001',
               None, loop=loop)
    w.start()

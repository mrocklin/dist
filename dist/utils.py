import trollius as asyncio
from concurrent.futures import ThreadPoolExecutor


executor = ThreadPoolExecutor(2)


def delay(loop, func, *args, **kwargs):
    """ Run function in separate thread, turn into coroutine """
    future = executor.submit(func, *args, **kwargs)
    return asyncio.futures.wrap_future(future, loop=loop)

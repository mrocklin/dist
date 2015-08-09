from dist import MDStore
from contextlib import contextmanager


@contextmanager
def mdstore():
    mds = MDStore('tcp://*:8003')
    mds.start()

    try:
        yield mds
    finally:
        mds.close()


def test_mdstore():
    with mdstore() as mds:
        pass

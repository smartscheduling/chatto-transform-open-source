from tempfile import mkstemp
import os
import os.path
import contextlib

from chatto_transform.config import config

def make_temporary_file(tmp_dir=None):
    if tmp_dir is None:
        tmp_dir = os.path.join(config.data_dir, 'tmp')
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    return mkstemp(dir=tmp_dir)[1]


@contextlib.contextmanager
def deleting(tmp_path):
    try:
        yield
    finally:
        os.remove(tmp_path)
import shelve
import os.path

from sqlalchemy import create_engine

class MimicLoginException(Exception):
    pass

def get_config():
    try:
        from chatto_transform.config import mimic_config
        return mimic_config
    except ImportError:
        raise MimicLoginException('config/mimic_config.py not found. You will be unable to log into the database until you create this file.')

    
def get_engine():
    mimic_config = get_config()
    return create_engine(mimic_config.mimic_psql_config)

def get_local_storage_dir():
    mimic_config = get_config()
    return getattr(mimic_config, 'local_storage_dir', None)

def get_db_file():
    local_storage_dir = get_local_storage_dir()
    if local_storage_dir is not None:
        return os.path.join(local_storage_dir, '_session')


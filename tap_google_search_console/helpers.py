import os
from enum import Enum


def get_abs_path(path: str):
    """
    Returns absolute path for URL
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


class ReplicationMethod(Enum):
    """
    Available replication methods 
    """
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    LOG_BASED = "LOG_BASED"

""" Enumeration of supported cluster init script types."""
from enum import Enum


class InitScriptType(Enum):
    DBFS = 'dbfs'
    VOLUMES = 'volumes'
    WORKSPACE = 'workspace'

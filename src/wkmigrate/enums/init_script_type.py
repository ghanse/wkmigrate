"""Enumeration of supported cluster init script types."""

from enum import Enum


class InitScriptType(Enum):
    """
    File systems for storing cluster init scripts.

    Valid options:
        * ``DBFS``: Store init scripts in the Databricks File System (DBFS).
        * ``VOLUMES``: Store init scripts on Unity Catalog Volumes.
        * ``WORKSPACE``: Store init scripts in the Databricks workspace filesystem.
    """

    DBFS = "dbfs"
    VOLUMES = "volumes"
    WORKSPACE = "workspace"

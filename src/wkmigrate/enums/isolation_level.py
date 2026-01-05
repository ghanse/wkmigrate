"""SQL isolation level enumerations."""

from enum import StrEnum


class IsolationLevel(StrEnum):
    """
    Database transaction isolation levels.

    Valid options:
        * ``READ_COMMITTED``: Prevents dirty reads; default isolation level for many databases.
        * ``READ_UNCOMMITTED``: Allows dirty reads; lowest level of isolation.
        * ``REPEATABLE_READ``: Prevents non-repeatable reads within a transaction.
        * ``SERIALIZABLE``: Fully serializable isolation, preventing phantom reads.
        * ``SNAPSHOT``: Uses row versioning to provide a consistent snapshot of data.
    """

    READ_COMMITTED = "ReadCommitted"
    READ_UNCOMMITTED = "ReadUncommitted"
    REPEATABLE_READ = "RepeatableRead"
    SERIALIZABLE = "Serializable"
    SNAPSHOT = "Snapshot"

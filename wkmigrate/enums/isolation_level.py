from enum import StrEnum


class IsolationLevel(StrEnum):
    ReadCommitted = 'READ_COMMITTED',
    ReadUncommitted = 'READ_UNCOMMITTED',
    RepeatableRead = 'REPEATABLE_READ',
    Serializable = 'SERIALIZABLE',
    Snapshot = 'SNAPSHOT'

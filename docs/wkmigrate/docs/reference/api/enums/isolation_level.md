---
sidebar_label: isolation_level
title: wkmigrate.enums.isolation_level
---

SQL isolation level enumerations.

## IsolationLevel Objects

```python
class IsolationLevel(StrEnum)
```

Database transaction isolation levels.

Valid options:
    * ``READ_COMMITTED``: Prevents dirty reads; default isolation level for many databases.
    * ``READ_UNCOMMITTED``: Allows dirty reads; lowest level of isolation.
    * ``REPEATABLE_READ``: Prevents non-repeatable reads within a transaction.
    * ``SERIALIZABLE``: Fully serializable isolation, preventing phantom reads.
    * ``SNAPSHOT``: Uses row versioning to provide a consistent snapshot of data.


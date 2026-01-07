---
sidebar_label: init_script_type
title: wkmigrate.enums.init_script_type
---

Enumeration of supported cluster init script types.

## InitScriptType Objects

```python
class InitScriptType(Enum)
```

File systems for storing cluster init scripts.

Valid options:
    * ``DBFS``: Store init scripts in the Databricks File System (DBFS).
    * ``VOLUMES``: Store init scripts on Unity Catalog Volumes.
    * ``WORKSPACE``: Store init scripts in the Databricks workspace filesystem.


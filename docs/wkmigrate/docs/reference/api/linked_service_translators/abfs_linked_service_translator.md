---
sidebar_label: abfs_linked_service_translator
title: wkmigrate.linked_service_translators.abfs_linked_service_translator
---

Translate Azure Blob/File Storage linked services into IR.

### translate\_abfs\_spec

```python
def translate_abfs_spec(abfs_spec: dict) -> AbfsLinkedService
```

Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

**Arguments**:

- `abfs_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  ABFS linked-service metadata as a ``AbfsLinkedService`` object.


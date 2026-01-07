---
sidebar_label: sql_server_linked_service_translator
title: wkmigrate.linked_service_translators.sql_server_linked_service_translator
---

This module defines methods for translating Azure SQL Server linked services from data pipeline definitions.

### translate\_sql\_server\_spec

```python
def translate_sql_server_spec(sql_server_spec: dict) -> SqlLinkedService
```

Parses a SQL Server linked service definition into an ``SqlLinkedService`` object.

**Arguments**:

- `sql_server_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  SQL Server linked-service metadata as a ``SqlLinkedService`` object.


---
sidebar_label: linked_service_translators
title: wkmigrate.translators.linked_service_translators
---

This module defines translators for Azure Data Factory linked service definitions.

Translators in this module normalize linked service payloads into internal representations. 
Translators must validate required fields, coerce connection settings, and emit an ``UnsupportedValue`` 
objects for any unparsable inputs.

#### translate\_abfs\_spec

```python
def translate_abfs_spec(
        abfs_spec: dict) -> AbfsLinkedService | UnsupportedValue
```

Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

**Arguments**:

- `abfs_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  ABFS linked-service metadata as a ``AbfsLinkedService`` object.

#### translate\_databricks\_cluster\_spec

```python
def translate_databricks_cluster_spec(
        cluster_spec: dict
) -> DatabricksClusterLinkedService | UnsupportedValue
```

Parses a Databricks linked service definition into a ``DatabricksClusterLinkedService`` object.

**Arguments**:

- `cluster_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  Databricks cluster linked-service metadata as a ``DatabricksClusterLinkedService`` object.

#### translate\_sql\_server\_spec

```python
def translate_sql_server_spec(
        sql_server_spec: dict) -> SqlLinkedService | UnsupportedValue
```

Parses a SQL Server linked service definition into an ``SqlLinkedService`` object.

**Arguments**:

- `sql_server_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  SQL Server linked-service metadata as a ``SqlLinkedService`` object.


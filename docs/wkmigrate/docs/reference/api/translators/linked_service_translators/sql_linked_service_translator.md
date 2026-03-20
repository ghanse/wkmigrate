---
sidebar_label: sql_linked_service_translator
title: wkmigrate.translators.linked_service_translators.sql_linked_service_translator
---

Translators for SQL-based linked service definitions.

This module normalizes SQL Server, PostgreSQL, MySQL, and Oracle linked-service
payloads into ``SqlLinkedService`` objects.

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

#### translate\_postgresql\_spec

```python
def translate_postgresql_spec(
        postgresql_spec: dict) -> SqlLinkedService | UnsupportedValue
```

Parses an Azure Database for PostgreSQL linked service definition into an ``SqlLinkedService`` object.

**Arguments**:

- `postgresql_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  PostgreSQL linked-service metadata as a ``SqlLinkedService`` object.

#### translate\_mysql\_spec

```python
def translate_mysql_spec(
        mysql_spec: dict) -> SqlLinkedService | UnsupportedValue
```

Parses an Azure Database for MySQL linked service definition into an ``SqlLinkedService`` object.

**Arguments**:

- `mysql_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  MySQL linked-service metadata as a ``SqlLinkedService`` object.

#### translate\_oracle\_spec

```python
def translate_oracle_spec(
        oracle_spec: dict) -> SqlLinkedService | UnsupportedValue
```

Parses an Oracle Database linked service definition into an ``SqlLinkedService`` object.

**Arguments**:

- `oracle_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  Oracle linked-service metadata as a ``SqlLinkedService`` object.


---
sidebar_label: sql_dataset_translator
title: wkmigrate.translators.dataset_translators.sql_dataset_translator
---

Translators for SQL-based dataset definitions.

This module normalizes SQL Server, PostgreSQL, MySQL, and Oracle dataset payloads
into ``SqlTableDataset`` objects, parsing table/schema names and linked-service metadata.

#### translate\_sql\_server\_dataset

```python
def translate_sql_server_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates a SQL Server dataset definition into a ``SqlTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  SQL Server dataset as a ``SqlTableDataset`` object.

#### translate\_postgresql\_dataset

```python
def translate_postgresql_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates an Azure Database for PostgreSQL dataset definition into a ``SqlTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  PostgreSQL dataset as a ``SqlTableDataset`` object.

#### translate\_mysql\_dataset

```python
def translate_mysql_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates an Azure Database for MySQL dataset definition into a ``SqlTableDataset`` object.

MySQL does not use a separate schema namespace, so ``schema_name`` is always ``None``
and ``dbtable`` contains only the bare table name.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  MySQL dataset as a ``SqlTableDataset`` object.

#### translate\_oracle\_dataset

```python
def translate_oracle_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates an Oracle Database dataset definition into a ``SqlTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Oracle dataset as a ``SqlTableDataset`` object.


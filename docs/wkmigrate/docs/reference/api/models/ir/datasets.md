---
sidebar_label: datasets
title: wkmigrate.models.ir.datasets
---

Dataset IR models.

## Dataset Objects

```python
@dataclass
class Dataset()
```

Base class representing a parsed dataset.

**Attributes**:

- `dataset_name` - Logical name of the dataset as defined in ADF.
- `dataset_type` - Normalized dataset type (for example ``csv``, ``delta``, ``sqlserver``).
- `service_name` - Name of the backing service or linked service associated with this dataset.

## FileDataset Objects

```python
@dataclass
class FileDataset(Dataset)
```

Dataset definition for file-based sources and sinks in an ABFS/ADLS storage account.

**Attributes**:

- `container` - Storage container name hosting the files.
- `folder_path` - Directory or path prefix inside the container.
- `storage_account_name` - Storage account name for ABFS/ADLS access.
- `url` - Fully qualified URL to the storage endpoint, when available.
- `format_options` - File-format specific options (delimiter, header flags, compression, and so on).
- `records_per_file` - Maximum number of rows per file when writing out data.

## DeltaTableDataset Objects

```python
@dataclass
class DeltaTableDataset(Dataset)
```

Dataset definition for Delta tables accessible from a Databricks cluster.

**Attributes**:

- `database_name` - Name of the Hive/Unity Catalog database containing the table.
- `table_name` - Name of the Delta table within the database.
- `catalog_name` - Catalog name when using Unity Catalog.

## SqlTableDataset Objects

```python
@dataclass
class SqlTableDataset(Dataset)
```

Dataset definition for JDBC-accessible tables in a relational database.

**Attributes**:

- `schema_name` - Database schema that contains the table.
- `table_name` - Table name referenced by the dataset.
- `dbtable` - Fully qualified ``schema.table`` name used by JDBC.
- `host` - Hostname or address of the database server.
- `database` - Logical database name within the server.
- `user_name` - Username used for JDBC authentication.
- `authentication_type` - Authentication mechanism used by the connection.
- `connection_options` - Additional JDBC options (for example fetch size, partitioning).

## DatasetProperties Objects

```python
@dataclass
class DatasetProperties()
```

Container for dataset property metadata produced during parsing.

**Attributes**:

- `dataset_type` - Normalized dataset type string matching the associated ``Dataset``.
- `options` - Dictionary of format- or connection-specific options derived from ADF properties.

## UnsupportedDataset Objects

```python
@dataclass
class UnsupportedDataset(Dataset)
```

IR representation for a dataset that cannot be translated.

**Attributes**:

- `message` - Description of why the dataset is unsupported.
- `adf_definition` - Raw ADF dataset payload that could not be parsed.


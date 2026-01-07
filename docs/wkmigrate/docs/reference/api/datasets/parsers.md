---
sidebar_label: parsers
title: wkmigrate.datasets.parsers
---

This module defines methods for parsing datasets into IR models.

### parse\_avro\_file\_dataset

```python
def parse_avro_file_dataset(dataset: dict) -> FileDataset
```

Parses an Avro dataset definition into a FileDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Avro dataset as a ``FileDataset`` object.
  

**Raises**:

- `ValueError` - If the ABFS linked service definition is missing or not a dictionary.

### parse\_avro\_file\_properties

```python
def parse_avro_file_properties(properties: dict) -> DatasetProperties
```

Parses Avro dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - Avro properties block from the dataset definition.
  

**Returns**:

  Avro dataset properties as a ``DatasetProperties`` object.

### parse\_delimited\_file\_dataset

```python
def parse_delimited_file_dataset(dataset: dict) -> FileDataset
```

Parses a delimited-text dataset definition into a FileDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Delimited-text dataset as a ``FileDataset`` object.

### parse\_delimited\_file\_properties

```python
def parse_delimited_file_properties(properties: dict) -> DatasetProperties
```

Parses delimited-text dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - Delimited-text properties block.
  

**Returns**:

  Delimited-text dataset properties as a ``DatasetProperties`` object.

### parse\_delta\_table\_dataset

```python
def parse_delta_table_dataset(dataset: dict) -> DeltaTableDataset
```

Parses a Delta dataset definition into a DeltaTableDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Delta table dataset as a ``DeltaTableDataset`` object.

### parse\_delta\_properties

```python
def parse_delta_properties(properties: dict) -> DatasetProperties
```

Parses Delta dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - Delta properties block.
  

**Returns**:

  Delta dataset properties as a ``DatasetProperties`` object.

### parse\_json\_file\_dataset

```python
def parse_json_file_dataset(dataset: dict) -> FileDataset
```

Parses a JSON dataset definition into a FileDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  JSON dataset as a ``FileDataset`` object.

### parse\_json\_file\_properties

```python
def parse_json_file_properties(properties: dict) -> DatasetProperties
```

Parses JSON dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - JSON properties block.
  

**Returns**:

  JSON dataset properties as a ``DatasetProperties`` object.

### parse\_orc\_file\_dataset

```python
def parse_orc_file_dataset(dataset: dict) -> FileDataset
```

Parses an ORC dataset definition into a FileDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  ORC dataset as a ``FileDataset`` object.

### parse\_orc\_file\_properties

```python
def parse_orc_file_properties(properties: dict) -> DatasetProperties
```

Parses ORC dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - ORC properties block.
  

**Returns**:

  ORC dataset properties as a ``DatasetProperties`` object.

### parse\_parquet\_file\_dataset

```python
def parse_parquet_file_dataset(dataset: dict) -> FileDataset
```

Parses a Parquet dataset definition into a FileDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Parquet dataset as a ``FileDataset`` object.

### parse\_parquet\_file\_properties

```python
def parse_parquet_file_properties(properties: dict) -> DatasetProperties
```

Parses Parquet dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - Parquet properties block.
  

**Returns**:

  Parquet dataset properties as a ``DatasetProperties`` object.

### parse\_sql\_server\_dataset

```python
def parse_sql_server_dataset(dataset: dict) -> SqlTableDataset
```

Parses a SQL Server dataset definition into a SqlTableDataset.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  SQL Server dataset as a ``SqlTableDataset`` object.

### parse\_sql\_server\_properties

```python
def parse_sql_server_properties(properties: dict) -> DatasetProperties
```

Parses SQL Server dataset properties into a DatasetProperties object.

**Arguments**:

- `properties` - SQL Server properties block.
  

**Returns**:

  SQL Server dataset properties as a ``DatasetProperties`` object.


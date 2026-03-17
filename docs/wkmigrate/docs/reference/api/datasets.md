---
sidebar_label: datasets
title: wkmigrate.datasets
---

This module defines dataset constants, type mappings, and shared helpers for working with datasets.

Dataset constants define the ADF dataset types that the library can translate, the secret keys
required per dataset type, and the Spark options emitted for each format.  Type-mapping helpers
normalize source-system column types into Spark equivalents.  Shared helpers convert ``Dataset``
and ``DatasetProperties`` IR objects into flat dictionaries and collect the ``SecretInstruction``
objects needed to materialise credentials in a Databricks workspace.

#### parse\_spark\_data\_type

```python
def parse_spark_data_type(sink_type: str, sink_system: str) -> str
```

Converts a source-system data type to the Spark equivalent.

**Arguments**:

- `sink_type` - Data type string defined in the source system.
- `sink_system` - Identifier for the source system (for example ``"sqlserver"``).
  

**Returns**:

  Spark-compatible data type string.
  

**Raises**:

- `ValueError` - If the sink system is unsupported.
- `ValueError` - If the sink type is not supported for the given system.

#### merge\_dataset\_definition

```python
def merge_dataset_definition(
        dataset: Dataset | dict | None,
        properties: DatasetProperties | dict | None) -> dict
```

Merges a ``Dataset`` IR object and its associated properties into a single flat dictionary.

**Arguments**:

- `dataset` - Parsed dataset or pre-built dictionary.
- `properties` - Parsed dataset properties or pre-built dictionary.
  

**Returns**:

  Flat dictionary combining all dataset and property fields.

#### dataset\_to\_dict

```python
def dataset_to_dict(dataset: Dataset | dict) -> dict
```

Converts a ``Dataset`` IR object into a dictionary.

**Arguments**:

- `dataset` - Parsed dataset or pre-built dictionary.
  

**Returns**:

  Dictionary representation of the dataset.

#### dataset\_properties\_to\_dict

```python
def dataset_properties_to_dict(
        properties: DatasetProperties | dict | None) -> dict
```

Converts ``DatasetProperties`` into a dictionary.

**Arguments**:

- `properties` - Parsed dataset properties object or pre-built dictionary.
  

**Returns**:

  Flat dictionary representation of the dataset properties with ``None`` values removed.

#### collect\_data\_source\_secrets

```python
def collect_data_source_secrets(definition: dict) -> list[SecretInstruction]
```

Builds the list of ``SecretInstruction`` objects required for a dataset definition.

Each provider type declares a set of secret keys in ``DATASET_PROVIDER_SECRETS``.
This helper creates one ``SecretInstruction`` per declared key, stamped with the
service name and type so the workspace deployer can materialise the secrets.

File datasets resolve secrets by ``provider_type`` (e.g. ``"abfs"``, ``"s3"``).
SQL datasets resolve secrets by ``service_type`` (e.g. ``"sqlserver"``).

**Arguments**:

- `definition` - Flat dataset definition dictionary produced by ``merge_dataset_definition``.
  

**Returns**:

  List of ``SecretInstruction`` objects. The list is empty when the dataset
  type or service name is missing, or when the type has no required secrets.


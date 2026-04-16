---
sidebar_label: file_dataset_translator
title: wkmigrate.translators.dataset_translators.file_dataset_translator
---

Translator for file-based dataset definitions (Avro, CSV, JSON, ORC, Parquet).

This module normalizes file-based dataset payloads into ``FileDataset`` objects,
parsing storage paths, linked-service metadata, and format options for ABFS,
Amazon S3, Google Cloud Storage, and Azure Blob Storage locations.

#### translate\_file\_dataset

```python
@translates_dataset("Avro", "DelimitedText", "Json", "Orc", "Parquet")
def translate_file_dataset(
        dataset_type: str,
        dataset: dict,
        provider_type: str | None = None) -> FileDataset | UnsupportedValue
```

Translates a file-based dataset definition into a ``FileDataset`` object.

Supports ABFS, Amazon S3, Google Cloud Storage, and Azure Blob Storage
locations.  When *provider_type* is ``None`` the provider is inferred from
``dataset.properties.location.type`` using ``CLOUD_LOCATION_TYPES``.

**Arguments**:

- `dataset_type` - ADF dataset type (e.g. ``"DelimitedText"``, ``"Parquet"``).
- `dataset` - Raw dataset definition from Azure Data Factory.
- `provider_type` - Cloud provider identifier. When ``None`` the provider is
  inferred from the location type.
  

**Returns**:

  File dataset as a ``FileDataset`` object.


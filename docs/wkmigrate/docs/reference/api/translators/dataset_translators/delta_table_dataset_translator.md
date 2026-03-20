---
sidebar_label: delta_table_dataset_translator
title: wkmigrate.translators.dataset_translators.delta_table_dataset_translator
---

Translator for Delta table dataset definitions.

This module normalizes Delta table dataset payloads into ``DeltaTableDataset``
objects, parsing Databricks linked-service metadata and table properties.

#### translate\_delta\_table\_dataset

```python
def translate_delta_table_dataset(
        dataset: dict) -> DeltaTableDataset | UnsupportedValue
```

Translates a Delta table dataset definition into a ``DeltaTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Delta table dataset as a ``DeltaTableDataset`` object.


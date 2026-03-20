---
sidebar_label: dataset_translator
title: wkmigrate.translators.dataset_translators.dataset_translator
---

Top-level dataset translator that dispatches to type-specific translators.

This module normalizes dataset payloads into internal representations. Each
translator validates required fields, coerces connection settings, and emits
``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_dataset

```python
def translate_dataset(dataset: dict) -> Dataset | UnsupportedValue
```

Translates a dataset definition returned by the Azure Data Factory API into a ``Dataset`` object. Supports files, SQL tables, and Delta tables. Any datasets which cannot be fully translated will return an ``UnsupportedValue`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Dataset as a ``Dataset`` object.


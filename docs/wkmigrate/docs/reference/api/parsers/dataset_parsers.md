---
sidebar_label: dataset_parsers
title: wkmigrate.parsers.dataset_parsers
---

This module defines parsers for dataset properties shared across translators.

Parsers in this module normalize raw ADF dataset definitions into structured format
options and type-specific metadata. Parsers should emit ``UnsupportedValue`` objects 
for any unparsable inputs.

#### parse\_format\_options

```python
def parse_format_options(dataset: dict) -> dict | UnsupportedValue
```

Parses the format options from a dataset definition. Parsing format options for dataset types which are not supported will return an ``UnsupportedValue`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Format options as a ``dict`` object.


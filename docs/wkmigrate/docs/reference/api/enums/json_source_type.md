---
sidebar_label: json_source_type
title: wkmigrate.enums.json_source_type
---

Enumerations describing JSON pipeline sources.

## JSONSourceType Objects

```python
class JSONSourceType(StrEnum)
```

Supported JSON definition sources for definition stores.

Valid options:
    * ``DATABRICKS_WORKFLOW``: JSON files that contain Databricks Jobs or Workflows definitions.
    * ``DATA_FACTORY_PIPELINE``: JSON files that contain Azure Data Factory pipeline definitions.


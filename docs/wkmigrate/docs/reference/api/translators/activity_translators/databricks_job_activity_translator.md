---
sidebar_label: databricks_job_activity_translator
title: wkmigrate.translators.activity_translators.databricks_job_activity_translator
---

This module defines a translator for Databricks Job activities.

Translators in this module normalize Databricks Job activity payloads into internal
representations. Each translator must validate required fields and emit ``UnsupportedValue``
objects for any unparsable inputs.

#### translate\_databricks\_job\_activity

```python
@translates_activity("DatabricksJob")
def translate_databricks_job_activity(
        activity: dict,
        base_kwargs: dict) -> RunJobActivity | UnsupportedValue
```

Translates an ADF Databricks Job activity into a ``RunJobActivity`` object.

**Arguments**:

- `activity` - Databricks Job activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
  

**Returns**:

  ``RunJobActivity`` referencing the existing Databricks job, or an
  ``UnsupportedValue`` if ``existing_job_id`` is missing.


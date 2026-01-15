---
sidebar_label: copy_activity_translator
title: wkmigrate.translators.activity_translators.copy_activity_translator
---

This module defines a translator for translating Copy activities.

Translators in this module normalize Copy Data activity payloads into internal representations.
Each translator must validate required fields, coerce connection settings, source and sink dataset
properties, and column mappings.Translators should emit ``UnsupportedValue`` objects for any unparsable 
inputs.

#### translate\_copy\_activity

```python
def translate_copy_activity(
        activity: dict, base_kwargs: dict) -> CopyActivity | UnsupportedValue
```

Translates an ADF Copy activity into a ``CopyActivity`` object. Copy activities are translated into Lakeflow Declarative Pipelines tasks or Notebook tasks depending on the source and target dataset types.

This method returns an ``UnsupportedValue`` if the activity cannot be translated. This can be due to:
* Missing or invalid dataset definitions
* Missing required dataset properties
* Unsupported dataset types
* Unsupported dataset format settings

**Arguments**:

- `activity` - Copy activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``CopyActivity`` representation of the Copy task.


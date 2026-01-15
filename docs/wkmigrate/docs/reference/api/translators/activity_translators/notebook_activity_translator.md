---
sidebar_label: notebook_activity_translator
title: wkmigrate.translators.activity_translators.notebook_activity_translator
---

This module defines a translator for translating Databricks Notebook activities.

Translators in this module normalize Databricks Notebook activity payloads into internal 
representations. Each translator must validate required fields, parse the activity's parameters, 
and emit ``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_notebook\_activity

```python
def translate_notebook_activity(
        activity: dict, base_kwargs: dict) -> DatabricksNotebookActivity
```

Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

**Arguments**:

- `activity` - Notebook activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
  

**Returns**:

  ``DatabricksNotebookActivity`` representation of the notebook task.


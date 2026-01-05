---
sidebar_label: notebook_activity_translator
title: wkmigrate.activity_translators.notebook_activity_translator
---

This module defines methods for translating Databricks Notebook activities.

### translate\_notebook\_activity

```python
def translate_notebook_activity(
        activity: dict, base_kwargs: dict) -> DatabricksNotebookActivity
```

Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

**Arguments**:

- `activity` - Notebook activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``DatabricksNotebookActivity`` representation of the notebook task.


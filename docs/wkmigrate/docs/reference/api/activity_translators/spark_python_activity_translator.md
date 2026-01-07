---
sidebar_label: spark_python_activity_translator
title: wkmigrate.activity_translators.spark_python_activity_translator
---

This module defines methods for translating Databricks Spark Python activities.

### translate\_spark\_python\_activity

```python
def translate_spark_python_activity(activity: dict,
                                    base_kwargs: dict) -> SparkPythonActivity
```

Translates an ADF Databricks Spark Python activity into a ``SparkPythonActivity`` object.

**Arguments**:

- `activity` - Spark Python activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``SparkPythonActivity`` representation of the Spark Python task.


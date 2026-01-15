---
sidebar_label: spark_python_activity_translator
title: wkmigrate.translators.activity_translators.spark_python_activity_translator
---

This module defines a translator for translating Databricks Spark Python activities.

Translators in this module normalize Databricks Spark Python activity payloads into internal 
representations. Each translator must validate required fields, parse the activity's parameters, 
and emit ``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_spark\_python\_activity

```python
def translate_spark_python_activity(activity: dict,
                                    base_kwargs: dict) -> SparkPythonActivity
```

Translates an ADF Databricks Spark Python activity into a ``SparkPythonActivity`` object.

**Arguments**:

- `activity` - Spark Python activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
  

**Returns**:

  ``SparkPythonActivity`` representation of the Spark Python task.


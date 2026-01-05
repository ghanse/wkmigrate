---
sidebar_label: spark_jar_activity_translator
title: wkmigrate.activity_translators.spark_jar_activity_translator
---

This module defines methods for translating Databricks Spark jar activities.

### translate\_spark\_jar\_activity

```python
def translate_spark_jar_activity(activity: dict,
                                 base_kwargs: dict) -> SparkJarActivity
```

Translates an ADF Databricks Spark JAR activity into a ``SparkJarActivity`` object.

**Arguments**:

- `activity` - Spark JAR activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata from ``_build_base_activity_kwargs``.
  

**Returns**:

  ``SparkJarActivity`` representation of the Spark JAR task.


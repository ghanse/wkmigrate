---
sidebar_label: spark_jar_activity_translator
title: wkmigrate.translators.activity_translators.spark_jar_activity_translator
---

This module defines a translator for translating Databricks Spark jar activities.

Translators in this module normalize Databricks Spark JAR activity payloads into internal 
representations. Each translator must validate required fields, parse the activity's parameters, 
and emit ``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_spark\_jar\_activity

```python
def translate_spark_jar_activity(activity: dict,
                                 base_kwargs: dict) -> SparkJarActivity
```

Translates an ADF Databricks Spark JAR activity into a ``SparkJarActivity`` object.

**Arguments**:

- `activity` - Spark JAR activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
  

**Returns**:

  ``SparkJarActivity`` representation of the Spark JAR task.


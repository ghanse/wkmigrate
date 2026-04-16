---
sidebar_label: lookup_activity_translator
title: wkmigrate.translators.activity_translators.lookup_activity_translator
---

This module defines a translator for translating Lookup activities.

Translators in this module normalize ADF Lookup activity payloads into internal
representations.  A Lookup activity reads data from a dataset (file or database)
and returns the result as a task value so that downstream tasks can reference it.
Each translator must validate required fields, parse the source dataset and its
properties, and emit ``UnsupportedValue`` objects for any unparsable inputs.

#### translate\_lookup\_activity

```python
@translates_activity("Lookup")
def translate_lookup_activity(
        activity: dict,
        base_kwargs: dict) -> LookupActivity | UnsupportedValue
```

Translates an ADF Lookup activity into a ``LookupActivity`` object.

Lookup activities are translated into notebook tasks that read data via Spark
(either a native file source or a database using JDBC), collect the rows, and
publish the result as a Databricks task value.

This method returns an ``UnsupportedValue`` if the activity cannot be translated
due to missing or invalid dataset definitions or unsupported dataset types.

**Arguments**:

- `activity` - Lookup activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
  

**Returns**:

  ``LookupActivity`` representation of the lookup task.


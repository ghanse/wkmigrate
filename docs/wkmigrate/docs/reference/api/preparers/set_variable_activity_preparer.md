---
sidebar_label: set_variable_activity_preparer
title: wkmigrate.preparers.set_variable_activity_preparer
---

This module defines a preparer for SetVariable activities.

The preparer builds a Databricks notebook task that evaluates the translated variable
expression and sets a Databricks task value via ``dbutils.jobs.taskValues.set()``.
Downstream tasks can retrieve the value from ``dbutils.jobs.taskValues``.

#### prepare\_set\_variable\_activity

```python
def prepare_set_variable_activity(
        activity: SetVariableActivity) -> PreparedActivity
```

Builds tasks and artifacts for a SetVariable activity.

**Arguments**:

- `activity` - :class:`SetVariableActivity` IR instance produced by the translator.
  

**Returns**:

  :class:`PreparedActivity` containing the notebook task configuration and
  the generated notebook artifact.


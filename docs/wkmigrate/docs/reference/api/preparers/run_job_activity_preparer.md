---
sidebar_label: run_job_activity_preparer
title: wkmigrate.preparers.run_job_activity_preparer
---

This module defines a preparer for Run Job activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Run Job activity. This includes all nested properties
and tasks of the job to be run.

#### prepare\_run\_job\_activity

```python
def prepare_run_job_activity(
        activity: RunJobActivity,
        default_files_to_delta_sinks: bool | None) -> PreparedActivity
```

Builds the task payload for a Run Job activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators
- `default_files_to_delta_sinks` - Optional override for DLT generation of inner activities.
  

**Returns**:

  Prepared activity containing the Run Job task configuration.


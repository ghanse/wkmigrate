---
sidebar_label: preparer
title: wkmigrate.preparers.preparer
---

This module defines a preparer for creating Databricks Lakeflow jobs from an ADF
pipeline which has been translated with wkmigrate.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed
to replicate the pipeline's functionality. This includes job settings, task definitions,
notebooks, pipelines, and secrets to be created in the target workspace.

#### prepare\_workflow

```python
def prepare_workflow(
        pipeline: Pipeline,
        files_to_delta_sinks: bool | None = None) -> PreparedWorkflow
```

Prepares a pipeline internal representation for creation as a Databricks Lakeflow job.

**Arguments**:

- `pipeline` - Pipeline internal representation to prepare.
- `files_to_delta_sinks` - Overrides the inferred Files-to-Delta behavior when set.
  

**Returns**:

  Prepared workflow containing the Databricks job payload and supporting artifacts for the pipeline.

#### prepare\_activity

```python
def prepare_activity(
        activity: Activity,
        default_files_to_delta_sinks: bool | None) -> PreparedActivity
```

Prepares an activity internal representation for creation as a Databricks Lakeflow job task.

**Arguments**:

- `activity` - Activity internal representation to prepare.
- `default_files_to_delta_sinks` - Whether to use the default files-to-delta sinks behavior.
  

**Returns**:

  Prepared activity containing the task configuration and any associated artifacts.


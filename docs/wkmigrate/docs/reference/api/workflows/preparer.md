---
sidebar_label: preparer
title: wkmigrate.workflows.preparer
---

Utilities for preparing Databricks workflow artifacts.

### prepare\_workflow

```python
def prepare_workflow(
        pipeline_definition: Pipeline,
        files_to_delta_sinks: bool | None = None) -> PreparedWorkflow
```

Translates a pipeline definition into notebook, pipeline, and secret artifacts.

**Arguments**:

- `pipeline_definition` - Parsed pipeline IR produced by the translator.
- `files_to_delta_sinks` - Overrides the inferred Files-to-Delta behavior when set.
  

**Returns**:

  Prepared workflow containing the Databricks job payload and supporting artifacts.


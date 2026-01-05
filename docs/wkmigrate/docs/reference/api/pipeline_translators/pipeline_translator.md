---
sidebar_label: pipeline_translator
title: wkmigrate.pipeline_translators.pipeline_translator
---

This module defines methods for translating data pipelines.

### translate\_pipeline

```python
def translate_pipeline(pipeline: dict) -> Pipeline
```

Translates an ADF pipeline dictionary into a ``Pipeline``.

**Arguments**:

- `pipeline` - Raw pipeline payload exported from ADF.
  

**Returns**:

  Dataclass representation including tasks, schedule, and tags.


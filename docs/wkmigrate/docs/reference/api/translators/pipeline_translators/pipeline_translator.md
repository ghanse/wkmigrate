---
sidebar_label: pipeline_translator
title: wkmigrate.translators.pipeline_translators.pipeline_translator
---

This module defines a pipeline-level translator from Azure Data Factory to internal IR.

Pipeline translators in this module call activity, parameter, and trigger translators to produce
an internal representation from an input ADF pipeline. They collect ``UnsupportedValue`` objects
and warnings so that callers can surface translation diagnostics alongside the generated payload.

#### translate\_pipeline

```python
def translate_pipeline(pipeline: dict) -> Pipeline
```

Translates an ADF pipeline dictionary into a ``Pipeline``.

**Arguments**:

- `pipeline` - Raw pipeline payload exported from ADF.
  

**Returns**:

  Dataclass representation including tasks, schedule, and tags.


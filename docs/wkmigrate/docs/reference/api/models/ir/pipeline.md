---
sidebar_label: pipeline
title: wkmigrate.models.ir.pipeline
---

This module defines internal representations for pipelines.

Pipelines in this module represent the top-level container for a pipeline. Each pipeline contains 
metadata, parameters, schedules, and tasks. Pipelines are translated from ADF payloads into internal 
representations that can be used to generate Databricks Lakeflow jobs.

## PipelineTask Objects

```python
@dataclass
class PipelineTask()
```

Wrapper associating an ``Activity`` with a workflow task slot.

**Attributes**:

- `activity` - Translated activity instance that will be executed as a Databricks task.

## Pipeline Objects

```python
@dataclass
class Pipeline()
```

Pipeline IR object produced by the translator.

**Attributes**:

- `name` - Logical pipeline name derived from the ADF pipeline.
- `parameters` - List of pipeline parameter definitions, or ``None`` when no parameters are defined.
- `schedule` - Serialized schedule definition for the pipeline trigger, if any.
- `tasks` - Ordered list of ``PipelineTask`` wrappers that make up the workflow.
- `tags` - Dictionary of system and user-defined tags attached to the workflow.
- `not_translatable` - Collection of warnings describing properties that could not be translated.


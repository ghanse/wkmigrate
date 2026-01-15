---
sidebar_label: artifacts
title: wkmigrate.models.workflows.artifacts
---

This module defines representational classes for Databricks workflow artifacts.

## NotebookArtifact Objects

```python
@dataclass
class NotebookArtifact()
```

Represents a notebook that needs to be materialized.

**Attributes**:

- `file_path` - Workspace path where the notebook will be created or updated.
- `content` - Notebook source content as a single string.
- `language` - Notebook language (for example ``python`` or ``sql``). Defaults to ``"python"``.

## PreparedWorkflow Objects

```python
@dataclass
class PreparedWorkflow()
```

Artifacts generated while preparing a workflow.

**Attributes**:

- `job_settings` - Databricks Jobs payload describing the workflow to be created.
- `notebooks` - List of ``NotebookArtifact`` objects to upload.
- `pipelines` - List of ``PipelineInstruction`` objects describing DLT pipelines to create.
- `secrets` - List of ``SecretInstruction`` objects describing secrets to materialize.
- `unsupported` - Collection of entries describing properties or nodes that could not be translated.


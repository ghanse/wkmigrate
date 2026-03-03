---
sidebar_label: artifacts
title: wkmigrate.models.workflows.artifacts
---

This module defines representational classes for Databricks workflow artifacts.

## PreparedWorkflow Objects

```python
@dataclass(slots=True)
class PreparedWorkflow()
```

Artifacts generated while preparing a workflow.

**Attributes**:

- `pipeline` - Pipeline IR that this workflow was prepared from.
- `activities` - Prepared activities that make up this workflow's tasks.

#### tasks

```python
@property
def tasks() -> list[dict[str, Any]]
```

Task dicts for the activities at this level.

#### all\_notebooks

```python
@property
def all_notebooks() -> list[NotebookArtifact]
```

All notebooks across this workflow and any nested inner workflows.

#### all\_pipelines

```python
@property
def all_pipelines() -> list[PipelineInstruction]
```

All pipeline instructions across this workflow and any nested inner workflows.

#### all\_secrets

```python
@property
def all_secrets() -> list[SecretInstruction]
```

All secret instructions across this workflow and any nested inner workflows.

#### inner\_workflows

```python
@property
def inner_workflows() -> list["PreparedWorkflow"]
```

All inner workflows (recursively) produced by activities in this workflow.

## PreparedActivity Objects

```python
@dataclass(slots=True)
class PreparedActivity()
```

Artifacts generated while preparing a workflow task.

**Attributes**:

- `task` - Task configuration as a dictionary.
- `notebooks` - List of ``NotebookArtifact`` objects to upload.
- `pipelines` - List of ``PipelineInstruction`` objects describing DLT pipelines to create.
- `secrets` - List of ``SecretInstruction`` objects describing secrets to materialize.
- `inner_workflow` - Additional workflow settings created for nested ForEach tasks.

## NotebookArtifact Objects

```python
@dataclass(slots=True)
class NotebookArtifact()
```

Represents a notebook that needs to be materialized.

**Attributes**:

- `file_path` - Workspace path where the notebook will be created or updated.
- `content` - Notebook source content as a single string.
- `language` - Notebook language (for example ``python`` or ``sql``). Defaults to ``"python"``.


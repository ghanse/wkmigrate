---
sidebar_label: instructions
title: wkmigrate.models.workflows.instructions
---

This module defines representational classes for Databricks workflow instructions.

## PipelineInstruction Objects

```python
@dataclass
class PipelineInstruction()
```

Represents a workflow pipeline that must be created.

**Attributes**:

- `task_ref` - Reference to the Databricks task dictionary that will consume the pipeline.
- `file_path` - Workspace path where the pipeline&#x27;s notebook or script is stored.
- `name` - Name to assign to the Databricks pipeline.

### local\_identifier

```python
@property
def local_identifier() -> str
```

Returns the local identifier for the pipeline.

## SecretInstruction Objects

```python
@dataclass
class SecretInstruction()
```

Represents a secret value that must exist in Databricks.

**Attributes**:

- `scope` - Name of the Databricks secret scope that will store the secret.
- `key` - Secret key name within the scope.
- `service_name` - Logical source system or service associated with the secret.
- `service_type` - Type of backing service (for example ``sqlserver`` or ``csv``).
- `provided_value` - Secret value obtained from source metadata, if available.
- `user_input_required` - ``True`` when the user must provide the secret value interactively.


---
sidebar_label: workspace_definition_store
title: wkmigrate.definition_stores.workspace_definition_store
---

This module defines the `WorkspaceDefinitionStore` class used to load and persist pipeline definitions in a Databricks workspace.

``WorkspaceDefinitionStore`` materializes translated pipelines into Databricks
Lakeflow Jobs, generates notebooks and Spark Declarative Pipelines for copying
data, and can list or update workspace assets. It is commonly used as the sink
when migrating from ADF definitions to Databricks.

**Example**:

    ```python
    from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore

    store = WorkspaceDefinitionStore(authentication_type="pat", host_name="https://adb-123.azuredatabricks.net", pat="TOKEN")
    workflow = store.load("existing_job_name")  # raises ValueError if missing
    store.dump(translated_pipeline_ir)
    ```

## WorkspaceDefinitionStore Objects

```python
@dataclass
class WorkspaceDefinitionStore(DefinitionStore)
```

Definition store implementation that lists, describes, and updates objects in a Databricks workspace.

**Attributes**:

- `authentication_type` - Authentication mode. Can be "pat", "basic", or "azure-client-secret".
- `host_name` - Workspace hostname for Databricks.
- `pat` - Personal access token used for "pat" authentication.
- `username` - Username used for "basic" authentication.
- `password` - Password used for "basic" authentication.
- `resource_id` - Azure resource ID for workspace-scoped authentication flows.
- `tenant_id` - Azure AD tenant identifier used for client-secret authentication.
- `client_id` - Application (client) ID used for client-secret authentication.
- `client_secret` - Secret associated with the client ID for client-secret authentication.
- `files_to_delta_sinks` - Overrides default behavior when generating DLT sinks from copy tasks.
- `workspace_client` - Databricks workspace client used to interact with the Databricks workspace. Automatically created using the provided credentials.

#### \_\_post\_init\_\_

```python
def __post_init__() -> None
```

Validates credentials and initializes the Databricks workspace client.

**Raises**:

- `ValueError` - If the authentication type is invalid or the host name is not provided.

#### load

```python
def load(pipeline_name: str) -> dict
```

Fetches a Databricks job definition by name.

**Arguments**:

- `pipeline_name` - Job name inside the target workspace.
  

**Returns**:

- `dict` - Job settings returned by the Jobs API.

#### to\_pipeline

```python
def to_pipeline(pipeline_definition: dict) -> int | None
```

Uploads artifacts and creates a Databricks job.

**Arguments**:

- `pipeline_definition` - Serialized ``Pipeline`` dataclass payload as a ``dict``.
  

**Returns**:

  Optional job identifier registered in the workspace.
  

**Raises**:

- `ValueError` - If the job cannot be created.

#### dump

```python
@deprecated("Use 'to_pipeline' as of wkmigrate 0.0.3")
def dump(pipeline_definition: dict) -> int | None
```

This method is deprecated. Use ``to_pipeline`` instead. Uploads artifacts and creates a Databricks job.

**Arguments**:

- `pipeline_definition` - Serialized ``Pipeline`` dataclass payload as a ``dict``.
  

**Returns**:

  Optional job identifier registered in the workspace.
  

**Raises**:

- `ValueError` - If the job cannot be created.

#### to\_local\_files

```python
def to_local_files(pipeline_definition: Pipeline,
                   local_directory: str) -> None
```

Materializes notebooks, workflows definitions, secret definitions, and unsupported nodes as files in a local directory.

**Arguments**:

- `pipeline_definition` - Prepared pipeline as a ``Pipeline``.
- `local_directory` - Destination directory for generated artifacts.


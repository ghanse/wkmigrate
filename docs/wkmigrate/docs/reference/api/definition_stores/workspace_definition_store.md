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
    store.to_local_files(translated_pipeline_ir, local_directory="/path/to/local/directory")
    store.to_job(translated_pipeline_ir)
    ```

## WorkspaceDefinitionStore Objects

```python
@dataclasses.dataclass(slots=True)
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

#### to\_job

```python
def to_job(pipeline_definition: Pipeline) -> int | None
```

Uploads artifacts and creates a Databricks job.

**Arguments**:

- `pipeline_definition` - ``Pipeline`` dataclass.
  

**Returns**:

  Optional job identifier registered in the workspace.
  

**Raises**:

- `ValueError` - If the job cannot be created.

#### dump

```python
@deprecated("Use 'to_job' as of wkmigrate 0.0.3")
def dump(pipeline_definition: Pipeline) -> int | None
```

This method is deprecated. Use ``to_job`` instead. Uploads artifacts and creates a Databricks job.

**Arguments**:

- `pipeline_definition` - Serialized ``Pipeline`` dataclass payload as a ``dict``.
  

**Returns**:

  Optional job identifier registered in the workspace.
  

**Raises**:

- `ValueError` - If the job cannot be created.

#### to\_local\_files

```python
@deprecated("Use 'to_asset_bundle' as of wkmigrate 0.0.3")
def to_local_files(pipeline_definition: Pipeline,
                   local_directory: str) -> None
```

Creates a Databricks asset bundle containing the workflow definition, notebooks, secrets, and unsupported nodes.

**Arguments**:

- `pipeline_definition` - Prepared pipeline as a ``Pipeline``.
- `local_directory` - Destination directory for generated artifacts.

#### to\_asset\_bundle

```python
def to_asset_bundle(pipeline_definition: Pipeline | dict,
                    bundle_directory: str,
                    download_notebooks: bool = True) -> None
```

Creates a Databricks asset bundle containing the workflow definition, notebooks, secrets, and unsupported nodes.

**Arguments**:

- `pipeline_definition` - Prepared pipeline as a ``Pipeline`` or raw dictionary payload.
- `bundle_directory` - Destination directory for the bundle artifacts.
- `download_notebooks` - If True, downloads referenced notebooks from the workspace to the bundle.


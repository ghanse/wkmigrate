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
- `options` - Dictionary of options that customize workflow generation and deployment behaviour.
- `workspace_client` - Databricks workspace client used to interact with the Databricks workspace. Automatically created using the provided credentials.

#### \_\_post\_init\_\_

```python
def __post_init__() -> None
```

Validates credentials, option keys, and initializes the Databricks workspace client.

**Raises**:

- `ValueError` - If the authentication type is invalid or the host name is not provided.
- `ValueError` - If any option key is not a recognised key.
- `ValueError` - If the ``compute_type`` option value is not a supported compute type.

#### set\_option

```python
def set_option(key: str, value: Any) -> None
```

Sets the value of a single option.

**Arguments**:

- `key` - Option name. Must be one of the recognised option keys.
- `value` - New value for the option.
  

**Raises**:

- `ValueError` - If *key* is not a recognised option.
- `ValueError` - If *key* is ``compute_type`` and *value* is not a supported compute type.

#### set\_options

```python
def set_options(options: dict[str, Any]) -> None
```

Replaces all options with the provided dictionary.

**Arguments**:

- `options` - Dictionary of option key-value pairs. All keys must be recognised.
  

**Raises**:

- `ValueError` - If any key is not a recognised option.
- `ValueError` - If the ``compute_type`` value is not a supported compute type.

#### to\_jobs

```python
def to_jobs(pipeline_definitions: list[Pipeline]) -> list[int]
```

Uploads artifacts and creates a Databricks job for each pipeline.

**Arguments**:

- `pipeline_definitions` - List of ``Pipeline`` dataclasses to deploy.
  

**Returns**:

  List of job identifiers registered in the workspace.

#### to\_asset\_bundles

```python
def to_asset_bundles(pipeline_definitions: list[Pipeline],
                     bundle_directory: str,
                     download_notebooks: bool = True) -> None
```

Creates a Databricks asset bundle for each pipeline inside a shared parent directory.

Each pipeline is written to a subdirectory named after the pipeline.

**Arguments**:

- `pipeline_definitions` - List of ``Pipeline`` dataclasses to export.
- `bundle_directory` - Parent directory for all generated bundles.
- `download_notebooks` - If True, downloads referenced notebooks from the workspace.

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

When ``download_notebooks`` is True, workspace notebook paths are extracted
using the original (pre-rewrite) paths so that downloads succeed.  The
``root_path`` rewrite is applied after the download-path mapping.

**Arguments**:

- `pipeline_definition` - Prepared pipeline as a ``Pipeline`` or raw dictionary payload.
- `bundle_directory` - Destination directory for the bundle artifacts.
- `download_notebooks` - If True, downloads referenced notebooks from the workspace to the bundle.


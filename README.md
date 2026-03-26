# Databricks Workflows Migrator (`wkmigrate`)

[![PyPi package](https://img.shields.io/pypi/v/wkmigrate?color=green)](https://pypi.org/project/wkmigrate)
[![PyPi downloads](https://img.shields.io/pypi/dm/wkmigrate?label=PyPi%20Downloads)](https://pypistats.org/packages/wkmigrate)

## Project Description

`wkmigrate` is a Python library for migrating data pipelines from Azure Data Factory (ADF) to
Databricks Lakeflow Jobs. It reads ADF pipeline definitions, translates them into an intermediate
representation, and materializes the result as Databricks jobs, asset bundles, or local files.

## Installation

```bash
pip install wkmigrate
```

## Quick Start

### Translate an ADF pipeline via the management API

```python
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore

# Connect to your ADF instance
factory_store = FactoryDefinitionStore(
    tenant_id="<AZURE_TENANT_ID>",
    client_id="<SERVICE_PRINCIPAL_CLIENT_ID>",
    client_secret="<SERVICE_PRINCIPAL_CLIENT_SECRET>",
    subscription_id="<AZURE_SUBSCRIPTION_ID>",
    resource_group_name="<RESOURCE_GROUP_NAME>",
    factory_name="<DATA_FACTORY_NAME>",
)

# Load and translate a pipeline
pipeline = factory_store.load("my_adf_pipeline")

# Create the equivalent Databricks job
workspace_store = WorkspaceDefinitionStore(
    authentication_type="pat",
    host_name="https://adb-<workspace-id>.<region>.azuredatabricks.net",
    pat="<DATABRICKS_PAT>",
)
job_id = workspace_store.to_pipeline(pipeline)
```

### Translate from exported JSON files

```python
from wkmigrate.definition_stores.json_definition_store import JsonDefinitionStore

json_store = JsonDefinitionStore(
    source_directory="/path/to/adf-export",
)
pipeline = json_store.load("my_pipeline")
```

The JSON store expects files organized in subdirectories:

```
source_directory/
├── pipelines/          # Pipeline definition JSON files
├── triggers/           # Trigger JSON files (optional)
├── datasets/           # Dataset JSON files (optional)
└── linked_services/    # Linked-service JSON files (optional)
```

### Generate a Databricks asset bundle

```python
workspace_store.to_asset_bundle(
    pipeline_definition=pipeline,
    bundle_directory="out/my_bundle",
)
```

## Supported Activity Types

| ADF Activity Type | Databricks Equivalent |
|---|---|
| DatabricksNotebook | Notebook task |
| DatabricksSparkJar | Spark JAR task |
| DatabricksSparkPython | Spark Python task |
| DatabricksJob | Run job task |
| Copy Data | Generated notebook (Spark read/write) |
| Lookup | Generated notebook (Spark read + task value) |
| ForEach | For-each task |
| IfCondition | If/else task |
| SetVariable | Set variable task |
| Web Activity | Generated notebook (HTTP request) |
| Unsupported types | Placeholder notebook |

## Supported Dataset Types

**File formats:** Avro, CSV (DelimitedText), JSON, ORC, Parquet

**Cloud storage:** Azure Blob FS (ABFS), Azure Blob Storage, Amazon S3, Google Cloud Storage

**Databases:** Azure SQL, PostgreSQL, MySQL, Oracle

**Lakehouse:** Azure Databricks Delta Lake

## Documentation

Full documentation is available at the [wkmigrate documentation site](https://ghanse.github.io/wkmigrate/).

## Compatibility

`wkmigrate` requires Python 3.12+. Some features (e.g. serverless compute) may require a
premium-tier Databricks workspace.

## Contributing

See the [development guide](https://ghanse.github.io/wkmigrate/guide) for setup instructions.

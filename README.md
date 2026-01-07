# Databricks Workflows Migrator (`wkmigrate`)

[![PyPi package](https://img.shields.io/pypi/v/wkmigrate?color=green)](https://pypi.org/project/wkmigrate)
[![PyPi downloads](https://img.shields.io/pypi/dm/wkmigrate?label=PyPi%20Downloads)](https://pypistats.org/packages/wkmigrate)

## Project Description
`wkmigrate` is a Python library for migrating data pipelines to Databricks workflows from various
frameworks. Users can programmatically create or migrate workflows with a simple set of commands.

Pipeline definitions are read from a user-specified source system, translated for compatibility
with Databricks workflows, and either directly created or stored in `json` or `yml` files.

## Installation

Use `pip install wkmigrate` to install the PyPi package.

## Compatibility 
`wkmigrate` is a standalone project. Using some features (e.g. serverless jobs compute options) may
require a premium-tier Databricks workspace.

## Using the Workflow Migrator
To use the `wkmigrate`, install the library using the `%pip install wkmigrate` method or install the 
Python wheel directly in your environment.

Once the library has been installed, create source and target **definition stores** for the migration.

```buildoutcfg
from wkmigrate.definition_store_builder import build_definition_store

# Create the source definition store (an ADF instance):
factory_options = {
    "tenant_id": "<TENANT_ID>",
    "client_id": "<CLIENT_ID>",
    "client_secret": "<CLIENT_SECRET>",
    "subscription_id": "<SUBSCRIPTION_ID>",
    "resource_group_name": "<RESOURCE_GROUP_NAME>",
    "factory_name": "<FACTORY_NAME>"
}
factory_store = build_definition_store(
    "factory_definition_store", 
    factory_options
)

# Create the target definition store (a Databricks workspace):
workspace_options = {
    "authentication_type": "pat",
    "host_name": "<DATABRICKS_HOST_URL>",
    "pat": "<DATABRICKS_PERSONAL_ACCESS_TOKEN>",
}
workspace_store = build_definition_store(
    "workspace_definition_store", 
    workspace_options
)                        
```

Use the `load` method to **get definitions** from a source.

```buildoutcfg
pipeline = factory_store.load(pipeline_name="<PIPELINE_NAME>")                      
```

Use `pipeline_translator.translate()` to **translate definitions** for compatibility
with Databricks workflows.

```buildoutcfg
from wkmigrate import pipeline_translator
translated_pipeline = pipeline_translator.translate(pipeline)
```

Use the ``to_pipeline`` method to **sync workflows** into a target.

```buildoutcfg
workspace_store.to_pipeline(translated_pipeline)
```

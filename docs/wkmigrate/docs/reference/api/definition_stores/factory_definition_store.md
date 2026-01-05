---
sidebar_label: factory_definition_store
title: wkmigrate.definition_stores.factory_definition_store
---

This module defines the ``FactoryDefinitionStore`` class.

## FactoryDefinitionStore Objects

```python
@dataclass
class FactoryDefinitionStore(DefinitionStore)
```

Definition store implementation backed by an Azure Data Factory instance.

**Attributes**:

- `tenant_id` - Azure AD tenant identifier.
- `client_id` - Service principal application (client) ID.
- `client_secret` - Secret used to authenticate the client.
- `subscription_id` - Azure subscription identifier.
- `resource_group_name` - Resource group name for the factory.
- `factory_name` - Name of the Azure Data Factory instance.
- `factory_client` - Concrete ``FactoryClient`` used to load pipelines and child resources. Automatically created using the provided credentials.

### \_\_post\_init\_\_

```python
def __post_init__() -> None
```

Validates configuration and initializes the Factory client.

**Raises**:

- `ValueError` - If the tenant ID is not provided.
- `ValueError` - If the client ID is not provided.
- `ValueError` - If the client secret is not provided.
- `ValueError` - If the subscription ID is not provided.
- `ValueError` - If the resource group name is not provided.
- `ValueError` - If the factory name is not provided.

### load

```python
def load(pipeline_name: str) -> dict
```

Returns a dictionary representation of a Data Factory pipeline.

**Arguments**:

- `pipeline_name` - Name of the pipeline to load as a ``str``.
  

**Returns**:

  Pipeline definition decorated with linked resources as a ``dict``.
  

**Raises**:

- `ValueError` - If the factory client is not initialized.

### dump

```python
def dump(pipeline_definition: dict) -> None
```

**Notes**:

  Saving pipeline definitions to Azure Data Factory is not currently supported.
  

**Arguments**:

- `pipeline_definition` - Pipeline definition to dump as a ``dict``.
  

**Raises**:

- `NotImplementedError` - Always.


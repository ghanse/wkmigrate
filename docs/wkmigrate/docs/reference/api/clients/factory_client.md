---
sidebar_label: factory_client
title: wkmigrate.clients.factory_client
---

This module defines a client for interacting with Azure Data Factory.

Clients in this module interact with Azure Data Factory to retrieve pipeline, dataset, 
linked service, and trigger definitions. Clients validate required fields, authenticate 
using the provided credentials, and make API calls to the Data Factory resource using
the Azure Data Factory management client.

## FactoryClient Objects

```python
@dataclass
class FactoryClient()
```

Data Factory management client for retrieving pipelines, datasets, linked services, and triggers.

**Attributes**:

- `tenant_id` - Azure Active Directory tenant identifier used for authentication.
- `client_id` - Application (client) ID of the service principal.
- `client_secret` - Client secret associated with the service principal.
- `subscription_id` - Azure subscription identifier that hosts the Data Factory instance.
- `resource_group_name` - Resource group containing the Data Factory.
- `factory_name` - Name of the Azure Data Factory instance.
- `management_client` - ``DataFactoryManagementClient`` used to make API calls. Automatically created using the provided credentials.

#### \_\_post\_init\_\_

```python
def __post_init__() -> None
```

Sets up the Data Factory management client for the provided credentials.

#### get\_pipeline

```python
def get_pipeline(pipeline_name: str) -> dict
```

Gets a pipeline definition with the specified name.

**Arguments**:

- `pipeline_name` - Name of the Data Factory pipeline.
  

**Returns**:

  Pipeline definition as a ``dict``.

#### get\_linked\_service

```python
def get_linked_service(linked_service_name: str) -> dict
```

Gets a linked-service definition with the specified name.

**Arguments**:

- `linked_service_name` - Name of the linked service in Data Factory.
  

**Returns**:

  Linked-service definition as a ``dict``.

#### get\_trigger

```python
def get_trigger(pipeline_name: str) -> dict
```

Gets the trigger associated with a pipeline.

**Arguments**:

- `pipeline_name` - Name of the Data Factory pipeline.
  

**Returns**:

  Trigger definition as a ``dict``.

#### get\_dataset

```python
def get_dataset(dataset_name: str) -> dict
```

Gets the dataset definition for a specified dataset name.

**Arguments**:

- `dataset_name` - Dataset name from the Data Factory.
  

**Returns**:

  Dataset definition as a ``dict``.


---
sidebar_label: linked_services
title: wkmigrate.models.ir.linked_services
---

This module defines internal representations for linked services.

Linked services in this module represent connections to external services and systems used by 
various pipeline activities (e.g. Copy Data, Lookup Value). Each linked service contains metadata
specific to the associated service. Linked services are translated from ADF payloads into internal 
representations that can be used to connect to services from Databricks.

## LinkedService Objects

```python
@dataclass
class LinkedService()
```

Base class representing a translated linked service.

**Attributes**:

- `service_name` - Logical name of the linked service in ADF.
- `service_type` - ADF linked-service type string (for example ``AzureSqlDatabase`` or ``AzureDatabricks``).

## SqlLinkedService Objects

```python
@dataclass
class SqlLinkedService(LinkedService)
```

Linked-service metadata for SQL/JDBC connections to a relational database.

**Attributes**:

- `host` - Hostname or address of the database server.
- `database` - Logical database name within the server.
- `user_name` - Username used to authenticate with the database.
- `authentication_type` - Authentication mechanism (for example ``SQL`` or ``AAD``).

## AbfsLinkedService Objects

```python
@dataclass
class AbfsLinkedService(LinkedService)
```

Linked-service metadata for ABFS/ADLS storage accounts.

**Attributes**:

- `storage_account_name` - Storage account name for the ABFS endpoint.
- `url` - Fully qualified base URL for the storage account.

## DatabricksClusterLinkedService Objects

```python
@dataclass
class DatabricksClusterLinkedService(LinkedService)
```

Linked-service metadata describing a Databricks workspace/cluster.

**Attributes**:

- `host_name` - Databricks workspace hostname (for example ``adb-<id>.<region>.azuredatabricks.net``).
- `node_type_id` - Default worker node type identifier.
- `spark_version` - Runtime version string for the cluster.
- `custom_tags` - Custom cluster tags applied at creation time.
- `driver_node_type_id` - Node type for the driver, when different from workers.
- `spark_conf` - Spark configuration dictionary applied to the cluster.
- `spark_env_vars` - Environment variables made available to Spark.
- `init_scripts` - List of init script descriptors attached to the cluster.
- `cluster_log_conf` - Cluster log configuration dictionary.
- `autoscale` - Autoscaling configuration specifying ``min_workers`` and ``max_workers``.
- `num_workers` - Fixed number of workers when autoscaling is disabled.
- `pat` - Personal access token used for workspace authentication, when applicable.


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
@dataclass(slots=True)
class LinkedService()
```

Base class representing a translated linked service.

**Attributes**:

- `service_name` - Logical name of the linked service in ADF.
- `service_type` - ADF linked-service type string (for example ``AzureSqlDatabase`` or ``AzureDatabricks``).

## SqlLinkedService Objects

```python
@dataclass(slots=True)
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
@dataclass(slots=True)
class AbfsLinkedService(LinkedService)
```

Linked-service metadata for ABFS/ADLS storage accounts.

**Attributes**:

- `storage_account_name` - Storage account name for the ABFS endpoint.
- `url` - Fully qualified base URL for the storage account.

## S3LinkedService Objects

```python
@dataclass(slots=True)
class S3LinkedService(LinkedService)
```

Linked-service metadata for Amazon S3.

**Attributes**:

- `access_key_id` - AWS access key identifier used for authentication.
- `service_url` - Custom S3-compatible endpoint URL, when applicable.

## GcsLinkedService Objects

```python
@dataclass(slots=True)
class GcsLinkedService(LinkedService)
```

Linked-service metadata for Google Cloud Storage.

**Attributes**:

- `access_key_id` - HMAC access key identifier used for authentication.
- `service_url` - Custom GCS-compatible endpoint URL, when applicable.

## AzureBlobLinkedService Objects

```python
@dataclass(slots=True)
class AzureBlobLinkedService(LinkedService)
```

Linked-service metadata for Azure Blob Storage.

**Attributes**:

- `storage_account_name` - Storage account name parsed from the connection string.
- `url` - Storage account URL (can be a container URL, SAS URI, or service endpoint URL).

## DatabricksClusterLinkedService Objects

```python
@dataclass(slots=True)
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


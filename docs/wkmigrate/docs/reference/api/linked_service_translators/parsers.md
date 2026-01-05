---
sidebar_label: parsers
title: wkmigrate.linked_service_translators.parsers
---

Helpers for parsing Databricks linked-service payloads.

### parse\_log\_conf

```python
def parse_log_conf(cluster_log_destination: str | None) -> dict | None
```

Parses a cluster log configuration from a DBFS destination into a dictionary of log settings.

**Arguments**:

- `cluster_log_destination` - Cluster log delivery path in DBFS.
  

**Returns**:

  Cluster log configuration as a ``dict``.

### parse\_number\_of\_workers

```python
def parse_number_of_workers(num_workers: str | None) -> int | None
```

Parses a static cluster size from the linked-service payload into an integer.

**Arguments**:

- `num_workers` - Number of workers, represented as a string.
  

**Returns**:

  Parsed worker count as an ``int``, or ``None`` if autoscaling is used.

### parse\_autoscale\_policy

```python
def parse_autoscale_policy(autoscale_policy: str | None) -> dict | None
```

Parses a Databricks autoscaling policy (e.g., ``&quot;1:4&quot;``) into a dictionary of worker counts.

**Arguments**:

- `autoscale_policy` - Autoscaling range encoded as ``&quot;min:max&quot;``.
  

**Returns**:

  Dictionary with ``min_workers`` / ``max_workers`` as a ``dict``.

### parse\_init\_scripts

```python
def parse_init_scripts(init_scripts: list[str] | None) -> list[dict] | None
```

Parses the init-script list included in a linked-service definition into a list of init script definitions.

**Arguments**:

- `init_scripts` - Paths to init scripts.
  

**Returns**:

  List of init script definitions as a ``list[dict]``.

### parse\_storage\_account\_connection\_string

```python
def parse_storage_account_connection_string(connection_string: str) -> str
```

Parses an Azure Storage account connection string into a URL.

**Arguments**:

- `connection_string` - Azure Storage connection string.
  

**Returns**:

  Blob endpoint URL extracted from the connection string as a ``str``.

### parse\_storage\_account\_name

```python
def parse_storage_account_name(connection_string: str) -> str
```

Parses the storage account name from a connection string into a string.

**Arguments**:

- `connection_string` - Azure Storage connection string.
  

**Returns**:

  Storage account name as a ``str``.


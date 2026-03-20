---
sidebar_label: databricks_linked_service_translator
title: wkmigrate.translators.linked_service_translators.databricks_linked_service_translator
---

Translator for Databricks cluster linked service definitions.

This module normalizes Databricks linked-service payloads into
``DatabricksClusterLinkedService`` objects, parsing cluster sizing,
init scripts, log configuration, and Spark settings.

#### translate\_databricks\_cluster\_spec

```python
def translate_databricks_cluster_spec(
        cluster_spec: dict
) -> DatabricksClusterLinkedService | UnsupportedValue
```

Parses a Databricks linked service definition into a ``DatabricksClusterLinkedService`` object.

**Arguments**:

- `cluster_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  Databricks cluster linked-service metadata as a ``DatabricksClusterLinkedService`` object.


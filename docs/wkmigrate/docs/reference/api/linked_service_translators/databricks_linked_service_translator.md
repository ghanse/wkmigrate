---
sidebar_label: databricks_linked_service_translator
title: wkmigrate.linked_service_translators.databricks_linked_service_translator
---

This module defines methods for translating Databricks cluster services from data pipeline definitions.

### translate\_cluster\_spec

```python
def translate_cluster_spec(
        cluster_spec: dict) -> DatabricksClusterLinkedService
```

Parses a Databricks linked service definition into a ``DatabricksClusterLinkedService`` object.

**Arguments**:

- `cluster_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  Databricks cluster linked-service metadata as a ``DatabricksClusterLinkedService`` object.


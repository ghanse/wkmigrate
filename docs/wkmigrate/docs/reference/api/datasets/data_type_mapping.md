---
sidebar_label: data_type_mapping
title: wkmigrate.datasets.data_type_mapping
---

This module defines methods for mapping data types from target systems to Spark.

### parse\_spark\_data\_type

```python
def parse_spark_data_type(sink_type: str, sink_system: str) -> str
```

Converts a source-system data type to the Spark equivalent.

**Arguments**:

- `sink_type` - Data type string defined in the source system.
- `sink_system` - Identifier for the source system (e.g., ``&quot;sqlserver&quot;``).
  

**Returns**:

  Spark-compatible data type string.
  

**Raises**:

- `ValueError` - If the sink system is unsupported.
- `ValueError` - If the sink type is not supported for the given system.


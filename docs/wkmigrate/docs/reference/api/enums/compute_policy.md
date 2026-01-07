---
sidebar_label: compute_policy
title: wkmigrate.enums.compute_policy
---

Enumerations for supported compute policy values.

## ComputePolicy Objects

```python
class ComputePolicy(StrEnum)
```

Preferred compute policy for notebook tasks.

Valid options:
    * ``USE_SERVERLESS``: Run tasks on Databricks serverless compute when available.
    * ``USE_CLASSIC``: Run tasks on classic (non-serverless) compute clusters.


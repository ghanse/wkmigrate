---
sidebar_label: profiler
title: wkmigrate.profiler.profiler
---

Profile an Azure Data Factory resource to assess migration readiness.

#### profile\_factory

```python
def profile_factory(client: FactoryClient) -> FactoryProfile
```

Profile an Azure Data Factory resource.

**Arguments**:

- `client` - Authenticated ``FactoryClient`` for the target factory.
  

**Returns**:

  A ``FactoryProfile`` summarising the factory contents.

#### format\_profile

```python
def format_profile(profile: FactoryProfile) -> str
```

Format a FactoryProfile as human-readable text.

**Arguments**:

- `profile` - The factory profile to format.
  

**Returns**:

  Formatted multi-line string.


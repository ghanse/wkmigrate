---
sidebar_label: profile
title: wkmigrate.profiler.profile
---

Data models for Azure Data Factory profiling results.

## ObjectCount Objects

```python
@dataclass(slots=True)
class ObjectCount()
```

Counts of total, supported, and unsupported objects.

## DatasetDetail Objects

```python
@dataclass(slots=True)
class DatasetDetail()
```

Detail record for a single dataset.

## IntegrationRuntimeDetail Objects

```python
@dataclass(slots=True)
class IntegrationRuntimeDetail()
```

Detail record for a single integration runtime.

## FactoryProfile Objects

```python
@dataclass(slots=True)
class FactoryProfile()
```

Complete profile of an Azure Data Factory resource.


---
sidebar_label: supported_types
title: wkmigrate.supported_types
---

Canonical sets of ADF object types that wkmigrate can translate.

Translator modules register themselves at import time using the
``translates_activity`` and ``translates_dataset`` decorators. The
resulting sets are the single source of truth consumed by the profiler.

Linked-service types have no translator dispatch, so they are listed
explicitly.

#### translates\_activity

```python
def translates_activity(*adf_type_names: str) -> Callable[[_F], _F]
```

Register one or more ADF activity type names as supported.

Use as a decorator on the top-level translator function::

    @translates_activity("Copy")
    def translate_copy_activity(...): ...

#### translates\_dataset

```python
def translates_dataset(*adf_type_names: str) -> Callable[[_F], _F]
```

Register one or more ADF dataset type names as supported.

Use as a decorator on the top-level translator function::

    @translates_dataset("Avro", "DelimitedText")
    def translate_file_dataset(...): ...


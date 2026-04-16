"""Canonical sets of ADF object types that wkmigrate can translate.

Translator modules register themselves at import time using the
``translates_activity`` and ``translates_dataset`` decorators. The
resulting sets are the single source of truth consumed by the profiler.

Linked-service types have no translator dispatch, so they are listed
explicitly.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

_F = TypeVar("_F", bound=Callable)

SUPPORTED_ACTIVITY_TYPES: set[str] = set()
SUPPORTED_DATASET_TYPES: set[str] = set()

SUPPORTED_LINKED_SERVICE_TYPES: frozenset[str] = frozenset(
    {
        "AzureBlobFS",
        "AzureBlobStorage",
        "AzureSqlDatabase",
        "AzureDatabricks",
        "AmazonS3",
        "GoogleCloudStorage",
        "AzurePostgreSql",
        "AzureMySql",
        "Oracle",
    }
)


def translates_activity(*adf_type_names: str) -> Callable[[_F], _F]:
    """Register one or more ADF activity type names as supported.

    Use as a decorator on the top-level translator function::

        @translates_activity("Copy")
        def translate_copy_activity(...): ...
    """

    def decorator(func: _F) -> _F:
        SUPPORTED_ACTIVITY_TYPES.update(adf_type_names)
        return func

    return decorator


def translates_dataset(*adf_type_names: str) -> Callable[[_F], _F]:
    """Register one or more ADF dataset type names as supported.

    Use as a decorator on the top-level translator function::

        @translates_dataset("Avro", "DelimitedText")
        def translate_file_dataset(...): ...
    """

    def decorator(func: _F) -> _F:
        SUPPORTED_DATASET_TYPES.update(adf_type_names)
        return func

    return decorator

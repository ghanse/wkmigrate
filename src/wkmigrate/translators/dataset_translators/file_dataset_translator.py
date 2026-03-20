"""Translator for file-based dataset definitions (Avro, CSV, JSON, ORC, Parquet).

This module normalizes file-based dataset payloads into ``FileDataset`` objects,
parsing storage paths, linked-service metadata, and format options for ABFS,
Amazon S3, Google Cloud Storage, and Azure Blob Storage locations.
"""

from collections.abc import Callable
from wkmigrate.parsers.dataset_parsers import CLOUD_LOCATION_TYPES
from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators.utils import (
    get_linked_service_definition,
    parse_abfs_container_name,
    parse_abfs_file_path,
    parse_cloud_bucket_name,
    parse_cloud_file_path,
    parse_format_options,
)
from wkmigrate.translators.linked_service_translators import (
    translate_abfs_spec,
    translate_azure_blob_spec,
    translate_gcs_spec,
    translate_s3_spec,
)

_CLOUD_TRANSLATORS: dict[str, Callable] = {
    "s3": translate_s3_spec,
    "gcs": translate_gcs_spec,
    "azure_blob": translate_azure_blob_spec,
}


def translate_file_dataset(
    dataset_type: str, dataset: dict, provider_type: str | None = None
) -> FileDataset | UnsupportedValue:
    """
    Translates a file-based dataset definition into a ``FileDataset`` object.

    Supports ABFS, Amazon S3, Google Cloud Storage, and Azure Blob Storage
    locations.  When *provider_type* is ``None`` the provider is inferred from
    ``dataset.properties.location.type`` using ``CLOUD_LOCATION_TYPES``.

    Args:
        dataset_type: ADF dataset type (e.g. ``"DelimitedText"``, ``"Parquet"``).
        dataset: Raw dataset definition from Azure Data Factory.
        provider_type: Cloud provider identifier. When ``None`` the provider is
            inferred from the location type.

    Returns:
        File dataset as a ``FileDataset`` object.
    """
    if not dataset:
        provider_label = provider_type or "file"
        return UnsupportedValue(value=dataset, message=f"Missing {provider_label} dataset definition")

    # Resolve provider type from location when not explicitly given.
    if provider_type is None:
        properties = dataset.get("properties", {})
        location = properties.get("location", {})
        location_type = location.get("type")
        provider_type = CLOUD_LOCATION_TYPES.get(location_type) if location_type else None
        if provider_type is None:
            return UnsupportedValue(
                value=dataset,
                message=f"Unsupported file location type '{location_type}'",
            )

    if provider_type == "abfs":
        return _translate_abfs_file_dataset(dataset_type, dataset, provider_type)
    return _translate_cloud_file_dataset(dataset_type, dataset, provider_type)


def _translate_abfs_file_dataset(
    dataset_type: str, dataset: dict, provider_type: str
) -> FileDataset | UnsupportedValue:
    """Translate an ABFS-backed file dataset.

    Args:
        dataset_type: ADF dataset type (e.g. ``"DelimitedText"``, ``"Parquet"``).
        dataset: Raw dataset definition from Azure Data Factory.
        provider_type: Cloud provider identifier (always ``"abfs"`` for this translator).

    Returns:
        File dataset as a ``FileDataset`` object, or ``UnsupportedValue`` when parsing fails.
    """
    properties = dataset.get("properties", {})

    container_name = parse_abfs_container_name(properties)
    if isinstance(container_name, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=container_name.message)

    folder_path = parse_abfs_file_path(properties)
    if isinstance(folder_path, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=folder_path.message)

    linked_service_definition = get_linked_service_definition(dataset)
    if isinstance(linked_service_definition, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service_definition.message)

    linked_service = translate_abfs_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    format_options = parse_format_options(dataset_type, dataset)
    if isinstance(format_options, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=format_options.message)

    return FileDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type=dataset_type,
        container=container_name,
        folder_path=folder_path,
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
        provider_type=provider_type,
    )


def _translate_cloud_file_dataset(
    dataset_type: str, dataset: dict, provider_type: str
) -> FileDataset | UnsupportedValue:
    """Translate a cloud-storage-backed file dataset (S3, GCS, Azure Blob).

    Args:
        dataset_type: ADF dataset type (e.g. ``"DelimitedText"``, ``"Parquet"``).
        dataset: Raw dataset definition from Azure Data Factory.
        provider_type: Cloud provider identifier (e.g. ``"s3"``, ``"gcs"``, ``"azure_blob"``).

    Returns:
        File dataset as a ``FileDataset`` object, or ``UnsupportedValue`` when parsing fails.
    """
    properties = dataset.get("properties", {})

    container_name = parse_cloud_bucket_name(properties)
    if isinstance(container_name, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=container_name.message)

    folder_path = parse_cloud_file_path(properties)
    if isinstance(folder_path, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=folder_path.message)

    linked_service_definition = get_linked_service_definition(dataset)
    if isinstance(linked_service_definition, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service_definition.message)

    translator = _CLOUD_TRANSLATORS.get(provider_type)
    if translator is None:
        return UnsupportedValue(
            value=dataset,
            message=f"Unsupported cloud provider type '{provider_type}'",
        )

    linked_service = translator(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    format_options = parse_format_options(dataset_type, dataset)
    if isinstance(format_options, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=format_options.message)

    return FileDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type=dataset_type,
        container=container_name,
        folder_path=folder_path,
        storage_account_name=getattr(linked_service, "storage_account_name", None),
        service_name=linked_service.service_name,
        url=getattr(linked_service, "url", None) or getattr(linked_service, "service_url", None),
        format_options=format_options,
        provider_type=provider_type,
    )

"""Translators for cloud storage linked service definitions (ABFS, S3, GCS, Azure Blob).

This module normalizes ABFS, Amazon S3, Google Cloud Storage, and Azure Blob Storage
linked-service payloads into their respective internal representations.
"""

import warnings
from uuid import uuid4

from wkmigrate.models.ir.linked_services import (
    AbfsLinkedService,
    AzureBlobLinkedService,
    GcsLinkedService,
    S3LinkedService,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.linked_service_translators.utils import (
    parse_storage_account_connection_string,
    parse_storage_account_name,
)


def translate_abfs_spec(abfs_spec: dict) -> AbfsLinkedService | UnsupportedValue:
    """
    Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

    Args:
        abfs_spec: Linked-service definition from Azure Data Factory.

    Returns:
        ABFS linked-service metadata as a ``AbfsLinkedService`` object.
    """
    if not abfs_spec:
        return UnsupportedValue(value=abfs_spec, message="Missing ABFS linked service definition")

    properties = abfs_spec.get("properties", {})
    raw_url = properties.get("url")
    if raw_url is None:
        return UnsupportedValue(value=abfs_spec, message="Missing property 'url' in ABFS linked service definition")
    url = parse_storage_account_connection_string(raw_url)

    if isinstance(url, UnsupportedValue):
        return UnsupportedValue(
            value=abfs_spec, message=f"Invalid property 'url' in ABFS linked service definition; {url.message}"
        )

    storage_account_name = parse_storage_account_name(properties.get("storage_account_name") or properties.get("url"))
    if isinstance(storage_account_name, UnsupportedValue):
        return UnsupportedValue(
            value=abfs_spec,
            message=f"Invalid property 'storage_account_name' in ABFS linked service definition; {storage_account_name.message}",
        )

    return AbfsLinkedService(
        service_name=abfs_spec.get("name", str(uuid4())),
        service_type="abfs",
        url=url,
        storage_account_name=storage_account_name,
    )


def translate_s3_spec(s3_spec: dict) -> S3LinkedService | UnsupportedValue:
    """
    Parses an Amazon S3 linked service definition into an ``S3LinkedService`` object.

    Args:
        s3_spec: Linked-service definition from Azure Data Factory.

    Returns:
        S3 linked-service metadata as an ``S3LinkedService`` object.
    """
    if not s3_spec:
        return UnsupportedValue(value=s3_spec, message="Missing S3 linked service definition")

    properties = s3_spec.get("properties", {})
    return S3LinkedService(
        service_name=s3_spec.get("name", str(uuid4())),
        service_type="s3",
        access_key_id=properties.get("access_key_id"),
        service_url=properties.get("service_url"),
    )


def translate_gcs_spec(gcs_spec: dict) -> GcsLinkedService | UnsupportedValue:
    """
    Parses a Google Cloud Storage linked service definition into a ``GcsLinkedService`` object.

    Args:
        gcs_spec: Linked-service definition from Azure Data Factory.

    Returns:
        GCS linked-service metadata as a ``GcsLinkedService`` object.
    """
    if not gcs_spec:
        return UnsupportedValue(value=gcs_spec, message="Missing GCS linked service definition")

    properties = gcs_spec.get("properties", {})
    return GcsLinkedService(
        service_name=gcs_spec.get("name", str(uuid4())),
        service_type="gcs",
        access_key_id=properties.get("access_key_id"),
        service_url=properties.get("service_url"),
    )


def translate_azure_blob_spec(azure_blob_spec: dict) -> AzureBlobLinkedService | UnsupportedValue:
    """
    Parses an Azure Blob Storage linked service definition into an ``AzureBlobLinkedService`` object.

    Args:
        azure_blob_spec: Linked-service definition from Azure Data Factory.

    Returns:
        Azure Blob linked-service metadata as an ``AzureBlobLinkedService`` object.
    """
    if not azure_blob_spec:
        return UnsupportedValue(value=azure_blob_spec, message="Missing Azure Blob linked service definition")

    properties = azure_blob_spec.get("properties", {})
    connection_string = properties.get("connection_string")

    storage_account_name = properties.get("storage_account_name")
    if storage_account_name is None and connection_string is not None:
        parsed = parse_storage_account_name(connection_string)
        if not isinstance(parsed, UnsupportedValue):
            storage_account_name = parsed

    url = _get_storage_account_url(properties)
    if isinstance(url, UnsupportedValue):
        return UnsupportedValue(value=azure_blob_spec, message=url.message)

    return AzureBlobLinkedService(
        service_name=azure_blob_spec.get("name", str(uuid4())),
        service_type="azure_blob",
        url=url,
        storage_account_name=storage_account_name,
    )


def _get_storage_account_url(storage_account_properties: dict) -> str | UnsupportedValue:
    """
    Parses the storage account URL from a linked service properties dictionary.

    Args:
        storage_account_properties: Storage account properties dictionary.

    Returns:
        Storage account URL as a ``str``.
    """
    if storage_account_properties.get("authentication_type") == "Anonymous":
        container_url = storage_account_properties.get("container_url")
        if container_url is None:
            return UnsupportedValue(
                value=storage_account_properties,
                message="Missing property 'container_url' in storage account properties",
            )
        return container_url

    connection_string = storage_account_properties.get("connection_string")
    if connection_string is not None:
        return parse_storage_account_connection_string(connection_string)

    sas_uri = storage_account_properties.get("sas_uri")
    if sas_uri is not None:
        return sas_uri

    service_endpoint = storage_account_properties.get("service_endpoint")
    if service_endpoint is not None:
        warnings.warn(
            NotTranslatableWarning(
                property_name="azure_blob_linked_service",
                message="Cannot use service principal or managed identity authentication with Azure Blob linked service",
            )
        )
        return service_endpoint

    return UnsupportedValue(
        value=storage_account_properties,
        message="Missing property 'container_url', 'sas_uri', or 'service_endpoint' in storage account properties",
    )

"""Translator for Azure Blob File System (ABFS) linked service definitions.

This module normalizes ABFS linked-service payloads into ``AbfsLinkedService``
objects, validating connection strings and storage account metadata.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import AbfsLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
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

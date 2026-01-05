"""Translate Azure Blob/File Storage linked services into IR."""

from wkmigrate.linked_service_translators.parsers import (
    parse_storage_account_name,
    parse_storage_account_connection_string,
)
from wkmigrate.models.ir.linked_services import AbfsLinkedService


def translate_abfs_spec(abfs_spec: dict) -> AbfsLinkedService:
    """
    Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

    Args:
        abfs_spec: Linked-service definition from Azure Data Factory.

    Returns:
        ABFS linked-service metadata as a ``AbfsLinkedService`` object.
    """
    properties = abfs_spec.get("properties", {})
    return AbfsLinkedService(
        service_name=abfs_spec.get("name", ""),
        service_type="abfs",
        url=parse_storage_account_connection_string(properties.get("url", "")),
        storage_account_name=parse_storage_account_name(properties.get("storage_account_name", "")),
    )

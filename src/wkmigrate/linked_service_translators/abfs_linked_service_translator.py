"""This module defines methods for translating Azure SQL Server linked services from data pipeline definitions."""

from wkmigrate.linked_service_translators.parsers import (
    parse_storage_account_name, parse_storage_account_connection_string
)
from wkmigrate.utils import identity, translate


mapping = {
    "url": {"key": "connection_string", "parser": parse_storage_account_connection_string},
    "storage_account_name": {"key": "connection_string", "parser": parse_storage_account_name},
}


def translate_abfs_spec(abfs_spec: dict | None) -> dict | None:
    """Translates an Azure Blob File Storage linked service in Data Factory's object model to a set of parameters
    used to connect to the ABFS location from a Databricks workspace.
    :parameter abfs_spec: Azure SQL Server linked service definition as a ``dict``
    :return: ABFS specification as a ``dict``
    """
    if abfs_spec is None:
        return None
    # Get the cluster properties:
    properties = abfs_spec.get("properties")
    # Translate the properties:
    translated_properties = translate(properties, mapping)
    result = {"service_name": abfs_spec.get("name")}
    if translated_properties is not None:
        result.update(translated_properties)
    return result

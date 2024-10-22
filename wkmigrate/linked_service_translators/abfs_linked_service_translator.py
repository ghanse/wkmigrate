""" This module defines methods for translating Azure SQL Server linked services from data pipeline definitions."""
from typing import Optional
from wkmigrate.linked_service_translators.parsers import parse_storage_account_name
from wkmigrate.utils import identity, translate


mapping = {
    'url': {'key': 'url', 'parser': identity},
    'storage_account_name': {'key': 'url', 'parser': parse_storage_account_name}
}


def translate_abfs_spec(abfs_spec: Optional[dict]) -> Optional[dict]:
    """ Translates an Azure Blob File Storage linked service in Data Factory's object model to a set of parameters
        used to connect to the ABFS location from a Databricks workspace.
        :parameter abfs_spec: Azure SQL Server linked service definition as a ``dict``
        :return: ABFS specification as a ``dict``
    """
    if abfs_spec is None:
        return None
    # Get the cluster properties:
    properties = abfs_spec.get('properties')
    # Translate the properties:
    return {'service_name': abfs_spec.get('name'), **translate(properties, mapping)}

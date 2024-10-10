""" This module defines methods for translating SQL Server datasets from data pipeline definitions."""
from wkmigrate.utils import identity, translate
from wkmigrate.linked_service_translators.sql_server_linked_service_translator import translate_sql_server_spec


mapping = {
    'query_name': {'key': 'name', 'parser': identity},
    'connection_properties': {'key': 'linked_service_definition', 'parser': translate_sql_server_spec},
    'query_properties': {'key': 'properties', 'parser': parse_query_properties}
}


def translate_sql_server_dataset(dataset: dict) -> dict:
    """ Translates an Azure SQL Server dataset in Data Factory's object model to a set of parameters
        used to read data using the Spark JDBC connector.
        :parameter dataset: Azure SQL Server dataset definition as a ``dict``
        :return: Spark JDBC SQL Server parameters as a ``dict``
    """
    return translate(dataset, mapping)

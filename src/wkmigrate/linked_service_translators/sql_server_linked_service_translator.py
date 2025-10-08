"""This module defines methods for translating Azure SQL Server linked services from data pipeline definitions."""

from wkmigrate.utils import identity, translate


mapping = {
    "host": {"key": "server", "parser": identity},
    "database": {"key": "database", "parser": identity},
    "user_name": {"key": "user_name", "parser": identity},
    "authentication_type": {"key": "authentication_type", "parser": identity},
}


def translate_sql_server_spec(sql_server_spec: dict | None) -> dict | None:
    """Translates an Azure SQL Server linked service in Data Factory's object model to a set of parameters
    used by the Spark JDBC connector for MSSQL.
    :parameter sql_server_spec: Azure SQL Server linked service definition as a ``dict``
    :return: Spark JDBC SQL Server specification as a ``dict``
    """
    if sql_server_spec is None:
        return None
    # Get the cluster properties:
    properties = sql_server_spec.get("properties")
    # Translate the properties:
    translated_properties = translate(properties, mapping)
    result = {"service_name": sql_server_spec.get("name")}
    if translated_properties is not None:
        result.update(translated_properties)
    return result

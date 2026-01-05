"""This module defines methods for translating Azure SQL Server linked services from data pipeline definitions."""

from wkmigrate.models.ir.linked_services import SqlLinkedService


def translate_sql_server_spec(sql_server_spec: dict) -> SqlLinkedService:
    """
    Parses a SQL Server linked service definition into an ``SqlLinkedService`` object.

    Args:
        sql_server_spec: Linked-service definition from Azure Data Factory.

    Returns:
        SQL Server linked-service metadata as a ``SqlLinkedService`` object.
    """
    properties = sql_server_spec.get("properties", {})
    return SqlLinkedService(
        service_name=sql_server_spec.get("name", ""),
        service_type="sqlserver",
        host=properties.get("host", ""),
        database=properties.get("database", ""),
        user_name=properties.get("user_name", ""),
        authentication_type=properties.get("authentication_type", ""),
    )

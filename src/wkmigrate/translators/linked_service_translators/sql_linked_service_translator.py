"""Translators for SQL-based linked service definitions.

This module normalizes SQL Server, PostgreSQL, MySQL, and Oracle linked-service
payloads into ``SqlLinkedService`` objects.
"""

from uuid import uuid4

from wkmigrate.parsers.dataset_parsers import DEFAULT_PORTS
from wkmigrate.models.ir.linked_services import SqlLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import get_value_or_unsupported


def translate_sql_server_spec(sql_server_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses a SQL Server linked service definition into an ``SqlLinkedService`` object.

    Args:
        sql_server_spec: Linked-service definition from Azure Data Factory.

    Returns:
        SQL Server linked-service metadata as a ``SqlLinkedService`` object.
    """
    return _translate_sql_spec(sql_server_spec, "sqlserver", "SQL Server")


def translate_postgresql_spec(postgresql_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses an Azure Database for PostgreSQL linked service definition into an ``SqlLinkedService`` object.

    Args:
        postgresql_spec: Linked-service definition from Azure Data Factory.

    Returns:
        PostgreSQL linked-service metadata as a ``SqlLinkedService`` object.
    """
    return _translate_sql_spec(postgresql_spec, "postgresql", "PostgreSQL")


def translate_mysql_spec(mysql_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses an Azure Database for MySQL linked service definition into an ``SqlLinkedService`` object.

    Args:
        mysql_spec: Linked-service definition from Azure Data Factory.

    Returns:
        MySQL linked-service metadata as a ``SqlLinkedService`` object.
    """
    return _translate_sql_spec(mysql_spec, "mysql", "MySQL")


def translate_oracle_spec(oracle_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses an Oracle Database linked service definition into an ``SqlLinkedService`` object.

    Args:
        oracle_spec: Linked-service definition from Azure Data Factory.

    Returns:
        Oracle linked-service metadata as a ``SqlLinkedService`` object.
    """
    return _translate_sql_spec(oracle_spec, "oracle", "Oracle")


def _translate_sql_spec(spec: dict, service_type: str, display_name: str) -> SqlLinkedService | UnsupportedValue:
    """
    Builds an ``SqlLinkedService`` from a raw ADF linked service definition.

    Args:
        spec: Linked-service definition from Azure Data Factory.
        service_type: Normalised service type string (e.g. ``"sqlserver"`` or ``"postgresql"``).
        display_name: Human-readable name used in the missing-definition error message.

    Returns:
        SQL linked-service metadata as a ``SqlLinkedService`` object.
    """
    if not spec:
        return UnsupportedValue(value=spec, message=f"Missing {display_name} linked service definition")

    properties = spec.get("properties", {})
    server = get_value_or_unsupported(properties, "server", "linked service properties")
    if isinstance(server, UnsupportedValue):
        return UnsupportedValue(value=spec, message=server.message)

    database = get_value_or_unsupported(properties, "database", "linked service properties")
    if isinstance(database, UnsupportedValue):
        return UnsupportedValue(value=spec, message=database.message)

    port = properties.get("port", DEFAULT_PORTS.get(service_type))

    return SqlLinkedService(
        service_name=spec.get("name", str(uuid4())),
        service_type=service_type,
        host=server,
        database=database,
        port=port,
        user_name=properties.get("user_name"),
        password=properties.get("password"),
        authentication_type=properties.get("authentication_type"),
    )

"""Translators for SQL-based dataset definitions.

This module normalizes SQL Server, PostgreSQL, MySQL, and Oracle dataset payloads
into ``SqlTableDataset`` objects, parsing table/schema names and linked-service metadata.
"""

from collections.abc import Callable

from wkmigrate.models.ir.datasets import SqlTableDataset
from wkmigrate.models.ir.linked_services import SqlLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators.utils import get_linked_service_definition
from wkmigrate.translators.linked_service_translators import (
    translate_mysql_spec,
    translate_oracle_spec,
    translate_postgresql_spec,
    translate_sql_server_spec,
)


def translate_sql_server_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates a SQL Server dataset definition into a ``SqlTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        SQL Server dataset as a ``SqlTableDataset`` object.
    """
    schema = dataset.get("properties", {}).get("schema_type_properties_schema")
    table = dataset.get("properties", {}).get("table")
    return _translate_sql_dataset(dataset, "sqlserver", translate_sql_server_spec, table, schema)


def translate_postgresql_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates an Azure Database for PostgreSQL dataset definition into a ``SqlTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        PostgreSQL dataset as a ``SqlTableDataset`` object.
    """
    schema = dataset.get("properties", {}).get("schema_type_properties_schema")
    table = dataset.get("properties", {}).get("table")
    return _translate_sql_dataset(dataset, "postgresql", translate_postgresql_spec, table, schema)


def translate_mysql_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates an Azure Database for MySQL dataset definition into a ``SqlTableDataset`` object.

    MySQL does not use a separate schema namespace, so ``schema_name`` is always ``None``
    and ``dbtable`` contains only the bare table name.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        MySQL dataset as a ``SqlTableDataset`` object.
    """
    table = dataset.get("properties", {}).get("table")
    return _translate_sql_dataset(dataset, "mysql", translate_mysql_spec, table)


def translate_oracle_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates an Oracle Database dataset definition into a ``SqlTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Oracle dataset as a ``SqlTableDataset`` object.
    """
    schema = dataset.get("properties", {}).get("schema_type_properties_schema")
    table = dataset.get("properties", {}).get("table")
    return _translate_sql_dataset(dataset, "oracle", translate_oracle_spec, table, schema)


def _translate_sql_dataset(
    dataset: dict,
    dataset_type: str,
    translate_spec: Callable[[dict], SqlLinkedService | UnsupportedValue],
    table_name: str | None,
    schema_name: str | None = None,
) -> SqlTableDataset | UnsupportedValue:
    """
    Builds a ``SqlTableDataset`` from a raw ADF dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.
        dataset_type: Normalised dataset type string (e.g. ``"sqlserver"`` or ``"postgresql"``).
        translate_spec: Linked-service translator callable for the given database type.
        table_name: Table name, or ``None`` when missing from the ADF payload.
        schema_name: Optional schema name, or None for databases without a schema namespace (e.g. MySQL).

    Returns:
        SQL table dataset as a ``SqlTableDataset`` object.
    """
    if not table_name:
        return UnsupportedValue(
            value=dataset,
            message=f"Missing required property 'table' in {dataset_type} dataset definition",
        )

    try:
        linked_service_definition = get_linked_service_definition(dataset)
    except (ValueError, KeyError, TypeError) as exc:
        return UnsupportedValue(value=dataset, message=f"Failed to resolve linked service: {exc}")
    if isinstance(linked_service_definition, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service_definition.message)

    linked_service = translate_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dbtable = table_name if not schema_name else f"{schema_name}.{table_name}"
    return SqlTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type=dataset_type,
        schema_name=schema_name,
        table_name=table_name,
        dbtable=dbtable,
        service_name=linked_service.service_name,
        host=linked_service.host,
        database=linked_service.database,
        port=linked_service.port,
        user_name=linked_service.user_name,
        authentication_type=linked_service.authentication_type,
        connection_options={},
    )

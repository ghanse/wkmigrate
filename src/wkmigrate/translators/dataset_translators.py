"""This module defines translators for translating datasets into internal representations.

Translators in this module normalize dataset payloads into internal representations. Each
translator must validate required fields, coerce connection settings, and emit ``UnsupportedValue``
objects for any unparsable inputs.
"""

import json
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.models.ir.datasets import (
    Dataset,
    DeltaTableDataset,
    FileDataset,
    SqlTableDataset,
)
from wkmigrate.translators.linked_service_translators import (
    translate_abfs_spec,
    translate_databricks_cluster_spec,
    translate_sql_server_spec,
)

_FILE_DATASET_TYPES = {"Avro", "DelimitedText", "Json", "Orc", "Parquet"}
_SQL_DATASET_TYPES = {"AzureSqlTable"}
_DELTA_DATASET_TYPES = {"AzureDatabricksDeltaLakeDataset"}
_IGNORED_FORMAT_OPTIONS = {"dataset_name", "container", "folder_path"}


def translate_dataset(dataset: dict) -> Dataset | UnsupportedValue:
    """
    Translates a dataset definition returned by the Azure Data Factory API into a ``Dataset`` object. Supports files, SQL tables, and Delta tables. Any datasets which cannot be fully translated will return an ``UnsupportedValue`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Dataset as a ``Dataset`` object.
    """
    dataset_properties = dataset.get("properties", {})
    if not dataset_properties:
        return UnsupportedValue(value=dataset, message="Missing property 'properties' in dataset definition")

    dataset_type = dataset_properties.get("type")
    if not dataset_type:
        return UnsupportedValue(value=dataset, message="Missing property 'type' in dataset properties")

    if dataset_type in _FILE_DATASET_TYPES:
        return translate_file_dataset(dataset_type, dataset)
    if dataset_type in _SQL_DATASET_TYPES:
        return translate_sql_server_dataset(dataset)
    if dataset_type in _DELTA_DATASET_TYPES:
        return translate_delta_table_dataset(dataset)

    return UnsupportedValue(value=dataset, message=f"Unsupported dataset type '{dataset_type}'")


def translate_file_dataset(dataset_type: str, dataset: dict) -> FileDataset | UnsupportedValue:
    """
    Translates a file-based dataset definition (e.g. CSV, JSON, or Parquet) into a ``FileDataset`` object.

    Args:
        dataset_type: Type of file-based dataset (e.g. "csv", "json", or "parquet").
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        ABFS dataset as a ``FileDataset`` object.
    """
    if not dataset:
        return UnsupportedValue(value=dataset, message="Missing Avro dataset definition")

    container_name = _parse_abfs_container_name(dataset.get("properties", {}))
    if isinstance(container_name, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=container_name.message)

    folder_path = _parse_abfs_file_path(dataset.get("properties", {}))
    if isinstance(folder_path, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=folder_path.message)

    linked_service = translate_abfs_spec(_get_linked_service_definition(dataset))
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    format_options = _parse_format_options(dataset_type, dataset)
    if isinstance(format_options, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=format_options.message)

    return FileDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type=dataset_type,
        container=container_name,
        folder_path=folder_path,
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )


def translate_delta_table_dataset(dataset: dict) -> DeltaTableDataset | UnsupportedValue:
    """
    Translates a Delta table dataset definition into a ``DeltaTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Delta table dataset as a ``DeltaTableDataset`` object.
    """
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_databricks_cluster_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dataset_properties = dataset.get("properties", {})
    return DeltaTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type="delta",
        database_name=dataset_properties.get("database"),
        table_name=dataset_properties.get("table"),
        catalog_name=dataset_properties.get("catalog"),
        service_name=linked_service.service_name,
    )


def translate_sql_server_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates a SQL Server dataset definition into a ``SqlTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        SQL Server dataset as a ``SqlTableDataset`` object.
    """
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_sql_server_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dataset_properties = dataset.get("properties", {})
    return SqlTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type="sqlserver",
        schema_name=dataset_properties.get("schema_type_properties_schema"),
        table_name=dataset_properties.get("table"),
        dbtable=f'{dataset_properties.get("schema_type_properties_schema")}.{dataset_properties.get("table")}',
        service_name=linked_service.service_name,
        host=linked_service.host,
        database=linked_service.database,
        user_name=linked_service.user_name,
        authentication_type=linked_service.authentication_type,
        connection_options={},
    )


def _parse_format_options(dataset_type: str, dataset: dict) -> dict | UnsupportedValue:
    """
    Parses the format options from a dataset definition.

    Args:
        dataset_type: Type of file-based dataset (e.g. "csv", "json", or "parquet").
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """

    format_parsers = {
        "avro": _parse_avro_format_options,
        "csv": _parse_delimited_format_options,
        "json": _parse_json_format_options,
        "orc": _parse_orc_format_options,
        "parquet": _parse_parquet_format_options,
    }
    format_parser = format_parsers.get(dataset_type)
    if format_parser is None:
        return UnsupportedValue(value=dataset, message=f"No format parser found for dataset type '{dataset_type}'")

    format_options = format_parser(dataset)
    return {option_key: option_value for option_key, option_value in format_options.items() if option_value is not None}


def _parse_avro_format_options(dataset: dict) -> dict:
    """
    Parses the format options from an Avro dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    return {"compression": dataset.get("avro_compression_codec")}


def _parse_delimited_format_options(dataset: dict) -> dict:
    """
    Parses the format options from a delimited text dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    return {
        "header": dataset.get("first_row_as_header", False),
        "sep": _parse_character_value(dataset.get("column_delimiter", ",")),
        "lineSep": _parse_character_value(dataset.get("row_delimiter", "\n")),
        "quote": _parse_character_value(dataset.get("quote_char", '"')),
        "escape": _parse_character_value(dataset.get("escape_char", "\\")),
        "nullValue": _parse_character_value(dataset.get("null_value", "")),
        "compression": dataset.get("compression_codec"),
        "encoding": dataset.get("encoding_name"),
    }


def _parse_json_format_options(dataset: dict) -> dict:
    """
    Parses the format options from a JSON dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.
    """
    properties = dataset.get("properties", {})
    return {
        "encoding": properties.get("encoding_name"),
        "compression": _parse_compression_type(properties.get("compression_codec")),
    }


def _parse_orc_format_options(dataset: dict) -> dict:
    """
    Parses the format options from an ORC dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.
    """
    properties = dataset.get("properties", {})
    return {
        "compression": properties.get("orc_compression_codec"),
    }


def _parse_parquet_format_options(dataset: dict) -> dict:
    """
    Parses the format options from a Parquet dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.
    """
    properties = dataset.get("properties", {})
    return {
        "compression": properties.get("compression_codec"),
    }


def _parse_character_value(char: str) -> str:
    """
    Parses a single character into a JSON-safe representation.

    Args:
        char: Character literal extracted from the dataset definition.

    Returns:
        JSON-escaped representation of the character.
    """
    return json.dumps(char).strip('"')


def _parse_compression_type(compression: dict) -> str | None:
    """
    Parses the compression type from a format settings object.

    Args:
        compression: Compression configuration dictionary.

    Returns:
        Compression type string, if present.
    """
    return compression.get("type")


def _parse_abfs_container_name(properties: dict) -> str | UnsupportedValue:
    """
    Parses the ABFS container name from dataset properties.

    Args:
        properties: File properties block.

    Returns:
        Storage container name.
    """
    location = properties.get("location")
    if location is None:
        return UnsupportedValue(value=properties, message="Missing property 'location' in dataset properties")
    return location.get("container")


def _parse_abfs_file_path(properties: dict) -> str | UnsupportedValue:
    """
    Parses the ABFS file path from a dataset definition.

    Args:
        properties: File properties from the dataset definition.

    Returns:
        Full ABFS path to the dataset.
    """
    location = properties.get("location")
    if location is None:
        return UnsupportedValue(value=properties, message="Missing property 'location' in dataset properties")

    folder_path = location.get("folder_path")
    file_name = location.get("file_name")
    if file_name is None:
        return UnsupportedValue(value=properties, message="Missing property 'file_name' in dataset properties")

    return file_name if not folder_path else f"{folder_path}/{file_name}"


def _get_linked_service_definition(dataset: dict) -> dict:
    """
    Gets the linked service definition from a dataset definition.

    Args:
        dataset: Dataset definition from Azure Data Factory.

    Returns:
        Linked service definition as a ``dict``.

    Raises:
        ValueError: If the linked service definition is not found or is not a dictionary.
    """
    linked_service_definition = dataset.get("linked_service_definition")
    if not linked_service_definition:
        raise ValueError("Missing linked service definition")
    if not isinstance(linked_service_definition, dict):
        raise ValueError("Linked service definition must be a dictionary")
    return linked_service_definition

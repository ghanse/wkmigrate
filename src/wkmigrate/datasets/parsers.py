"""This module defines methods for parsing datasets into IR models."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
import warnings
from wkmigrate.enums.isolation_level import IsolationLevel
from wkmigrate.linked_service_translators.abfs_linked_service_translator import (
    translate_abfs_spec,
)
from wkmigrate.linked_service_translators.sql_server_linked_service_translator import (
    translate_sql_server_spec,
)
from wkmigrate.linked_service_translators.databricks_linked_service_translator import (
    translate_cluster_spec,
)
from wkmigrate.models.ir.datasets import (
    DatasetProperties,
    DeltaTableDataset,
    FileDataset,
    SqlTableDataset,
)
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.utils import identity, translate


def parse_avro_file_dataset(dataset: dict) -> FileDataset:
    """
    Parses an Avro dataset definition into a FileDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Avro dataset as a ``FileDataset`` object.

    Raises:
        ValueError: If the ABFS linked service definition is missing or not a dictionary.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "container": {"key": "properties", "parser": _parse_abfs_container_name},
        "folder_path": {"key": "properties", "parser": _parse_abfs_file_path},
        "compression_codec": {"key": "avro_compression_codec", "parser": identity},
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_abfs_spec(linked_service_definition)
    format_options = (
        {"compression": translated_dataset.get("compression_codec")}
        if translated_dataset.get("compression_codec") is not None
        else {}
    )
    return FileDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="avro",
        container=translated_dataset.get("container"),
        folder_path=translated_dataset.get("folder_path"),
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )


def parse_avro_file_properties(properties: dict) -> DatasetProperties:
    """
    Parses Avro dataset properties into a DatasetProperties object.

    Args:
        properties: Avro properties block from the dataset definition.

    Returns:
        Avro dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {
        "type": {"key": "type", "parser": _parse_dataset_type},
        "records_per_file": {
            "key": "format_settings",
            "parser": lambda x: x.get("max_rows_per_file"),
        },
        "file_path_prefix": {
            "key": "format_settings",
            "parser": lambda x: x.get("file_name_prefix"),
        },
    }
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="avro")


def parse_delimited_file_dataset(dataset: dict) -> FileDataset:
    """
    Parses a delimited-text dataset definition into a FileDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Delimited-text dataset as a ``FileDataset`` object.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "container": {"key": "properties", "parser": _parse_abfs_container_name},
        "folder_path": {"key": "properties", "parser": _parse_abfs_file_path},
        "sep": {
            "key": "properties",
            "parser": lambda x: _parse_character_value(x.get("column_delimiter")),
        },
        "lineSep": {
            "key": "properties",
            "parser": lambda x: _parse_character_value(x.get("row_delimiter")),
        },
        "header": {
            "key": "properties",
            "parser": lambda x: x.get("first_row_as_header"),
        },
        "quote": {
            "key": "properties",
            "parser": lambda x: _parse_character_value(x.get("quote_char")),
        },
        "escape": {
            "key": "properties",
            "parser": lambda x: _parse_character_value(x.get("escape_char")),
        },
        "nullValue": {
            "key": "properties",
            "parser": lambda x: _parse_character_value(x.get("null_value")),
        },
        "compression": {
            "key": "properties",
            "parser": lambda x: x.get("compression_codec"),
        },
        "encoding": {"key": "properties", "parser": lambda x: x.get("encoding_name")},
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_abfs_spec(linked_service_definition)
    if linked_service is None:
        warnings.warn(
            NotTranslatableWarning("unparsable_linked_service", "Linked service definition cannot be parsed"),
            stacklevel=3,
        )
        return None
    base_fields = {"dataset_name", "container", "folder_path"}
    format_options = {
        k: translated_dataset.get(k)
        for k in translated_dataset
        if k not in base_fields and translated_dataset.get(k) is not None
    }
    return FileDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="csv",
        container=translated_dataset.get("container"),
        folder_path=translated_dataset.get("folder_path"),
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )


def parse_delimited_file_properties(properties: dict) -> DatasetProperties:
    """
    Parses delimited-text dataset properties into a DatasetProperties object.

    Args:
        properties: Delimited-text properties block.

    Returns:
        Delimited-text dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {
        "type": {"key": "type", "parser": _parse_dataset_type},
        "quoteAll": {
            "key": "format_settings",
            "parser": lambda x: x.get("quote_all_text"),
        },
        "records_per_file": {
            "key": "format_settings",
            "parser": lambda x: x.get("max_rows_per_file"),
        },
    }
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="csv")


def parse_delta_table_dataset(dataset: dict) -> DeltaTableDataset:
    """
    Parses a Delta dataset definition into a DeltaTableDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Delta table dataset as a ``DeltaTableDataset`` object.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "database_name": {"key": "properties", "parser": lambda x: x.get("database")},
        "table_name": {"key": "properties", "parser": lambda x: x.get("table")},
        "catalog_name": {"key": "properties", "parser": lambda x: x.get("catalog")},
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_cluster_spec(linked_service_definition)
    return DeltaTableDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="delta",
        database_name=translated_dataset.get("database_name"),
        table_name=translated_dataset.get("table_name"),
        catalog_name=translated_dataset.get("catalog_name"),
        service_name=linked_service.service_name,
    )


def parse_delta_properties(properties: dict) -> DatasetProperties:
    """
    Parses Delta dataset properties into a DatasetProperties object.

    Args:
        properties: Delta properties block.

    Returns:
        Delta dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {"type": {"key": "type", "parser": _parse_dataset_type}}
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="delta")


def parse_json_file_dataset(dataset: dict) -> FileDataset:
    """
    Parses a JSON dataset definition into a FileDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        JSON dataset as a ``FileDataset`` object.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "container": {"key": "properties", "parser": _parse_abfs_container_name},
        "folder_path": {"key": "properties", "parser": _parse_abfs_file_path},
        "encoding": {"key": "properties", "parser": lambda x: x.get("encoding_name")},
        "compression": {
            "key": "properties",
            "parser": lambda x: _parse_compression_type(x.get("compression")),
        },
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_abfs_spec(linked_service_definition)
    base_fields = {"dataset_name", "container", "folder_path"}
    format_options = {
        k: translated_dataset.get(k)
        for k in translated_dataset
        if k not in base_fields and translated_dataset.get(k) is not None
    }
    return FileDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="json",
        container=translated_dataset.get("container"),
        folder_path=translated_dataset.get("folder_path"),
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )


def parse_json_file_properties(properties: dict) -> DatasetProperties:
    """
    Parses JSON dataset properties into a DatasetProperties object.

    Args:
        properties: JSON properties block.

    Returns:
        JSON dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {
        "type": {"key": "type", "parser": _parse_dataset_type},
        "records_per_file": {
            "key": "format_settings",
            "parser": lambda x: x.get("maxRowsPerFile"),
        },
    }
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="json")


def parse_orc_file_dataset(dataset: dict) -> FileDataset:
    """
    Parses an ORC dataset definition into a FileDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        ORC dataset as a ``FileDataset`` object.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "container": {"key": "properties", "parser": _parse_abfs_container_name},
        "folder_path": {"key": "properties", "parser": _parse_abfs_file_path},
        "compression": {
            "key": "properties",
            "parser": lambda x: x.get("orc_compression_codec"),
        },
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_abfs_spec(linked_service_definition)
    base_fields = {"dataset_name", "container", "folder_path"}
    format_options = {
        k: translated_dataset.get(k)
        for k in translated_dataset
        if k not in base_fields and translated_dataset.get(k) is not None
    }
    return FileDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="orc",
        container=translated_dataset.get("container"),
        folder_path=translated_dataset.get("folder_path"),
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )


def parse_orc_file_properties(properties: dict) -> DatasetProperties:
    """
    Parses ORC dataset properties into a DatasetProperties object.

    Args:
        properties: ORC properties block.

    Returns:
        ORC dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {
        "type": {"key": "type", "parser": _parse_dataset_type},
        "file_name_prefix": {
            "key": "format_settings",
            "parser": lambda x: x.get("file_name_prefix"),
        },
        "records_per_file": {
            "key": "format_settings",
            "parser": lambda x: x.get("max_rows_per_file"),
        },
    }
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="orc")


def parse_parquet_file_dataset(dataset: dict) -> FileDataset:
    """
    Parses a Parquet dataset definition into a FileDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Parquet dataset as a ``FileDataset`` object.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "container": {"key": "properties", "parser": _parse_abfs_container_name},
        "folder_path": {"key": "properties", "parser": _parse_abfs_file_path},
        "compression": {
            "key": "properties",
            "parser": lambda x: x.get("compression_codec"),
        },
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_abfs_spec(linked_service_definition)
    base_fields = {"dataset_name", "container", "folder_path"}
    format_options = {
        k: translated_dataset.get(k)
        for k in translated_dataset
        if k not in base_fields and translated_dataset.get(k) is not None
    }
    return FileDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="parquet",
        container=translated_dataset.get("container"),
        folder_path=translated_dataset.get("folder_path"),
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )


def parse_parquet_file_properties(properties: dict) -> DatasetProperties:
    """
    Parses Parquet dataset properties into a DatasetProperties object.

    Args:
        properties: Parquet properties block.

    Returns:
        Parquet dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {
        "type": {"key": "type", "parser": _parse_dataset_type},
        "file_name_prefix": {
            "key": "format_settings",
            "parser": lambda x: x.get("file_name_prefix"),
        },
        "records_per_file": {
            "key": "format_settings",
            "parser": lambda x: x.get("max_rows_per_file"),
        },
    }
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="parquet")


def parse_sql_server_dataset(dataset: dict) -> SqlTableDataset:
    """
    Parses a SQL Server dataset definition into a SqlTableDataset.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        SQL Server dataset as a ``SqlTableDataset`` object.
    """
    mapping = {
        "dataset_name": {"key": "name", "parser": identity},
        "schema_name": {
            "key": "properties",
            "parser": lambda x: x.get("schema_type_properties_schema"),
        },
        "table_name": {"key": "properties", "parser": lambda x: x.get("table")},
        "dbtable": {
            "key": "properties",
            "parser": lambda x: f"{x.get('schema_type_properties_schema')}.{x.get('table')}",
        },
    }
    translated_dataset = translate(dataset, mapping) or {}
    linked_service_definition = _get_linked_service_definition(dataset)
    linked_service = translate_sql_server_spec(linked_service_definition)
    return SqlTableDataset(
        dataset_name=translated_dataset.get("dataset_name", dataset.get("name", "")),
        dataset_type="sqlserver",
        schema_name=translated_dataset.get("schema_name"),
        table_name=translated_dataset.get("table_name"),
        dbtable=translated_dataset.get("dbtable"),
        service_name=linked_service.service_name,
        host=linked_service.host,
        database=linked_service.database,
        user_name=linked_service.user_name,
        authentication_type=linked_service.authentication_type,
        connection_options={},
    )


def parse_sql_server_properties(properties: dict) -> DatasetProperties:
    """
    Parses SQL Server dataset properties into a DatasetProperties object.

    Args:
        properties: SQL Server properties block.

    Returns:
        SQL Server dataset properties as a ``DatasetProperties`` object.
    """
    mapping = {
        "type": {"key": "type", "parser": _parse_dataset_type},
        "query_isolation_level": {
            "key": "isolation_level",
            "parser": _parse_query_isolation_level,
        },
        "query_timeout_seconds": {
            "key": "query_timeout",
            "parser": _parse_query_timeout_seconds,
        },
        "numPartitions": {"key": "max_concurrent_connections", "parser": identity},
        "batchsize": {"key": "write_batch_size", "parser": identity},
        "sessionInitStatement": {"key": "pre_copy_script", "parser": identity},
        "mode": {"key": "write_behavior", "parser": _parse_sql_write_behavior},
    }
    translated = translate(properties, mapping) or {}
    return _build_dataset_properties(translated, default_type="sqlserver")


def _parse_sql_write_behavior(write_behavior: str) -> str:
    """
    Parses an ADF write behavior into a Spark output mode.

    Args:
        write_behavior: ADF write behavior expression.

    Returns:
        Normalized Spark output mode.

    Raises:
        NotTranslatableWarning: If the behavior cannot be translated.
    """
    if write_behavior == "insert":
        return "append"
    raise NotTranslatableWarning(
        "write_behavior",
        f"Cannot create an equivalent Copy Data task for writing to SQL Server in '{write_behavior}' mode",
    )


def _parse_character_value(char: str) -> str:
    """
    Parses a single character into a JSON-safe representation.

    Args:
        char: Character literal extracted from the dataset definition.

    Returns:
        JSON-escaped representation of the character.
    """
    return json.dumps(char).strip('"')


def _parse_dataset_type(dataset_type: str) -> str:
    """
    Parses an ADF dataset type into a Spark data format.

    Args:
        dataset_type: Dataset type string from the ADF definition.

    Returns:
        Spark data format string (e.g., ``csv``, ``json``).

    Raises:
        NotTranslatableWarning: If the dataset type is unsupported.
    """
    mappings = {
        "AvroSource": "avro",
        "AvroSink": "avro",
        "AzureDatabricksDeltaLakeSource": "delta",
        "AzureDatabricksDeltaLakeSink": "delta",
        "AzureSqlSource": "sqlserver",
        "AzureSqlSink": "sqlserver",
        "DelimitedTextSource": "csv",
        "DelimitedTextSink": "csv",
        "JsonSource": "json",
        "JsonSink": "json",
        "OrcSource": "orc",
        "OrcSink": "orc",
        "ParquetSource": "parquet",
        "ParquetSink": "parquet",
    }
    result = mappings.get(dataset_type)
    if result is None:
        raise NotTranslatableWarning("dataset_type", f"Unsupported dataset type: {dataset_type}")
    return result


def _parse_compression_type(compression: dict) -> str | None:
    """
    Parses the compression type from a format settings object.

    Args:
        compression: Compression configuration dictionary.

    Returns:
        Compression type string, if present.
    """
    return compression.get("type")


def _parse_query_timeout_seconds(properties: dict | None) -> int:
    """
    Parses the query timeout from dataset properties.

    Args:
        properties: Optional properties dictionary.

    Returns:
        Timeout in seconds.
    """
    if properties is None or "query_timeout" not in properties:
        return 0
    query_timeout = properties.get("query_timeout")
    if query_timeout is None:
        return 0
    return _parse_query_timeout_string(query_timeout)


def _parse_query_isolation_level(properties: dict | None) -> str | None:
    """
    Parses the isolation level from dataset properties.

    Args:
        properties: Optional properties dictionary.

    Returns:
        Query isolation level name.
    """
    if properties is None or "isolation_level" not in properties:
        return "READ_COMMITTED"
    isolation_level = properties.get("isolation_level")
    if isolation_level is None:
        return "READ_COMMITTED"
    return IsolationLevel(isolation_level).name


def _parse_query_timeout_string(timeout_string: str) -> int:
    """
    Parses an ``hh:mm:ss`` string into seconds.

    Args:
        timeout_string: Timeout string in ``HH:MM:SS`` format.

    Returns:
        Integer number of seconds represented by the string.
    """
    time_format = "%H:%M:%S"
    date_time = datetime.strptime(timeout_string, time_format)
    time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
    return int(time_delta.total_seconds())


def _parse_abfs_container_name(properties: dict) -> str:
    """
    Parses the ABFS container name from dataset properties.

    Args:
        properties: File properties block.

    Returns:
        Storage container name.

    Raises:
        NotTranslatableWarning: If the container name cannot be parsed.
    """
    location = properties.get("location")
    if location is None:
        raise NotTranslatableWarning("storage_container_location", "Storage container location cannot be None")
    return location.get("container")


def _parse_abfs_file_path(properties: dict) -> str:
    """
    Parses the ABFS file path from a dataset definition.

    Args:
        properties: File properties from the dataset definition.

    Returns:
        Full ABFS path to the dataset.

    Raises:
        NotTranslatableWarning: If the file path cannot be parsed.
    """
    location = properties.get("location")
    if location is None:
        raise NotTranslatableWarning("storage_container_location", "Storage container location cannot be None")
    folder_path = location.get("folder_path")
    file_name = location.get("file_name")
    return file_name if not folder_path else f"{folder_path}/{file_name}"


def _build_dataset_properties(translated: dict, default_type: str) -> DatasetProperties:
    """
    Constructs a DatasetProperties object from translated values.

    Args:
        translated: Translated property values.
        default_type: Dataset type to apply when ``translated`` omits ``type``.

    Returns:
        Dataset properties as a ``DatasetProperties`` object.

    Raises:
        NotTranslatableWarning: If the dataset type cannot be parsed.
    """
    dataset_type = translated.pop("type", default_type)
    if dataset_type is None:
        raise NotTranslatableWarning("dataset_type", "Dataset property type cannot be None")
    return DatasetProperties(dataset_type=dataset_type, options=translated)


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
    if linked_service_definition is None:
        raise ValueError("Linked service definition not found")
    if not isinstance(linked_service_definition, dict):
        raise ValueError("Linked service definition must be a dictionary")
    return linked_service_definition

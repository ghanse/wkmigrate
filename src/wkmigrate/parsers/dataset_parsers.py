"""This module defines dataset constants, type mappings, parsers, and shared helpers for working with datasets.

Dataset constants define the ADF dataset types that the library can translate, the secret keys
required per dataset type, and the Spark options emitted for each format.  Type-mapping helpers
normalize source-system column types into Spark equivalents.  Shared helpers convert ``Dataset``
and ``DatasetProperties`` IR objects into flat dictionaries and collect the ``SecretInstruction``
objects needed to materialise credentials in a Databricks workspace.  Parsers normalize raw ADF
dataset definitions into structured format options and type-specific metadata.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import asdict, is_dataclass
from typing import Any

from wkmigrate.enums.isolation_level import IsolationLevel
from wkmigrate.models.ir.datasets import Dataset, DatasetProperties
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.models.workflows.instructions import SecretInstruction
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.utils import parse_mapping, parse_timeout_string


_JDBC_SECRETS = ["user_name", "password"]
_JDBC_OPTIONS = ["dbtable", "numPartitions", "batchsize", "sessionInitStatement"]
DEFAULT_PORTS: dict[str, int] = {
    "sqlserver": 1433,
    "postgresql": 5432,
    "mysql": 3306,
    "oracle": 1521,
}
CLOUD_LOCATION_TYPES: dict[str, str] = {
    "AmazonS3Location": "s3",
    "GoogleCloudStorageLocation": "gcs",
    "AzureBlobStorageLocation": "azure_blob",
    "AzureBlobFSLocation": "abfs",
}
DEFAULT_CREDENTIALS_SCOPE = "wkmigrate_credentials_scope"
DATASET_PROVIDER_SECRETS: dict[str, list[str]] = {
    "abfs": ["storage_account_key"],
    "azure_blob": ["storage_account_key"],
    "delta": [],
    "gcs": ["access_key_id", "secret_access_key"],
    "mysql": _JDBC_SECRETS,
    "oracle": _JDBC_SECRETS,
    "postgresql": _JDBC_SECRETS,
    "s3": ["access_key_id", "secret_access_key"],
    "sqlserver": _JDBC_SECRETS,
}
DATASET_OPTIONS: dict[str, list[str]] = {
    "csv": [
        "header",
        "sep",
        "lineSep",
        "quote",
        "quoteAll",
        "escape",
        "nullValue",
        "compression",
        "encoding",
    ],
    "json": ["encoding", "compression"],
    "mysql": _JDBC_OPTIONS,
    "oracle": _JDBC_OPTIONS,
    "orc": ["compression"],
    "parquet": ["compression"],
    "postgresql": _JDBC_OPTIONS,
    "sqlserver": _JDBC_OPTIONS,
}

_sql_server_type_mapping: dict[str, str] = {
    "Boolean": "boolean",
    "Byte": "tinyint",
    "Int16": "short",
    "Int32": "int",
    "Int64": "long",
    "Single": "float",
    "Double": "double",
    "Decimal": "decimal(38, 38)",
    "String": "string",
    "DateTime": "timestamp",
    "DateTimeOffset": "timestamp",
    "Guid": "string",
    "Byte[]": "binary",
    "TimeSpan": "string",
}
_postgresql_type_mapping: dict[str, str] = {
    "smallint": "short",
    "integer": "int",
    "bigint": "long",
    "real": "float",
    "float": "float",
    "double precision": "double",
    "numeric": "decimal(38, 38)",
    "decimal": "decimal(38, 38)",
    "boolean": "boolean",
    "character varying": "string",
    "varchar": "string",
    "text": "string",
    "char": "string",
    "character": "string",
    "date": "date",
    "timestamp without time zone": "timestamp_ntz",
    "timestamp with time zone": "timestamp",
    "timestamp": "timestamp",
    "time without time zone": "timestamp_ntz",
    "time with time zone": "timestamp_ntz",
    "time": "string",
    "interval": "string",
    "enum": "string",
    "money": "string",
    "inet": "string",
    "cidr": "string",
    "macaddr": "string",
    "macaddr8": "string",
    "point": "string",
    "line": "string",
    "lseg": "string",
    "box": "string",
    "path": "string",
    "polygon": "string",
    "circle": "string",
    "pg_lsn": "string",
    "bytea": "binary",
    "bit": "boolean",
    "bit varying": "binary",
    "tsvector": "string",
    "tsquery": "string",
    "uuid": "string",
    "xml": "string",
    "json": "string",
    "jsonb": "string",
    "int4range": "string",
    "int8range": "string",
    "numrange": "string",
    "tsrange": "string",
    "tstzrange": "string",
    "daterange": "string",
    "oid": "decimal(20, 0)",
    "regxxx": "string",
    "void": "void",
}
_mysql_type_mapping: dict[str, str] = {
    "bit": "boolean",
    "tinyint": "boolean",
    "smallint": "short",
    "mediumint": "int",
    "int": "int",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "decimal": "decimal(38, 18)",
    "char": "string",
    "varchar": "string",
    "text": "string",
    "tinytext": "string",
    "mediumtext": "string",
    "longtext": "string",
    "date": "date",
    "datetime": "timestamp",
    "timestamp": "timestamp",
    "blob": "binary",
    "tinyblob": "binary",
    "mediumblob": "binary",
    "longblob": "binary",
    "json": "string",
}
_oracle_type_mapping: dict[str, str] = {
    "NUMBER": "decimal(38, 38)",
    "FLOAT": "double",
    "BINARY_FLOAT": "float",
    "BINARY_DOUBLE": "double",
    "VARCHAR2": "string",
    "NVARCHAR2": "string",
    "CHAR": "string",
    "NCHAR": "string",
    "CLOB": "string",
    "NCLOB": "string",
    "DATE": "timestamp",
    "TIMESTAMP": "timestamp",
    "RAW": "binary",
    "BLOB": "binary",
    "LONG": "binary",
}
_JDBC_TYPE_MAPPINGS: dict[str, dict[str, str]] = {
    "sqlserver": _sql_server_type_mapping,
    "postgresql": _postgresql_type_mapping,
    "mysql": _mysql_type_mapping,
    "oracle": _oracle_type_mapping,
}


def parse_spark_data_type(sink_type: str, sink_system: str) -> str:
    """
    Converts a source-system data type to the Spark equivalent.

    Args:
        sink_type: Data type string defined in the source system.
        sink_system: Identifier for the source system (for example ``"sqlserver"``).

    Returns:
        Spark-compatible data type string.
    """
    if sink_system == "delta":
        return sink_type
    mapping = _JDBC_TYPE_MAPPINGS.get(sink_system)
    if mapping is None:
        warnings.warn(
            NotTranslatableWarning(
                "sink_type",
                f"No data type mapping available for target system '{sink_system}'; "
                f"using ADF type '{sink_type}' as-is.",
            ),
            stacklevel=2,
        )
        return sink_type
    mapped = mapping.get(sink_type)
    if mapped is None:
        warnings.warn(
            NotTranslatableWarning(
                "sink_type",
                f"No data type mapping for '{sink_system}' type '{sink_type}'; " f"using ADF type '{sink_type}' as-is.",
            ),
            stacklevel=2,
        )
        return sink_type
    return mapped


def merge_dataset_definition(dataset: Dataset | dict | None, properties: DatasetProperties | dict | None) -> dict:
    """
    Merges a ``Dataset`` IR object and its associated properties into a single flat dictionary.

    Args:
        dataset: Parsed dataset or pre-built dictionary.
        properties: Parsed dataset properties or pre-built dictionary.

    Returns:
        Flat dictionary combining all dataset and property fields.
    """
    if dataset is None or properties is None:
        raise ValueError("Dataset definition or properties missing")
    dataset_dict = dataset_to_dict(dataset)
    properties_dict = dataset_properties_to_dict(properties)
    return {**dataset_dict, **properties_dict}


def dataset_to_dict(dataset: Dataset | dict) -> dict:
    """
    Converts a ``Dataset`` IR object into a dictionary.

    Args:
        dataset: Parsed dataset or pre-built dictionary.

    Returns:
        Dictionary representation of the dataset.
    """
    if isinstance(dataset, dict):
        return dataset
    if is_dataclass(dataset):
        dataset_dict = asdict(dataset)
        dataset_type_value = dataset_dict.pop("dataset_type", None)
        if dataset_type_value is not None:
            dataset_dict["type"] = dataset_type_value
        format_options = dataset_dict.pop("format_options", None)
        if isinstance(format_options, dict):
            dataset_dict.update(parse_mapping(format_options))
        connection_options = dataset_dict.pop("connection_options", None)
        if isinstance(connection_options, dict):
            dataset_dict.update(parse_mapping(connection_options))
        return parse_mapping(dataset_dict)
    return {}


def dataset_properties_to_dict(properties: DatasetProperties | dict | None) -> dict:
    """
    Converts ``DatasetProperties`` into a dictionary.

    Args:
        properties: Parsed dataset properties object or pre-built dictionary.

    Returns:
        Flat dictionary representation of the dataset properties with ``None`` values removed.
    """
    if properties is None:
        return {}
    if isinstance(properties, dict):
        return properties
    values: dict[str, Any] = {"type": properties.dataset_type}
    values.update(parse_mapping(properties.options))
    return values


def collect_data_source_secrets(definition: dict) -> list[SecretInstruction]:
    """
    Builds the list of ``SecretInstruction`` objects required for a dataset definition.

    Each provider type declares a set of secret keys in ``DATASET_PROVIDER_SECRETS``.
    This helper creates one ``SecretInstruction`` per declared key, stamped with the
    service name and type so the workspace deployer can materialise the secrets.

    File datasets resolve secrets by ``provider_type`` (e.g. ``"abfs"``, ``"s3"``).
    SQL datasets resolve secrets by ``service_type`` (e.g. ``"sqlserver"``).

    Args:
        definition: Flat dataset definition dictionary produced by ``merge_dataset_definition``.

    Returns:
        List of ``SecretInstruction`` objects. The list is empty when the dataset
        type or service name is missing, or when the type has no required secrets.
    """
    service_type = definition.get("type")
    service_name = definition.get("service_name")
    if service_type is None or service_name is None:
        return []

    provider_type = definition.get("provider_type")
    if provider_type is not None:
        # File dataset: look up by provider
        secret_keys = DATASET_PROVIDER_SECRETS.get(provider_type, [])
        lookup_type = provider_type
    else:
        # SQL or other dataset: look up by service type
        secret_keys = DATASET_PROVIDER_SECRETS.get(service_type, [])
        lookup_type = service_type

    return [
        SecretInstruction(
            scope=DEFAULT_CREDENTIALS_SCOPE,
            key=f"{service_name}_{secret}",
            service_name=service_name,
            service_type=lookup_type,
            provided_value=definition.get(secret),
        )
        for secret in secret_keys
    ]


_SQL_DATASET_TYPES = frozenset({"sqlserver", "postgresql", "mysql", "oracle"})


def parse_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses the format options from a dataset definition. Parsing format options for dataset types which are not
    supported will return an ``UnsupportedValue`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    dataset_type = _parse_dataset_type(dataset.get("type", ""))
    if isinstance(dataset_type, UnsupportedValue):
        return dataset_type
    if dataset_type is None:
        return UnsupportedValue(value=dataset, message="Missing property 'type' in dataset definition")

    match dataset_type:
        case "avro":
            return _parse_avro_format_options(dataset)
        case "csv":
            return _parse_delimited_format_options(dataset)
        case "delta":
            return _parse_delta_format_options()
        case "json":
            return _parse_json_format_options(dataset)
        case "orc":
            return _parse_orc_format_options(dataset)
        case "parquet":
            return _parse_parquet_format_options(dataset)
        case sql_type if sql_type in _SQL_DATASET_TYPES:
            return _parse_sql_format_options(dataset, sql_type)
        case _:
            return UnsupportedValue(value=dataset, message=f"Unsupported dataset type '{dataset_type}'")


def _parse_avro_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses the format options from an Avro dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    format_settings = dataset.get("format_settings", {})
    return {
        "type": "avro",
        "compression": dataset.get("compression_codec"),
        "records_per_file": format_settings.get("max_rows_per_file"),
        "file_path_prefix": format_settings.get("file_name_prefix"),
    }


def _parse_delta_format_options() -> dict:
    """
    Parses format options from a Delta dataset definition.

    Returns:
        Delta dataset properties as a dictionary of format options.
    """
    return {
        "type": "delta",
    }


def _parse_delimited_format_options(dataset: dict) -> dict:
    """
    Parses the format options from a delimited text dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    return {
        "type": "csv",
        "quoteAll": dataset.get("quote_all_text"),
        "extension": dataset.get("file_extension"),
        "records_per_file": dataset.get("max_rows_per_file"),
        "file_path_prefix": dataset.get("file_name_prefix"),
    }


def _parse_json_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses format options from a JSON dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        JSON dataset properties as a dictionary of format options.
    """
    format_settings = dataset.get("format_settings", {})
    return {
        "type": "json",
        "records_per_file": format_settings.get("max_rows_per_file"),
    }


def _parse_orc_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses format options from an ORC dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        ORC dataset properties as a dictionary of format options.
    """
    format_settings = dataset.get("format_settings", {})
    return {
        "type": "orc",
        "file_name_prefix": format_settings.get("file_name_prefix"),
        "records_per_file": format_settings.get("max_rows_per_file"),
    }


def _parse_parquet_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses format options from a Parquet dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Parquet dataset properties as a dictionary of format options.
    """
    format_settings = dataset.get("format_settings", {})
    return {
        "type": "parquet",
        "file_name_prefix": format_settings.get("file_name_prefix"),
        "records_per_file": format_settings.get("max_rows_per_file"),
    }


def _parse_sql_format_options(dataset: dict, dataset_type: str) -> dict | UnsupportedValue:
    """
    Parses format options from a SQL (SQL Server, PostgreSQL, MySQL, or Oracle) dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.
        dataset_type: Normalised dataset type string (e.g. ``"sqlserver"`` or ``"postgresql"``).

    Returns:
        SQL dataset properties as a dictionary of format options.
    """
    format_settings = dataset.get("format_settings", {})
    return {
        "type": dataset_type,
        "query_isolation_level": _parse_query_isolation_level(format_settings.get("query_isolation_level")),
        "query_timeout_seconds": _parse_query_timeout_seconds(format_settings.get("query_timeout_seconds")),
        "numPartitions": format_settings.get("numPartitions"),
        "batchsize": format_settings.get("batchsize"),
        "sessionInitStatement": format_settings.get("sessionInitStatement"),
        "mode": _parse_sql_write_behavior(format_settings.get("mode")),
    }


def _parse_dataset_type(dataset_type: str) -> str | UnsupportedValue:
    """
    Parses a dataset type from an Azure Data Factory source or sink dataset (e.g. 'DeltaSource') into a Spark data
    source or sink format (e.g. 'delta'). Any datasets which are not supported will return an ``UnsupportedValue``
    object.

    Args:
        dataset_type: Dataset type string from the Azure Data Factory source or sink dataset.

    Returns:
        Spark data source or sink format string (e.g., ``csv``, ``json``).
    """
    mappings = {
        "AvroSource": "avro",
        "AvroSink": "avro",
        "AzureDatabricksDeltaLakeSource": "delta",
        "AzureDatabricksDeltaLakeSink": "delta",
        "AzureMySqlSource": "mysql",
        "AzureMySqlSink": "mysql",
        "AzurePostgreSqlSource": "postgresql",
        "AzurePostgreSqlSink": "postgresql",
        "AzureSqlSource": "sqlserver",
        "AzureSqlSink": "sqlserver",
        "DelimitedTextSource": "csv",
        "DelimitedTextSink": "csv",
        "JsonSource": "json",
        "JsonSink": "json",
        "OracleSource": "oracle",
        "OracleSink": "oracle",
        "OrcSource": "orc",
        "OrcSink": "orc",
        "ParquetSource": "parquet",
        "ParquetSink": "parquet",
    }
    result = mappings.get(dataset_type)
    if result is None:
        return UnsupportedValue(value=dataset_type, message=f"Unsupported dataset type '{dataset_type}'")
    return result


def _parse_sql_write_behavior(write_mode: str) -> str | UnsupportedValue:
    """
    Parses an ADF write mode into a Spark output mode. Any SQL write modes which are not translatable will return an
    ``UnsupportedValue`` object.

    Args:
        write_mode: ADF write mode expression.

    Returns:
        Normalized Spark output mode.
    """
    if write_mode is None:
        return None
    if write_mode == "insert":
        return "append"
    return UnsupportedValue(value=write_mode, message=f"Unsupported SQL write mode '{write_mode}'")


def _parse_query_timeout_seconds(properties: dict | None) -> int | UnsupportedValue:
    """
    Parses the query timeout from dataset properties. Any query timeout strings which are not translatable into an
    integer number of seconds will return an ``UnsupportedValue`` object.

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
    return parse_timeout_string(query_timeout)


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


def _parse_character_value(char: str) -> str:
    """
    Parses a single character into a JSON-safe representation.

    Args:
        char: Character literal extracted from the dataset definition.

    Returns:
        JSON-escaped representation of the character.
    """
    return json.dumps(char).strip('"')

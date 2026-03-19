"""This module defines dataset constants, type mappings, and shared helpers for working with datasets.

Dataset constants define the ADF dataset types that the library can translate, the secret keys
required per dataset type, and the Spark options emitted for each format.  Type-mapping helpers
normalize source-system column types into Spark equivalents.  Shared helpers convert ``Dataset``
and ``DatasetProperties`` IR objects into flat dictionaries and collect the ``SecretInstruction``
objects needed to materialise credentials in a Databricks workspace.
"""

from __future__ import annotations

import warnings
from dataclasses import asdict, is_dataclass
from typing import Any

from wkmigrate.models.ir.datasets import Dataset, DatasetProperties
from wkmigrate.models.workflows.instructions import SecretInstruction
from wkmigrate.translation_warnings import TranslationWarning
from wkmigrate.utils import parse_mapping


JDBC_SECRETS = ["user_name", "password"]
JDBC_OPTIONS = ["dbtable", "numPartitions", "batchsize", "sessionInitStatement"]
DEFAULT_PORTS: dict[str, int] = {
    "sqlserver": 1433,
    "postgresql": 5432,
    "mysql": 3306,
    "oracle": 1521,
}
FILE_DATASET_TYPES = {"Avro", "DelimitedText", "Json", "Orc", "Parquet"}
SQL_DATASET_TYPES = {"AzureSqlTable"}
DELTA_DATASET_TYPES = {"AzureDatabricksDeltaLakeDataset"}
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
            TranslationWarning(
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
            TranslationWarning(
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

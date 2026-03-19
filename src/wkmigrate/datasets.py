"""This module defines dataset constants, type mappings, and shared helpers for working with datasets.

Dataset constants define the ADF dataset types that the library can translate, the secret keys
required per dataset type, and the Spark options emitted for each format.  Type-mapping helpers
normalize source-system column types into Spark equivalents.  Shared helpers convert ``Dataset``
and ``DatasetProperties`` IR objects into flat dictionaries and collect the ``SecretInstruction``
objects needed to materialise credentials in a Databricks workspace.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from wkmigrate.models.ir.datasets import Dataset, DatasetProperties
from wkmigrate.models.workflows.instructions import SecretInstruction
from wkmigrate.utils import parse_mapping

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
    "s3": ["access_key_id", "secret_access_key"],
    "sqlserver": ["host", "database", "user_name", "password"],
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
    "orc": ["compression"],
    "parquet": ["compression"],
    "sqlserver": ["mode", "dbtable", "numPartitions", "batchsize", "sessionInitStatement"],
}

sql_server_type_mapping: dict[str, str] = {
    "Boolean": "boolean",
    "Int16": "short",
    "Int32": "int",
    "Int64": "long",
    "Single": "float",
    "Double": "double",
    "Decimal": "decimal(38, 38)",
}


def parse_spark_data_type(sink_type: str, sink_system: str) -> str:
    """
    Converts a source-system data type to the Spark equivalent.

    Args:
        sink_type: Data type string defined in the source system.
        sink_system: Identifier for the source system (for example ``"sqlserver"``).

    Returns:
        Spark-compatible data type string.

    Raises:
        ValueError: If the sink system is unsupported.
        ValueError: If the sink type is not supported for the given system.
    """
    if sink_system == "delta":
        return sink_type
    if sink_system == "sqlserver":
        mapped_type = sql_server_type_mapping.get(sink_type)
        if mapped_type is None:
            raise ValueError(f"No data type mapping available for SQL Server type '{sink_type}'")
        return mapped_type
    raise ValueError(f"No data type mapping available for target system '{sink_system}'")


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

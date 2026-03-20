"""Top-level dataset translator that dispatches to type-specific translators.

This module normalizes dataset payloads into internal representations. Each
translator validates required fields, coerces connection settings, and emits
``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.parsers.dataset_parsers import CLOUD_LOCATION_TYPES
from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators.delta_table_dataset_translator import translate_delta_table_dataset
from wkmigrate.translators.dataset_translators.file_dataset_translator import translate_file_dataset
from wkmigrate.translators.dataset_translators.sql_dataset_translator import (
    translate_mysql_dataset,
    translate_oracle_dataset,
    translate_postgresql_dataset,
    translate_sql_server_dataset,
)

FILE_DATASET_TYPES = {"Avro", "DelimitedText", "Json", "Orc", "Parquet"}


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

    if dataset_type in FILE_DATASET_TYPES:
        # Determine provider from location type; translate_file_dataset handles dispatch.
        location = dataset_properties.get("location", {})
        location_type = location.get("type")
        provider_type = CLOUD_LOCATION_TYPES.get(location_type) if location_type else None
        if provider_type is None:
            return UnsupportedValue(
                value=dataset,
                message=f"Unsupported file location type '{location_type}'",
            )
        return translate_file_dataset(dataset_type, dataset, provider_type)

    match dataset_type:
        case "AzureSqlTable":
            return translate_sql_server_dataset(dataset)
        case "AzurePostgreSqlTable":
            return translate_postgresql_dataset(dataset)
        case "AzureMySqlTable":
            return translate_mysql_dataset(dataset)
        case "OracleTable":
            return translate_oracle_dataset(dataset)
        case "AzureDatabricksDeltaLakeDataset":
            return translate_delta_table_dataset(dataset)
        case _:
            return UnsupportedValue(value=dataset, message=f"Unsupported dataset type '{dataset_type}'")

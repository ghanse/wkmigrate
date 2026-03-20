"""Shared helpers for dataset translators.

These utilities are used by multiple dataset translators to parse linked-service
definitions, format options, and ABFS paths from ADF dataset payloads.
"""

import json

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import get_value_or_unsupported


def get_linked_service_definition(dataset: dict) -> dict | UnsupportedValue:
    """
    Gets the linked service definition from a dataset definition.

    Args:
        dataset: Dataset definition from Azure Data Factory.

    Returns:
        Linked service definition as a ``dict`` or an ``UnsupportedValue``.
    """
    linked_service_definition = get_value_or_unsupported(dataset, "linked_service_definition", "dataset")
    if not isinstance(linked_service_definition, dict):
        if isinstance(linked_service_definition, UnsupportedValue):
            return linked_service_definition
        return UnsupportedValue(dataset, "Invalid value for 'linked_service_definition'")
    return linked_service_definition


def parse_format_options(dataset_type: str, dataset: dict) -> dict | UnsupportedValue:
    """
    Parses the format options from a dataset definition.

    Args:
        dataset_type: Type of file-based dataset (e.g. "csv", "json", or "parquet").
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """

    format_parsers = {
        "Avro": _parse_avro_format_options,
        "avro": _parse_avro_format_options,
        "DelimitedText": _parse_delimited_format_options,
        "csv": _parse_delimited_format_options,
        "Json": _parse_json_format_options,
        "json": _parse_json_format_options,
        "Orc": _parse_orc_format_options,
        "orc": _parse_orc_format_options,
        "Parquet": _parse_parquet_format_options,
        "parquet": _parse_parquet_format_options,
    }
    format_parser = format_parsers.get(dataset_type)
    if format_parser is None:
        return UnsupportedValue(value=dataset, message=f"No format parser found for dataset type '{dataset_type}'")

    format_options = format_parser(dataset)
    return {option_key: option_value for option_key, option_value in format_options.items() if option_value is not None}


def parse_abfs_container_name(properties: dict) -> str | UnsupportedValue:
    """
    Parses the ABFS container name from dataset properties.

    Args:
        properties: File properties block.

    Returns:
        Storage container name.
    """
    location = get_value_or_unsupported(properties, "location", "dataset properties")
    if isinstance(location, UnsupportedValue):
        return location
    result = location.get("container") or location.get("file_system")
    if result is None:
        return UnsupportedValue(
            value=properties, message="Missing property 'container' or 'file_system' in dataset location"
        )
    return result


def parse_cloud_bucket_name(properties: dict) -> str | UnsupportedValue:
    """
    Parses the cloud storage bucket/container name from dataset properties.

    Checks ``bucket_name``, ``container``, and ``file_system`` keys in the
    dataset location block.

    Args:
        properties: File properties block.

    Returns:
        Bucket or container name.
    """
    location = get_value_or_unsupported(properties, "location", "dataset properties")
    if isinstance(location, UnsupportedValue):
        return location
    result = location.get("bucket_name") or location.get("container") or location.get("file_system")
    if result is None:
        return UnsupportedValue(
            value=properties,
            message="Missing property 'bucket_name', 'container', or 'file_system' in dataset location",
        )
    return result


def parse_cloud_file_path(properties: dict) -> str | UnsupportedValue:
    """
    Parses the file path from a cloud dataset definition.

    Args:
        properties: File properties from the dataset definition.

    Returns:
        Full path to the dataset file.
    """
    location = properties.get("location")
    if location is None:
        return UnsupportedValue(value=properties, message="Missing property 'location' in dataset properties")

    folder_path = location.get("folder_path")
    file_name = location.get("file_name")
    if file_name is None:
        return UnsupportedValue(value=properties, message="Missing property 'file_name' in dataset properties")

    return file_name if not folder_path else f"{folder_path}/{file_name}"


def parse_abfs_file_path(properties: dict) -> str | UnsupportedValue:
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


def _parse_avro_format_options(dataset: dict) -> dict:
    """
    Parses the format options from an Avro dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    properties = dataset.get("properties", {})
    return {"compression": properties.get("avro_compression_codec")}


def _parse_delimited_format_options(dataset: dict) -> dict:
    """
    Parses the format options from a delimited text dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    properties = dataset.get("properties", {})
    return {
        "header": properties.get("first_row_as_header", False),
        "sep": _parse_character_value(properties.get("column_delimiter", ",")),
        "lineSep": _parse_character_value(properties.get("row_delimiter", "\n")),
        "quote": _parse_character_value(properties.get("quote_char", '"')),
        "escape": _parse_character_value(properties.get("escape_char", "\\")),
        "nullValue": _parse_character_value(properties.get("null_value", "")),
        "compression": properties.get("compression_codec"),
        "encoding": properties.get("encoding_name"),
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


def _parse_compression_type(compression: dict | None) -> str | None:
    """
    Parses the compression type from a format settings object.

    Args:
        compression: Compression configuration dictionary, or ``None`` when no compression is specified.

    Returns:
        Compression type string, if present.
    """
    if compression is None:
        return None
    return compression.get("type")

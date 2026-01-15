"""This module defines parsers for dataset properties shared across translators.

Parsers in this module normalize raw ADF dataset definitions into structured format
options and type-specific metadata. Parsers should emit ``UnsupportedValue`` objects
for any unparsable inputs.
"""

from datetime import datetime, timedelta
import json
from wkmigrate.enums.isolation_level import IsolationLevel
from wkmigrate.models.ir.unsupported import UnsupportedValue


def parse_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses the format options from a dataset definition. Parsing format options for dataset types which are not supported will return an ``UnsupportedValue`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    dataset_type = _parse_dataset_type(dataset.get("type", ""))
    if dataset_type is None:
        return UnsupportedValue(value=dataset, message="Missing property 'type' in dataset definition")
    if dataset_type == "avro":
        return _parse_avro_format_options(dataset)
    if dataset_type == "csv":
        return _parse_delimited_format_options(dataset)
    if dataset_type == "delta":
        return _parse_delta_format_options()
    if dataset_type == "json":
        return _parse_json_format_options(dataset)
    if dataset_type == "orc":
        return _parse_orc_format_options(dataset)
    if dataset_type == "parquet":
        return _parse_parquet_format_options(dataset)
    if dataset_type == "sqlserver":
        return _parse_sql_server_format_options(dataset)
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


def _parse_sql_server_format_options(dataset: dict) -> dict | UnsupportedValue:
    """
    Parses format options from a SQL Server dataset definition.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        SQL Server dataset properties as a dictionary of format options.
    """
    format_settings = dataset.get("format_settings", {})
    return {
        "type": "sqlserver",
        "query_isolation_level": _parse_query_isolation_level(format_settings.get("query_isolation_level")),
        "query_timeout_seconds": _parse_query_timeout_seconds(format_settings.get("query_timeout_seconds")),
        "numPartitions": format_settings.get("numPartitions"),
        "batchsize": format_settings.get("batchsize"),
        "sessionInitStatement": format_settings.get("sessionInitStatement"),
        "mode": _parse_sql_write_behavior(format_settings.get("mode")),
    }


def _parse_dataset_type(dataset_type: str) -> str | UnsupportedValue:
    """
    Parses a dataset type from an Azure Data Factory source or sink dataset (e.g. 'DeltaSource') into a Spark data source or sink format (e.g. 'delta'). Any datasets which are not supported will return an ``UnsupportedValue`` object.

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
        return UnsupportedValue(value=dataset_type, message=f"Unsupported dataset type '{dataset_type}'")
    return result


def _parse_sql_write_behavior(write_mode: str) -> str | UnsupportedValue:
    """
    Parses an ADF write mode into a Spark output mode. Any SQL write modes which are not translatable will return an ``UnsupportedValue`` object.

    Args:
        write_mode: ADF write mode expression.

    Returns:
        Normalized Spark output mode.
    """
    if write_mode == "insert":
        return "append"
    return UnsupportedValue(value=write_mode, message=f"Unsupported SQL write mode '{write_mode}'")


def _parse_query_timeout_seconds(properties: dict | None) -> int | UnsupportedValue:
    """
    Parses the query timeout from dataset properties. Any query timeout strings which are not translatable into an integer number of secondswill return an ``UnsupportedValue`` object.

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


def _parse_query_timeout_string(timeout_string: str) -> int | UnsupportedValue:
    """
    Parses an ``hh:mm:ss`` string into seconds.

    Args:
        timeout_string: Timeout string in ``HH:MM:SS`` format.

    Returns:
        Integer number of seconds represented by the string.
    """
    try:
        time_format = "%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
        return int(time_delta.total_seconds())
    except ValueError:
        return UnsupportedValue(value=timeout_string, message=f"Invalid query timeout string '{timeout_string}'")


def _parse_character_value(char: str) -> str:
    """
    Parses a single character into a JSON-safe representation.

    Args:
        char: Character literal extracted from the dataset definition.

    Returns:
        JSON-escaped representation of the character.
    """
    return json.dumps(char).strip('"')

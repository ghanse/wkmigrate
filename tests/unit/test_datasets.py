"""Tests for dataset type mappings, parse_spark_data_type, and dataset translator utils."""

from __future__ import annotations

import pytest

from wkmigrate.parsers.dataset_parsers import parse_spark_data_type
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.dataset_translators import translate_dataset
from wkmigrate.translators.dataset_translators.utils import parse_abfs_container_name


@pytest.mark.parametrize(
    "adf_type, expected",
    [
        ("Boolean", "boolean"),
        ("Byte", "tinyint"),
        ("Int16", "short"),
        ("Int32", "int"),
        ("Int64", "long"),
        ("Single", "float"),
        ("Double", "double"),
        ("Decimal", "decimal(38, 38)"),
        ("String", "string"),
        ("DateTime", "timestamp"),
        ("DateTimeOffset", "timestamp"),
        ("Guid", "string"),
        ("Byte[]", "binary"),
        ("TimeSpan", "string"),
    ],
)
def test_known_sqlserver_types(adf_type: str, expected: str) -> None:
    assert parse_spark_data_type(adf_type, "sqlserver") == expected


def test_unknown_sqlserver_type_warns_and_returns_original() -> None:
    with pytest.warns(NotTranslatableWarning, match="UnknownDotNetType"):
        result = parse_spark_data_type("UnknownDotNetType", "sqlserver")
    assert result == "UnknownDotNetType"


@pytest.mark.parametrize(
    "adf_type, expected",
    [
        ("smallint", "short"),
        ("integer", "int"),
        ("bigint", "long"),
        ("real", "float"),
        ("double precision", "double"),
        ("numeric", "decimal(38, 38)"),
        ("boolean", "boolean"),
        ("varchar", "string"),
        ("text", "string"),
        ("date", "date"),
        ("timestamp with time zone", "timestamp"),
        ("bytea", "binary"),
        ("uuid", "string"),
        ("json", "string"),
        ("jsonb", "string"),
    ],
)
def test_known_postgresql_types(adf_type: str, expected: str) -> None:
    assert parse_spark_data_type(adf_type, "postgresql") == expected


@pytest.mark.parametrize(
    "adf_type, expected",
    [
        ("tinyint", "boolean"),
        ("smallint", "short"),
        ("int", "int"),
        ("bigint", "long"),
        ("float", "float"),
        ("double", "double"),
        ("decimal", "decimal(38, 18)"),
        ("varchar", "string"),
        ("datetime", "timestamp"),
        ("blob", "binary"),
        ("json", "string"),
    ],
)
def test_known_mysql_types(adf_type: str, expected: str) -> None:
    assert parse_spark_data_type(adf_type, "mysql") == expected


@pytest.mark.parametrize(
    "adf_type, expected",
    [
        ("NUMBER", "decimal(38, 38)"),
        ("FLOAT", "double"),
        ("BINARY_FLOAT", "float"),
        ("VARCHAR2", "string"),
        ("DATE", "timestamp"),
        ("RAW", "binary"),
        ("BLOB", "binary"),
    ],
)
def test_known_oracle_types(adf_type: str, expected: str) -> None:
    assert parse_spark_data_type(adf_type, "oracle") == expected


@pytest.mark.parametrize("spark_type", ["string", "int", "timestamp", "decimal(10, 2)", "array<string>"])
def test_passthrough_delta_types(spark_type: str) -> None:
    assert parse_spark_data_type(spark_type, "delta") == spark_type


def test_unknown_system_warns_and_returns_original() -> None:
    with pytest.warns(NotTranslatableWarning, match="No data type mapping available"):
        result = parse_spark_data_type("varchar", "cosmosdb")
    assert result == "varchar"


def test_unknown_type_warns_and_returns_original() -> None:
    with pytest.warns(NotTranslatableWarning, match="No data type mapping for"):
        result = parse_spark_data_type("GEOGRAPHY", "sqlserver")
    assert result == "GEOGRAPHY"


def test_dataset_missing_linked_service_returns_unsupported() -> None:
    """Dataset without linked_service_definition returns UnsupportedValue."""
    dataset = {
        "name": "test_dataset",
        "properties": {
            "type": "Avro",
            "location": {"type": "AzureBlobFSLocation", "container": "c", "folder_path": "f", "file_name": "x.avro"},
        },
    }
    result = translate_dataset(dataset)

    assert isinstance(result, UnsupportedValue)
    assert "linked_service" in result.message.lower()


def test_parse_abfs_container_name_missing_location() -> None:
    """Properties without 'location' returns UnsupportedValue."""
    result = parse_abfs_container_name({})

    assert isinstance(result, UnsupportedValue)
    assert "location" in result.message


def test_parse_abfs_container_name_missing_container() -> None:
    """Location without 'container' returns UnsupportedValue."""
    result = parse_abfs_container_name({"location": {}})

    assert isinstance(result, UnsupportedValue)
    assert "container" in result.message

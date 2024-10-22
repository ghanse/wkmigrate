""" This module defines methods for parsing dataset properties to the Databricks SDK's object model."""
import json
from datetime import datetime, timedelta
from typing import Optional
from wkmigrate.enums.isolation_level import IsolationLevel
from wkmigrate.linked_service_translators.abfs_linked_service_translator import translate_abfs_spec
from wkmigrate.linked_service_translators.sql_server_linked_service_translator import translate_sql_server_spec
from wkmigrate.linked_service_translators.databricks_linked_service_translator import translate_cluster_spec
from wkmigrate.utils import identity, translate


def parse_avro_file_dataset(dataset: dict) -> dict:
    """ Parses avro dataset properties to Spark avro reader properties.
        :parameter properties: Avro dataset properties as a ``dict``
        :return: Spark avro reader properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'container': {'key': 'properties', 'parser': _parse_abfs_container_name},
        'folder_path': {'key': 'properties', 'parser': _parse_abfs_file_path},
        'compression_codec': {'key': 'avro_compression_codec', 'parser': identity}
    }
    return {**translate(dataset, mapping), **translate_abfs_spec(dataset.get('linked_service_definition'))}


def parse_avro_file_properties(properties: dict) -> dict:
    """ Parses avro file properties to Spark avro reader properties.
        :parameter properties: Avro file properties as a ``dict``
        :return: Spark avro reader properties as a ``dict``
    """
    mapping = {
        'type': {'key': 'type', 'parser': _parse_dataset_type},
        'records_per_file': {'key': 'format_settings', 'parser': lambda x: x.get('max_rows_per_file')},
        'file_path_prefix': {'key': 'format_settings', 'parser': lambda x: x.get('file_name_prefix')},
    }
    return translate(properties, mapping)


def parse_delimited_file_dataset(dataset: dict) -> dict:
    """ Parses delimited dataset properties to Spark CSV reader properties.
        :parameter properties: Delimited dataset properties as a ``dict``
        :return: Spark CSV reader properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'container': {'key': 'properties', 'parser': _parse_abfs_container_name},
        'folder_path': {'key': 'properties', 'parser': _parse_abfs_file_path},
        'sep': {'key': 'properties', 'parser': lambda x: _parse_character_value(x.get('column_delimiter'))},
        'lineSep': {'key': 'properties', 'parser': lambda x: _parse_character_value(x.get('row_delimiter'))},
        'header': {'key': 'properties', 'parser': lambda x: x.get('first_row_as_header')},
        'quote': {'key': 'properties', 'parser': lambda x: _parse_character_value(x.get('quote_char'))},
        'escape': {'key': 'properties', 'parser': lambda x: _parse_character_value(x.get('escape_char'))},
        'nullValue': {'key': 'properties', 'parser': lambda x: _parse_character_value(x.get('null_value'))},
        'compression': {'key': 'properties', 'parser': lambda x: x.get('compression_codec')},
        'encoding': {'key': 'properties', 'parser': lambda x: x.get('encoding_name')}
    }
    return {**translate(dataset, mapping), **translate_abfs_spec(dataset.get('linked_service_definition'))}


def parse_delimited_file_properties(properties: dict) -> dict:
    """ Parses delimited file properties to Spark CSV reader properties.
        :parameter properties: Delimited file properties as a ``dict``
        :return: Spark CSV reader properties as a ``dict``
    """
    mapping = {
        'type': {'key': 'type', 'parser': _parse_dataset_type},
        'quoteAll': {'key': 'format_settings', 'parser': lambda x: x.get('quote_all_text')},
        'records_per_file': {'key': 'format_settings', 'parser': lambda x: x.get('max_rows_per_file')}
    }
    return translate(properties, mapping)


def parse_delta_table_dataset(dataset: dict) -> dict:
    """ Parses Delta dataset properties to Delta table reader properties.
        :parameter properties: Delta dataset properties as a ``dict``
        :return: Delta table reader properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'database_name': {'key': 'properties', 'parser': lambda x: x.get('database')},
        'table_name': {'key': 'properties', 'parser': lambda x: x.get('table')}
    }
    return {**translate(dataset, mapping), **translate_cluster_spec(dataset.get('linked_service_definition'))}


def parse_delta_properties(properties: dict) -> dict:
    """ Parses Delta file to Delta table reader properties.
        :parameter properties: Delta file properties as a ``dict``
        :return: Delta table reader properties as a ``dict``
    """
    mapping = {'type': {'key': 'type', 'parser': _parse_dataset_type}}
    return translate(properties, mapping)


def parse_json_file_dataset(dataset: dict) -> dict:
    """ Parses JSON dataset properties to Spark JSON reader properties.
        :parameter properties: JSON dataset properties as a ``dict``
        :return: Spark JSON reader properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'container': {'key': 'properties', 'parser': _parse_abfs_container_name},
        'folder_path': {'key': 'properties', 'parser': _parse_abfs_file_path},
        'encoding': {'key': 'properties', 'parser': lambda x: x.get('encoding_name')},
        'compression': {'key': 'properties', 'parser': lambda x: _parse_compression_type(x.get('compression'))}
    }
    return {**translate(dataset, mapping), **translate_abfs_spec(dataset.get('linked_service_definition'))}


def parse_json_file_properties(properties: dict) -> dict:
    """ Parses JSON file properties to Spark JSON reader properties.
        :parameter properties: JSON file properties as a ``dict``
        :return: Spark JSON reader properties as a ``dict``
    """
    mapping = {
        'type': {'key': 'type', 'parser': _parse_dataset_type},
        'records_per_file': {'key': 'format_settings', 'parser': lambda x: x.get('maxRowsPerFile')}
    }
    return translate(properties, mapping)


def parse_orc_file_dataset(dataset: dict) -> dict:
    """ Parses ORC dataset properties to Spark ORC reader properties.
        :parameter properties: ORC dataset properties as a ``dict``
        :return: Spark ORC reader properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'container': {'key': 'properties', 'parser': _parse_abfs_container_name},
        'folder_path': {'key': 'properties', 'parser': _parse_abfs_file_path},
        'compression': {'key': 'properties', 'parser': lambda x: x.get('orc_compression_codec')}
    }
    return {**translate(dataset, mapping), **translate_abfs_spec(dataset.get('linked_service_definition'))}


def parse_orc_file_properties(properties: dict) -> dict:
    """ Parses ORC file properties to Spark ORC reader properties.
        :parameter properties: ORC file properties as a ``dict``
        :return: Spark ORC reader properties as a ``dict``
    """
    mapping = {
        'type': {'key': 'type', 'parser': _parse_dataset_type},
        'file_name_prefix': {'key': 'format_settings', 'parser': lambda x: x.get('file_name_prefix')},
        'records_per_file': {'key': 'format_settings', 'parser': lambda x: x.get('max_rows_per_file')}
    }
    return translate(properties, mapping)


def parse_parquet_file_dataset(dataset: dict) -> dict:
    """ Parses parquet dataset properties to Spark parquet reader properties.
        :parameter properties: Parquet dataset properties as a ``dict``
        :return: Spark parquet reader properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'container': {'key': 'properties', 'parser': _parse_abfs_container_name},
        'folder_path': {'key': 'properties', 'parser': _parse_abfs_file_path},
        'compression': {'key': 'properties', 'parser': lambda x: x.get('compression_codec')}
    }
    return {**translate(dataset, mapping), **translate_abfs_spec(dataset.get('linked_service_definition'))}


def parse_parquet_file_properties(properties: dict) -> dict:
    """ Parses parquet file properties to Spark parquet reader properties.
        :parameter properties: Parquet properties as a ``dict``
        :return: Spark parquet reader properties as a ``dict``
    """
    mapping = {
        'type': {'key': 'type', 'parser': _parse_dataset_type},
        'file_name_prefix': {'key': 'format_settings', 'parser': lambda x: x.get('file_name_prefix')},
        'records_per_file': {'key': 'format_settings', 'parser': lambda x: x.get('max_rows_per_file')}
    }
    return translate(properties, mapping)


def parse_sql_server_dataset(dataset: dict) -> dict:
    """ Parses SQL Server dataset properties to Spark SQL Server connector properties.
        :parameter properties: SQL Server dataset properties as a ``dict``
        :return: Spark SQL Server connector properties as a ``dict``
    """
    mapping = {
        'dataset_name': {'key': 'name', 'parser': identity},
        'schema_name': {'key': 'properties', 'parser': lambda x: x.get('schema_type_properties_schema')},
        'table_name': {'key': 'properties', 'parser': lambda x: x.get('table')}
    }
    return {**translate(dataset, mapping), **translate_sql_server_spec(dataset.get('linked_service_definition'))}


def parse_sql_server_properties(properties: dict) -> dict:
    """ Parses a set of SQL Server query properties to Spark SQL Server connector properties.
        :parameter properties: SQL Server query properties as a ``dict``
        :return: Spark SQL Server connection properties as a ``dict``
    """
    mapping = {
        'type': {'key': 'type', 'parser': _parse_dataset_type},
        'query_isolation_level': {'key': 'isolation_level', 'parser': _parse_query_isolation_level},
        'query_timeout_seconds': {'key': 'query_timeout', 'parser': _parse_query_timeout_seconds}
    }
    return translate(properties, mapping)


def _parse_character_value(char: str) -> str:
    return json.dumps(char).strip('"')


def _parse_dataset_type(dataset_type: str) -> str:
    """ Parses a Data Factory dataset type to a Spark DataFrameReader format.
        :parameter dataset_type: Data Factory dataset type as a ``str``
        :return: Spark DataFrameReader format as a ``str``
    """
    mappings = {
        'AvroSource': 'avro',
        'AvroSink': 'avro',
        'AzureDatabricksDeltaLakeSource': 'delta',
        'AzureDatabricksDeltaLakeSink': 'delta',
        'AzureSqlSource': 'sqlserver',
        'AzureSqlSink': 'sqlserver',
        'DelimitedTextSource': 'csv',
        'DelimitedTextSink': 'csv',
        'JsonSource': 'json',
        'JsonSink': 'json',
        'OrcSource': 'orc',
        'OrcSink': 'orc',
        'ParquetSource': 'parquet',
        'ParquetSink': 'parquet'
    }
    return mappings.get(dataset_type)


def _parse_compression_type(compression: dict) -> str:
    return compression.get('type')


def _parse_query_timeout_seconds(properties: Optional[dict]) -> int:
    """ Parses the timeout number of seconds from the dataset properties.
        :parameter properties: Dataset properties as a ``dict``
        :return: Timeout seconds as an ``int``
    """
    if properties is None or 'query_timeout' not in properties:
        return 0
    return _parse_query_timeout_string(properties.get('query_timeout'))


def _parse_query_isolation_level(properties: Optional[dict]) -> Optional[str]:
    """ Parses the database transaction isolation level from the dataset properties.
        :parameter properties: Dataset properties as a ``dict``
        :return: Isolation level as an ``str``
    """
    if properties is None or 'isolation_level' not in properties:
        return 'READ_COMMITTED'
    return IsolationLevel[properties.get('isolation_level')]


def _parse_query_timeout_string(timeout_string: str) -> int:
    """ Parses a timeout string in the format ``hh:mm:ss`` into an integer number of seconds.
        :parameter timeout_string: Timeout string in the format ``hh:mm:ss``
        :return: Integer number of seconds
    """
    time_format = '%H:%M:%S'
    date_time = datetime.strptime(timeout_string, time_format)
    time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
    return int(time_delta.total_seconds())


def _parse_abfs_container_name(properties: dict) -> str:
    """ Parses an ABFS container name from the full ABFS location in a file properties object.
        :parameter properties: File properties as a ``dict``
        :return: The ABFS container name as a ``str``
    """
    location = properties.get('location')
    return location.get('file_system')


def _parse_abfs_file_path(properties: dict) -> str:
    """ Parses an ABFS file path from the full ABFS location in a file properties object.
        :parameter properties: File properties as a ``dict``
        :return: The ABFS file path as a ``str``
    """
    location = properties.get('location')
    return location.get('folder_path')

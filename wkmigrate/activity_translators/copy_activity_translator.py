""" This module defines methods for translating Databricks Spark jar activities."""
from wkmigrate.utils import identity, translate
from wkmigrate.activity_translators.parsers import parse_data_column_mapping, \
    parse_query_isolation_level, parse_query_timeout_seconds


mapping = {
    'source_query_timeout': {'key': 'source', 'parser': parse_query_timeout_seconds},
    'source_query_isolation_level': {'key': 'source', 'parser': parse_query_isolation_level},
    'column_mapping': {'key': 'translator', 'parser': parse_data_column_mapping},
    'source_dataset_definition': {'key': 'input_dataset_definitions', 'parser': identity},
    'sink_dataset_definition': {'key': 'output_dataset_definitions', 'parser': identity}
}


def translate_copy_activity(activity: dict) -> dict:
    """ Translates a Databricks Spark jar activity definition in Data Factory's object model to a Databricks Spark jar
        task in the Databricks SDK object model.
        :parameter activity: Databricks Spark jar activity definition as a ``dict``
        :return: Databricks Spark jar task properties as a ``dict``
    """
    return translate(activity, mapping)

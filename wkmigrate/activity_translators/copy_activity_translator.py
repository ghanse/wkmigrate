""" This module defines methods for translating Databricks Spark jar activities."""
from wkmigrate.utils import identity, translate
from wkmigrate.activity_translators.parsers import parse_dataset, parse_dataset_mapping, parse_dataset_properties


mapping = {
    'source_properties': {'key': 'source', 'parser': parse_dataset_properties},
    'sink_properties': {'key': 'sink', 'parser': parse_dataset_properties},
    'column_mapping': {'key': 'translator', 'parser': parse_dataset_mapping},
    'source_dataset': {'key': 'input_dataset_definitions', 'parser': parse_dataset},
    'sink_dataset': {'key': 'output_dataset_definitions', 'parser': parse_dataset}
}


def translate_copy_activity(activity: dict) -> dict:
    """ Translates a Databricks Spark jar activity definition in Data Factory's object model to a Databricks Spark jar
        task in the Databricks SDK object model.
        :parameter activity: Databricks Spark jar activity definition as a ``dict``
        :return: Databricks Spark jar task properties as a ``dict``
    """
    return translate(activity, mapping)

""" This module defines methods for translating Databricks Notebook activities."""
from wkmigrate.activity_translators.parsers import parse_notebook_parameters
from wkmigrate.utils import identity, translate

mapping = {
    'notebook_path': {'key': 'notebook_path', 'parser': identity},
    'base_parameters': {'key': 'base_parameters', 'parser': parse_notebook_parameters}
}


def translate_notebook_activity(activity: dict) -> dict:
    """ Translates a Databricks notebook activity definition in Data Factory's object model to a Databricks notebook
        task in the Databricks SDK object model.
        :parameter activity: Databricks notebook activity definition as a ``dict``
        :return: Databricks notebook task properties as a ``dict``
    """
    return translate(activity, mapping)

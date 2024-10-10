""" This module defines methods for translating Databricks parameter values from data pipelines."""
from typing import Optional
from wkmigrate.pipeline_translators.parsers import parse_parameter_value
from wkmigrate.utils import translate


mapping = {'default': {'key': 'default_value', 'parser': parse_parameter_value}}


def translate_parameters(parameters: Optional[dict]) -> Optional[list[dict]]:
    """ Translates a set of parameter definitions in the Data Factory object model to the Databricks SDK object model.
        :parameter parameters: List of parameter definitions as a nested ``dict``
        :return: List of translated parameter definitions as ``dict`` objects
    """
    if parameters is None:
        return None
    return [
        translate_parameter(parameter_name, parameter_def)
        for parameter_name, parameter_def in parameters.items()
    ]


def translate_parameter(parameter_name: str, parameter: dict) -> dict:
    """ Translates a parameter definition in the Data Factory object model to the Databricks SDK object model.
        :parameter parameter_name: Parameter name as a ``str``
        :parameter parameter: Parameter definition as a ``dict``
        :return: Translated parameter definition as a ``dict``
    """
    return {**{'name': parameter_name}, **translate(parameter, mapping)}

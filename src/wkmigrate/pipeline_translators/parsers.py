"""This module defines methods for parsing pipeline objects to the Databricks SDK's object model."""

from typing import Any


def parse_parameter_value(parameter_value: Any) -> Any:
    """Parses a value from a JSON string.
    :parameter parameter_value: Parameter value as a ``str``
    :return: Parsed parameter value with inferred data type
    """
    return str(parameter_value).replace("'", '"')

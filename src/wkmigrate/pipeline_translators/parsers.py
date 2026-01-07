"""This module defines methods for parsing pipeline objects to the Databricks SDK's object model."""

from typing import Any


def parse_parameter_value(parameter_value: Any) -> Any:
    """
    Parses a parameter value and normalizes it to JSON-friendly formatting.

    Args:
        parameter_value: Raw parameter value.

    Returns:
        Parsed parameter value with inferred data type.
    """
    return str(parameter_value).replace("'", '"')

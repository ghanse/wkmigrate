"""This module defines a translator for translating Databricks Notebook activities.

Translators in this module normalize Databricks Notebook activity payloads into internal
representations. Each translator must validate required fields, parse the activity's parameters,
and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

import re
import warnings

from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.supported_types import translates_activity

# Braces are independently optional; ADF serializes consistently so unbalanced
# forms like ``@pipeline().parameters.X}`` won't appear in practice.
_PIPELINE_PARAM_PATTERN = re.compile(r"^@\{?pipeline\(\)\.parameters\.(\w+)\}?$")


@translates_activity("DatabricksNotebook")
def translate_notebook_activity(activity: dict, base_kwargs: dict) -> DatabricksNotebookActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

    Args:
        activity: Notebook activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``DatabricksNotebookActivity`` representation of the notebook task.
    """
    notebook_path = activity.get("notebook_path")
    if not notebook_path:
        return UnsupportedValue(activity, "Missing field 'notebook_path' for Spark Python activity")
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        base_parameters=_parse_notebook_parameters(activity.get("base_parameters")),
        linked_service_definition=activity.get("linked_service_definition"),
    )


def _parse_notebook_parameters(parameters: dict | None) -> dict | None:
    """
    Parses task parameters in a Databricks notebook activity definition.

    Args:
        parameters: Parameter dictionary from the ADF activity.

    Returns:
        Mapping of parameter names to their default values.

    Raises:
        NotTranslatableWarning: If a parameter cannot be resolved.
    """
    if parameters is None:
        return None
    parsed_parameters = {}
    for name, value in parameters.items():
        if isinstance(value, dict):
            value = _resolve_parameter_expression(name, value)
        elif not isinstance(value, str):
            warnings.warn(
                NotTranslatableWarning(
                    f"parameters.{name}",
                    f'Could not resolve default value for parameter {name}, setting to ""',
                ),
                stacklevel=3,
            )
            value = ""
        parsed_parameters[name] = value
    return parsed_parameters


def _resolve_parameter_expression(name: str, expression: dict) -> str:
    """Resolve an ADF expression dict in a notebook base_parameter.

    Handles ``@pipeline().parameters.<param>`` by mapping it to the
    Databricks job parameter reference ``{{job.parameters.<param>}}``.
    Other expressions fall back to ``""`` with a warning.

    Args:
        name: Parameter name from the activity definition.
        expression: Expression dictionary with ``type`` and ``value`` keys.

    Returns:
        Resolved parameter string, or ``""`` if the expression cannot be resolved.
    """
    if expression.get("type") != "Expression":
        warnings.warn(
            NotTranslatableWarning(
                f"parameters.{name}",
                f'Could not resolve default value for parameter {name}, setting to ""',
            ),
            stacklevel=4,
        )
        return ""

    raw = expression.get("value", "")
    match = _PIPELINE_PARAM_PATTERN.match(raw)
    if match:
        return f"{{{{job.parameters.{match.group(1)}}}}}"

    warnings.warn(
        NotTranslatableWarning(
            f"parameters.{name}",
            f'Could not resolve expression for parameter {name}, setting to ""',
        ),
        stacklevel=4,
    )
    return ""

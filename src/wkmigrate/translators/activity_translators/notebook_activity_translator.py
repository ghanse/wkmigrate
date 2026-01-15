"""This module defines a translator for translating Databricks Notebook activities.

Translators in this module normalize Databricks Notebook activity payloads into internal
representations. Each translator must validate required fields, parse the activity's parameters,
and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

import warnings
from wkmigrate.models.ir.activities import DatabricksNotebookActivity
from wkmigrate.not_translatable import NotTranslatableWarning


def translate_notebook_activity(activity: dict, base_kwargs: dict) -> DatabricksNotebookActivity:
    """
    Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

    Args:
        activity: Notebook activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``DatabricksNotebookActivity`` representation of the notebook task.
    """
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path=activity.get("notebook_path"),
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
    # Parse the parameters:
    parsed_parameters = {}
    for name, value in parameters.items():
        if not isinstance(value, str):
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

"""This module defines methods for translating Databricks Notebook activities."""

from wkmigrate.activity_translators.parsers import parse_notebook_parameters
from wkmigrate.models.ir.activities import DatabricksNotebookActivity


def translate_notebook_activity(activity: dict, base_kwargs: dict) -> DatabricksNotebookActivity:
    """
    Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

    Args:
        activity: Notebook activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``DatabricksNotebookActivity`` representation of the notebook task.
    """
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path=activity.get("notebook_path"),
        base_parameters=parse_notebook_parameters(activity.get("base_parameters")),
        linked_service_definition=activity.get("linked_service_definition"),
    )

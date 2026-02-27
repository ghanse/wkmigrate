"""
This module defines a preparer for Notebook activities. The preparer builds a notebook
task definition from the translated notebook activity.
"""

from __future__ import annotations
from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_notebook_activity(activity: DatabricksNotebookActivity) -> PreparedActivity:
    """
    Builds the task payload for a Databricks notebook activity.

    Args:
        activity: Activity definition emitted by the translators
    Returns:
        Databricks notebook task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "notebook_task": {
                "notebook_path": activity.notebook_path,
                "base_parameters": activity.base_parameters,
            },
        }
    )
    return PreparedActivity(task=task)

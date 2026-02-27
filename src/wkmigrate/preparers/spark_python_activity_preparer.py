"""
This module defines a preparer for Spark Python activities. The preparer builds a Spark
Python task definition from the translated Spark Python activity.
"""

from __future__ import annotations
from wkmigrate.models.ir.pipeline import SparkPythonActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_spark_python_activity(activity: SparkPythonActivity) -> PreparedActivity:
    """
    Builds the task payload for a Spark Python activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Spark Python task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "spark_python_task": {
                "python_file": activity.python_file,
                "parameters": activity.parameters,
            },
        }
    )
    return PreparedActivity(task=task)

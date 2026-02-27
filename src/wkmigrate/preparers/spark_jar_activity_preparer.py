"""
This module defines a preparer for Spark JAR activities. The preparer builds a Spark
JAR task definition from the translated Spark JAR activity.
"""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import SparkJarActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_spark_jar_activity(activity: SparkJarActivity) -> PreparedActivity:
    """
    Builds the task payload for a Spark JAR activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Spark JAR task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "spark_jar_task": {
                "main_class_name": activity.main_class_name,
                "parameters": activity.parameters,
            },
        }
    )
    return PreparedActivity(task=task)

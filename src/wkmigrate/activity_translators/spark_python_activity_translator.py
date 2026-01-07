"""This module defines methods for translating Databricks Spark Python activities."""

from wkmigrate.models.ir.activities import SparkPythonActivity


def translate_spark_python_activity(activity: dict, base_kwargs: dict) -> SparkPythonActivity:
    """
    Translates an ADF Databricks Spark Python activity into a ``SparkPythonActivity`` object.

    Args:
        activity: Spark Python activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``SparkPythonActivity`` representation of the Spark Python task.
    """
    return SparkPythonActivity(
        **base_kwargs,
        python_file=activity.get("python_file"),
        parameters=activity.get("parameters"),
    )

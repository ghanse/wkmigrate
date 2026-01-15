"""This module defines a translator for translating Databricks Spark Python activities.

Translators in this module normalize Databricks Spark Python activity payloads into internal
representations. Each translator must validate required fields, parse the activity's parameters,
and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.models.ir.activities import SparkPythonActivity


def translate_spark_python_activity(activity: dict, base_kwargs: dict) -> SparkPythonActivity:
    """
    Translates an ADF Databricks Spark Python activity into a ``SparkPythonActivity`` object.

    Args:
        activity: Spark Python activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``SparkPythonActivity`` representation of the Spark Python task.
    """
    return SparkPythonActivity(
        **base_kwargs,
        python_file=activity.get("python_file"),
        parameters=activity.get("parameters"),
    )

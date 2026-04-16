"""This module defines a translator for translating Databricks Spark Python activities.

Translators in this module normalize Databricks Spark Python activity payloads into internal
representations. Each translator must validate required fields, parse the activity's parameters,
and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.models.ir.pipeline import SparkPythonActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.supported_types import translates_activity


@translates_activity("DatabricksSparkPython")
def translate_spark_python_activity(activity: dict, base_kwargs: dict) -> SparkPythonActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Spark Python activity into a ``SparkPythonActivity`` object.

    Args:
        activity: Spark Python activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``SparkPythonActivity`` representation of the Spark Python task.
    """
    python_file = activity.get("python_file")
    if not python_file:
        return UnsupportedValue(activity, "Missing field 'python_file' for Spark Python activity")
    return SparkPythonActivity(
        **base_kwargs,
        python_file=python_file,
        parameters=activity.get("parameters"),
    )

"""This module defines a translator for translating Databricks Spark jar activities.

Translators in this module normalize Databricks Spark JAR activity payloads into internal
representations. Each translator must validate required fields, parse the activity's parameters,
and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.models.ir.activities import SparkJarActivity


def translate_spark_jar_activity(activity: dict, base_kwargs: dict) -> SparkJarActivity:
    """
    Translates an ADF Databricks Spark JAR activity into a ``SparkJarActivity`` object.

    Args:
        activity: Spark JAR activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``SparkJarActivity`` representation of the Spark JAR task.
    """
    return SparkJarActivity(
        **base_kwargs,
        main_class_name=activity.get("main_class_name"),
        parameters=activity.get("parameters"),
        libraries=activity.get("libraries"),
    )

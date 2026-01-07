"""This module defines methods for translating Databricks Spark jar activities."""

from wkmigrate.models.ir.activities import SparkJarActivity


def translate_spark_jar_activity(activity: dict, base_kwargs: dict) -> SparkJarActivity:
    """
    Translates an ADF Databricks Spark JAR activity into a ``SparkJarActivity`` object.

    Args:
        activity: Spark JAR activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``SparkJarActivity`` representation of the Spark JAR task.
    """
    return SparkJarActivity(
        **base_kwargs,
        main_class_name=activity.get("main_class_name"),
        parameters=activity.get("parameters"),
        libraries=activity.get("libraries"),
    )

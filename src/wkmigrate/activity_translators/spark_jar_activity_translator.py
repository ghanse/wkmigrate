"""This module defines methods for translating Databricks Spark jar activities."""

from wkmigrate.utils import identity, translate


mapping = {
    "main_class_name": {"key": "main_class_name", "parser": identity},
    "parameters": {"key": "parameters", "parser": identity},
}


def translate_spark_jar_activity(activity: dict) -> dict:
    """Translates a Databricks Spark jar activity definition in Data Factory's object model to a Databricks Spark jar
    task in the Databricks SDK object model.
    :parameter activity: Databricks Spark jar activity definition as a ``dict``
    :return: Databricks Spark jar task properties as a ``dict``
    """
    translated = translate(activity, mapping)
    if translated is None:
        raise ValueError('Translation failed')
    return translated

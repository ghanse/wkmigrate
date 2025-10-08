"""This module defines methods for translating Databricks Spark Python activities."""

from wkmigrate.utils import identity, translate


mapping = {
    "python_file": {"key": "python_file", "parser": identity},
    "parameters": {"key": "parameters", "parser": identity},
}


def translate_spark_python_activity(activity: dict) -> dict:
    """Translates a Databricks Spark Python activity definition in Data Factory's object model to a Databricks Spark
    Python task in the Databricks SDK object model.
    :parameter activity: Databricks Spark Python activity definition as a ``dict``
    :return: Databricks Spark Python task properties as a ``dict``
    """
    translated = translate(activity, mapping)
    if translated is None:
        raise ValueError('Translation failed')
    return translated

"""This module defines methods for translating activities from data pipelines."""

from __future__ import annotations
from collections.abc import Callable

from wkmigrate.activity_translators.parsers import parse_dependencies, parse_policy
from wkmigrate.linked_service_translators.databricks_linked_service_translator import (
    translate_cluster_spec,
)
from wkmigrate.activity_translators.notebook_activity_translator import translate_notebook_activity
from wkmigrate.activity_translators.spark_jar_activity_translator import translate_spark_jar_activity
from wkmigrate.activity_translators.spark_python_activity_translator import translate_spark_python_activity
from wkmigrate.activity_translators.if_condition_activity_translator import translate_if_condition_activity
from wkmigrate.activity_translators.for_each_activity_translator import translate_for_each_activity
from wkmigrate.activity_translators.copy_activity_translator import translate_copy_activity
from wkmigrate.models.ir.activities import Activity, DatabricksNotebookActivity
from wkmigrate.not_translatable import not_translatable_context

TypeTranslator = Callable[[dict, dict], Activity | tuple[Activity, list[Activity]]]
_type_translators: dict[str, TypeTranslator] = {
    "DatabricksNotebook": translate_notebook_activity,
    "DatabricksSparkJar": translate_spark_jar_activity,
    "DatabricksSparkPython": translate_spark_python_activity,
    "IfCondition": translate_if_condition_activity,
    "ForEach": translate_for_each_activity,
    "Copy": translate_copy_activity,
}


def translate_activities(activities: list[dict] | None) -> list[Activity] | None:
    """
    Translates a collection of ADF activities into a list of ``Activity`` objects.

    Args:
        activities: List of activity definitions to translate.

    Returns:
        List of translated activities as a ``list[Activity]`` or ``None`` when no input was provided.
    """
    if activities is None:
        return None
    translated = []
    for activity in activities:
        translated_activity = translate_activity(activity)
        if isinstance(translated_activity, tuple):
            translated.append(translated_activity[0])
            translated.extend(translated_activity[1])
            continue
        translated.append(translated_activity)
    return translated


def translate_activity(activity: dict) -> Activity | tuple[Activity, list[Activity]]:
    """
    Translates a single ADF activity into an ``Activity`` object.

    Args:
        activity: Activity definition emitted by ADF.

    Returns:
        Translated activity and an optional list of nested activities (for If/ForEach activities).
    """
    activity_name = activity.get("name")
    activity_type = activity.get("type") or "Unsupported"
    with not_translatable_context(activity_name, activity_type):
        base_kwargs = _build_base_activity_kwargs(activity, activity_type)
        return _translate_activity(activity_type, activity, base_kwargs)


def _build_base_activity_kwargs(activity: dict, activity_type: str) -> dict:
    """
    Builds keyword arguments shared across activity types.

    Args:
        activity: Activity definition as a ``dict``.
        activity_type: Activity type string emitted by ADF.

    Returns:
        Keyword arguments common to all translated activities as a ``dict``.
    """
    policy = parse_policy(activity.get("policy"))
    depends_on = parse_dependencies(activity.get("depends_on"))
    new_cluster = translate_cluster_spec(activity.get("linked_service_definition", {}))
    name = activity.get("name") or "UNNAMED_TASK"
    task_key = name or "TASK_NAME_NOT_PROVIDED"
    return {
        "name": name,
        "task_key": task_key,
        "activity_type": activity_type,
        "description": activity.get("description"),
        "timeout_seconds": policy.get("timeout_seconds"),
        "max_retries": policy.get("max_retries"),
        "min_retry_interval_millis": policy.get("min_retry_interval_millis"),
        "depends_on": depends_on,
        "new_cluster": new_cluster,
    }


def _get_placeholder_activity(base_kwargs: dict) -> DatabricksNotebookActivity:
    """
    Creates a placeholder notebook task for unsupported activities.

    Args:
        base_kwargs: Common task metadata.

    Returns:
        Databricks ``NotebookActivity`` object as a placeholder task.
    """
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path="/UNSUPPORTED_ADF_ACTIVITY",
    )


def _translate_activity(
    activity_type: str,
    activity: dict,
    base_kwargs: dict,
) -> Activity | tuple[Activity, list[Activity]]:
    """
    Dispatches activity translation to the appropriate translator.

    Args:
        activity_type: ADF activity type string.
        activity: Activity definition as a ``dict``.
        base_kwargs: Shared task metadata.

    Returns:
        Translated activity and an optional list of nested activities (for If/ForEach activities).
    """
    translator = _type_translators.get(activity_type)
    if translator is not None:
        return translator(activity, base_kwargs)
    return _get_placeholder_activity(base_kwargs)

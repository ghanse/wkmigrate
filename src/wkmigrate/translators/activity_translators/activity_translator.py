"""This module defines methods for translating activities from data pipelines."""

from __future__ import annotations
from collections.abc import Callable
from datetime import datetime, timedelta
import warnings

from wkmigrate.translators.linked_service_translators import (
    translate_databricks_cluster_spec,
)
from wkmigrate.activity_translators.notebook_activity_translator import translate_notebook_activity
from wkmigrate.activity_translators.spark_jar_activity_translator import translate_spark_jar_activity
from wkmigrate.activity_translators.spark_python_activity_translator import translate_spark_python_activity
from wkmigrate.activity_translators.if_condition_activity_translator import translate_if_condition_activity
from wkmigrate.activity_translators.for_each_activity_translator import translate_for_each_activity
from wkmigrate.activity_translators.copy_activity_translator import translate_copy_activity
from wkmigrate.models.ir.activities import Activity, Dependency, ForEachActivity, IfConditionActivity
from wkmigrate.models.ir.translator_result import ActivityTranslatorResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning, not_translatable_context
from wkmigrate.utils import get_placeholder_activity, normalize_translated_result


TypeTranslator = Callable[[dict, dict], ActivityTranslatorResult]

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
    Translates a collection of ADF activities into a flattened list of ``Activity`` objects.

    Args:
        activities: List of activity definitions to translate.

    Returns:
        Flattened list of translated activities as a ``list[Activity]`` or ``None`` when no input was provided.
    """
    if activities is None:
        return None
    translated: list[Activity] = []
    for activity in activities:
        translated_activity = translate_activity(activity)
        translated.extend(_flatten_activities(translated_activity))
    return translated


def translate_activity(activity: dict) -> Activity:
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
        result = _translate_activity(activity_type, activity, base_kwargs)
        return normalize_translated_result(result, base_kwargs)


def _flatten_activities(activity: Activity) -> list[Activity]:
    """
    Flattens an activity or list of activities, including any nested If/ForEach children.

    Args:
        activity: Activity to flatten.

    Returns:
        List of activities.
    """
    flattened = [activity]
    if isinstance(activity, IfConditionActivity):
        for child in activity.child_activities:
            flattened.extend(_flatten_activities(child))
    if isinstance(activity, ForEachActivity):
        for child in activity.inner_activities:
            flattened.extend(_flatten_activities(child))
    return flattened


def _build_base_activity_kwargs(activity: dict, activity_type: str) -> dict:
    """
    Builds keyword arguments shared across activity types.

    Args:
        activity: Activity definition as a ``dict``.
        activity_type: Activity type string emitted by ADF.

    Returns:
        Keyword arguments common to all translated activities as a ``dict``.
    """
    policy = _parse_policy(activity.get("policy"))
    depends_on = _parse_dependencies(activity.get("depends_on"))
    cluster_spec = activity.get("linked_service_definition")
    new_cluster = translate_databricks_cluster_spec(cluster_spec) if cluster_spec else None
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


def _translate_activity(
    activity_type: str,
    activity: dict,
    base_kwargs: dict,
) -> ActivityTranslatorResult:
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
    return get_placeholder_activity(base_kwargs)


def _parse_policy(policy: dict | None) -> dict:
    """
    Parses a data factory pipeline activity policy into a dictionary of policy settings.

    Args:
        policy: Activity policy block from the ADF definition.

    Returns:
        Dictionary containing policy settings.

    Raises:
        NotTranslatableWarning: If secure input/output logging is used.
    """
    if policy is None:
        return {}
    cached_policy = policy.get("_wkmigrate_cached_policy")
    if cached_policy is not None:
        return cached_policy
    # Warn about secure input/output logging:
    if "secure_input" in policy:
        warnings.warn(
            NotTranslatableWarning(
                "secure_input",
                "Secure input logging not applicable to Databricks workflows.",
            ),
            stacklevel=3,
        )
    if "secure_output" in policy:
        warnings.warn(
            NotTranslatableWarning(
                "secure_output",
                "Secure output logging not applicable to Databricks workflows.",
            ),
            stacklevel=3,
        )
    # Parse the policy attributes:
    parsed_policy = {}
    # Parse the timeout seconds:
    if "timeout" in policy:
        timeout_value = policy.get("timeout")
        if timeout_value is not None:
            parsed_policy["timeout_seconds"] = _parse_activity_timeout_string(timeout_value)
    # Parse the number of retry attempts:
    if "retry" in policy:
        retry_value = policy.get("retry")
        if retry_value is not None:
            parsed_policy["max_retries"] = int(retry_value)
    # Parse the retry wait time in milliseconds:
    if "retry_interval_in_seconds" in policy:
        parsed_policy["min_retry_interval_millis"] = 1000 * int(policy.get("retry_interval_in_seconds", 0))
    policy["_wkmigrate_cached_policy"] = parsed_policy
    return parsed_policy


def _parse_dependencies(dependencies: list[dict] | None) -> list[Dependency] | UnsupportedValue | None:
    """
    Parses a data factory pipeline activity's dependencies.

    Args:
        dependencies: Dependency definitions provided by the activity.

    Returns:
        List of ``Dependency`` objects describing upstream relationships.
    """
    if dependencies is None:
        return None
    # Parse the dependencies from the list:
    parsed_dependencies: list[Dependency] = []
    for dependency in dependencies:
        # Get the dependency condition:
        conditions = dependency.get("dependencyConditions")
        # Validate the dependency conditions:
        if conditions is not None and len(conditions) > 1:
            return UnsupportedValue(
                value=dependency, message="Dependencies with multiple conditions are not supported."
            )
        # Append the dependency:
        parsed_dependencies.append(
            Dependency(
                task_key=dependency.get("activity"),
                outcome=dependency.get("dependency_conditions"),
            )
        )
    return parsed_dependencies


def _parse_activity_timeout_string(timeout_string: str) -> int:
    """
    Parses a timeout string in the format ``d.hh:mm:ss`` into seconds.

    Args:
        timeout_string: Timeout string from the activity policy.

    Returns:
        Total seconds represented by the timeout.
    """
    if timeout_string[:2] == "0.":
        # Parse the timeout string to HH:MM:SS format:
        timeout_string = timeout_string[2:]
        time_format = "%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
    else:
        # Parse the timeout string to DD.HH:MM:SS format:
        timeout_string = timeout_string.zfill(11)
        time_format = "%d.%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(
            days=date_time.day,
            hours=date_time.hour,
            minutes=date_time.minute,
            seconds=date_time.second,
        )
    return int(time_delta.total_seconds())

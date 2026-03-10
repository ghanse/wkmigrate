"""This module defines an activity translator from ADF payloads to internal IR.

The activity translator routes each ADF activity to its corresponding translator, stitches in
shared metadata (policy, dependencies, cluster specs), and flattens nested control-flow
constructs. It also captures non-translatable warnings so that callers receive structured
diagnostics with the translated activities.

Translation state is captured in a ``TranslationContext`` that is threaded through function calls
and returned alongside results.  No mutable state is shared between functions — each state transition
produces a new context, making the data flow fully explicit.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from types import MappingProxyType
from typing import Any

from wkmigrate.models.ir.pipeline import Activity, Dependency, IfConditionActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning, not_translatable_context
from wkmigrate.translators.activity_translators.copy_activity_translator import translate_copy_activity
from wkmigrate.translators.activity_translators.databricks_job_activity_translator import (
    translate_databricks_job_activity,
)
from wkmigrate.translators.activity_translators.for_each_activity_translator import translate_for_each_activity
from wkmigrate.translators.activity_translators.if_condition_activity_translator import translate_if_condition_activity
from wkmigrate.translators.activity_translators.lookup_activity_translator import translate_lookup_activity
from wkmigrate.translators.activity_translators.notebook_activity_translator import translate_notebook_activity
from wkmigrate.translators.activity_translators.set_variable_activity_translator import translate_set_variable_activity
from wkmigrate.translators.activity_translators.spark_jar_activity_translator import translate_spark_jar_activity
from wkmigrate.translators.activity_translators.spark_python_activity_translator import translate_spark_python_activity
from wkmigrate.translators.activity_translators.web_activity_translator import translate_web_activity
from wkmigrate.translators.linked_service_translators import translate_databricks_cluster_spec
from wkmigrate.utils import get_placeholder_activity, normalize_translated_result, parse_activity_timeout_string


TypeTranslator = Callable[[dict, dict], TranslationResult]

_default_type_translators: dict[str, TypeTranslator] = {
    "DatabricksJob": translate_databricks_job_activity,
    "DatabricksNotebook": translate_notebook_activity,
    "DatabricksSparkJar": translate_spark_jar_activity,
    "DatabricksSparkPython": translate_spark_python_activity,
    "Copy": translate_copy_activity,
    "Lookup": translate_lookup_activity,
    "WebActivity": translate_web_activity,
}


def default_context() -> TranslationContext:
    """
    Creates a ``TranslationContext`` initialised with the default type-translator registry.

    Returns:
        Fresh ``TranslationContext`` with an empty activity cache and the default registry.
    """
    return TranslationContext(registry=MappingProxyType(dict(_default_type_translators)))


def translate_activities_with_context(
    activities: list[dict] | None,
    context: TranslationContext | None = None,
) -> tuple[list[Activity] | None, TranslationContext]:
    """
    Translates a collection of ADF activities in dependency-first order, returning the
    final translation context alongside the results.

    Activities with no upstream dependencies are visited first, followed by their
    dependents.  Each translated activity is stored in the returned context so that
    callers can inspect the final cache.

    Args:
        activities: List of raw ADF activity definitions, or ``None``.
        context: Optional translation context.  When ``None`` a fresh context with the
            default type-translator registry is used.

    Returns:
        Tuple of ``(translated_activities, final_context)``.  The activity list is
        ``None`` when no input was provided.
    """
    if context is None:
        context = default_context()
    if activities is None:
        return None, context

    index, order = _build_activity_index(activities)
    return _topological_visit(index, order, context)


def translate_activities(activities: list[dict] | None) -> list[Activity] | None:
    """
    Translates a collection of ADF activities into a flattened list of ``Activity`` objects.

    This is a convenience wrapper around ``translate_activities_with_context`` that
    discards the final context.

    Args:
        activities: List of activity definitions to translate.

    Returns:
        Flattened list of translated activities as a ``list[Activity]`` or ``None`` when
        no input was provided.
    """
    result, _ = translate_activities_with_context(activities)
    return result


def translate_activity(activity: dict, is_conditional_task: bool = False) -> Activity:
    """
    Translates a single ADF activity into an ``Activity`` object.

    Args:
        activity: Activity definition emitted by ADF.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Translated ``Activity`` object.
    """
    context = default_context()
    translated, _ = visit_activity(activity, is_conditional_task, context)
    return translated


def visit_activity(
    activity: dict,
    is_conditional_task: bool,
    context: TranslationContext,
) -> tuple[Activity, TranslationContext]:
    """
    Translates a single ADF activity, returning the result and an updated context.

    If the activity has already been translated the cached result is returned with the
    context unchanged.

    Args:
        activity: Activity definition emitted by ADF.
        is_conditional_task: Whether the task lives inside a conditional branch.
        context: Current translation context.

    Returns:
        Tuple of ``(translated_activity, updated_context)``.
    """
    name = activity.get("name")
    cached = context.get_activity(name) if name else None
    if cached is not None:
        return cached, context

    activity_type = activity.get("type") or "Unsupported"
    with not_translatable_context(name, activity_type):
        base_properties = _get_base_properties(activity, is_conditional_task)
        result, context = _dispatch_activity(activity_type, activity, base_properties, context)
        translated = normalize_translated_result(result, base_properties)

    if name:
        context = context.with_activity(name, translated)
    return translated, context


def _dispatch_activity(
    activity_type: str,
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext,
) -> tuple[TranslationResult, TranslationContext]:
    """
    Dispatches activity translation to the appropriate translator.

    For control-flow types (``IfCondition``, ``ForEach``) the context is threaded through
    child translations.  Leaf translators do not modify the context.

    Args:
        activity_type: ADF activity type string.
        activity: Activity definition as a ``dict``.
        base_kwargs: Shared task metadata.
        context: Current translation context.

    Returns:
        Tuple of ``(translator_result, updated_context)``.
    """
    match activity_type:
        case "IfCondition":
            return translate_if_condition_activity(activity, base_kwargs, context)
        case "ForEach":
            return translate_for_each_activity(activity, base_kwargs, context)
        case "SetVariable":
            return translate_set_variable_activity(activity, base_kwargs, context)
        case _:
            translator = context.registry.get(activity_type)
            if translator is not None:
                return translator(activity, base_kwargs), context
            return get_placeholder_activity(base_kwargs), context


def _build_activity_index(activities: list[dict]) -> tuple[dict[str, dict], list[str]]:
    """
    Indexes activities by name for dependency lookup.

    Activities without a name receive a synthetic key (``__unnamed_N__``) to avoid
    collisions when multiple unnamed activities exist in the same pipeline.

    Args:
        activities: Raw ADF activity definitions.

    Returns:
        Tuple of ``(activity_index, visit_order)`` where ``activity_index`` maps keys to
        raw dicts and ``visit_order`` preserves the original ordering.
    """
    activity_index: dict[str, dict] = {}
    visit_order: list[str] = []
    unnamed_counter = 0
    for activity in activities:
        name = activity.get("name")
        if name:
            key = name
        else:
            key = f"__unnamed_{unnamed_counter}__"
            unnamed_counter += 1
        activity_index[key] = activity
        visit_order.append(key)
    return activity_index, visit_order


def _topological_visit(
    activity_index: dict[str, dict],
    visit_order: list[str],
    context: TranslationContext,
) -> tuple[list[Activity], TranslationContext]:
    """
    Visits activities in dependency-first (topological) order.

    Each activity's upstream dependencies are visited before the activity itself.
    The context is threaded through every visit so that each translation sees the
    results of all preceding translations.

    Args:
        activity_index: Mapping of activity keys to raw ADF dicts.
        visit_order: Keys in their original pipeline ordering.
        context: Current translation context.

    Returns:
        Tuple of ``(flattened_activities, final_context)``.
    """
    visited: set[str] = set()
    result: list[Activity] = []

    def _visit(key: str, context: TranslationContext) -> TranslationContext:
        """Recursively visits an activity after all of its dependencies."""
        if key in visited:
            return context
        visited.add(key)

        raw = activity_index.get(key)
        if raw is None:
            return context

        for dep in raw.get("depends_on") or []:
            dep_name = dep.get("activity")
            if dep_name and dep_name in activity_index:
                context = _visit(dep_name, context)

        translated, context = visit_activity(raw, False, context)
        result.extend(_flatten_activities(translated))
        return context

    for key in visit_order:
        context = _visit(key, context)

    return result, context


def _flatten_activities(activity: Activity) -> list[Activity]:
    """
    Flattens an activity, including any nested IfCondition children.

    Args:
        activity: Activity to flatten.

    Returns:
        List of activities with nested children inlined.
    """
    flattened = [activity]
    if isinstance(activity, IfConditionActivity):
        for child in activity.child_activities:
            flattened.extend(_flatten_activities(child))
    return flattened


def _get_base_properties(activity: dict, is_conditional_task: bool = False) -> dict[str, Any]:
    """
    Builds keyword arguments shared across activity types.

    Args:
        activity: Activity definition as a ``dict``.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Activity base properties (e.g. name, description) as a ``dict``.
    """
    policy = _parse_policy(activity.get("policy"))
    depends_on = _parse_dependencies(activity.get("depends_on"), is_conditional_task)
    cluster_spec = activity.get("linked_service_definition")
    new_cluster = translate_databricks_cluster_spec(cluster_spec) if cluster_spec else None
    task_key = activity.get("name") or "UNNAMED_TASK"
    return {
        "name": task_key,
        "task_key": task_key,
        "description": activity.get("description"),
        "timeout_seconds": policy.get("timeout_seconds"),
        "max_retries": policy.get("max_retries"),
        "min_retry_interval_millis": policy.get("min_retry_interval_millis"),
        "depends_on": depends_on,
        "new_cluster": new_cluster,
        "libraries": activity.get("libraries"),
    }


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
    if not policy:
        return {}

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

    parsed_policy = {}
    if "timeout" in policy and policy.get("timeout"):
        timeout_value = policy.get("timeout")
        if timeout_value is not None:
            parsed_policy["timeout_seconds"] = parse_activity_timeout_string(timeout_value)

    if "retry" in policy:
        retry_value = policy.get("retry")
        if retry_value is not None:
            parsed_policy["max_retries"] = int(retry_value)

    if "retry_interval_in_seconds" in policy:
        parsed_policy["min_retry_interval_millis"] = 1000 * int(policy.get("retry_interval_in_seconds", 0))

    return parsed_policy


def _parse_dependencies(
    dependencies: list[dict] | None, is_conditional_task: bool = False
) -> list[Dependency | UnsupportedValue] | None:
    """
    Parses a data factory pipeline activity's dependencies.

    Args:
        dependencies: Dependency definitions provided by the activity.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        List of ``Dependency`` objects describing upstream relationships.
    """
    if not dependencies:
        return None
    return [_parse_dependency(dependency, is_conditional_task) for dependency in dependencies]


def _parse_dependency(dependency: dict, is_conditional_task: bool = False) -> Dependency | UnsupportedValue:
    """
    Parses an individual dependency from a dictionary.

    Args:
        dependency: Dependency definition as a ``dict``.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Dependency object describing the upstream relationship.
    """
    conditions = dependency.get("dependency_conditions", [])
    if len(conditions) > 1:
        return UnsupportedValue(value=dependency, message="Dependencies with multiple conditions are not supported.")

    if is_conditional_task:
        supported_conditions = ["TRUE", "FALSE"]
        outcome = dependency.get("outcome")
    else:
        supported_conditions = ["SUCCEEDED"]
        outcome = None

    if any(condition.upper() not in supported_conditions for condition in conditions):
        return UnsupportedValue(
            value=dependency, message="Dependencies with conditions other than 'Succeeded' are not supported."
        )

    task_key = dependency.get("activity")
    if not task_key:
        return UnsupportedValue(value=dependency, message="Missing value 'activity' for task dependency")

    return Dependency(task_key=task_key, outcome=outcome)

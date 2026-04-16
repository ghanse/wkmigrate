"""This module defines a translator for translating For Each activities.

Translators in this module normalize For Each activity payloads into internal representations.
Each translator must validate required fields, parse the activity's items expression, and emit
``UnsupportedValue`` objects for any unparsable inputs.
"""

from __future__ import annotations
from importlib import import_module

import ast
import re
import warnings

from wkmigrate.models.ir.pipeline import Activity, ForEachActivity, Pipeline, RunJobActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.supported_types import translates_activity


@translates_activity("ForEach")
def translate_for_each_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    """
    Translates an ADF ForEach activity into a ``ForEachActivity`` object.

    ForEach activities are translated into For Each tasks in Databricks Lakeflow Jobs.

    This method returns an ``UnsupportedValue`` as the first element if the activity
    cannot be translated due to missing items or inner activities.

    Args:
        activity: ForEach activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is created.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    if "items" not in activity:
        return UnsupportedValue(value=activity, message="Missing property 'items' in ForEach activity"), context

    items = activity.get("items")
    if not isinstance(items, dict):
        return (
            UnsupportedValue(
                value=activity, message=f"Invalid value '{items}' for property 'items' in ForEach activity"
            ),
            context,
        )

    items_string = _parse_for_each_items(items)
    if isinstance(items_string, UnsupportedValue):
        return items_string, context

    inner_activity_defs = activity.get("activities") or []
    if not inner_activity_defs:
        return (
            UnsupportedValue(value=activity, message="ForEach activity requires at least one inner activity"),
            context,
        )

    for_each_task, context = _translate_inner_activities(activity, inner_activity_defs, context)
    if isinstance(for_each_task, UnsupportedValue):
        return for_each_task, context

    result = ForEachActivity(
        **base_kwargs,
        items_string=items_string,
        for_each_task=for_each_task,
        concurrency=activity.get("batch_count"),
    )
    return result, context


def _translate_inner_activities(
    activity: dict,
    inner_activity_defs: list[dict],
    context: TranslationContext,
) -> tuple[Activity, TranslationContext]:
    """
    Translates the inner activities of a ForEach into either a direct task or a RunJobActivity.

    A single inner activity is translated directly.  Multiple inner activities are wrapped
    in a ``RunJobActivity`` backed by a synthetic inner pipeline.

    Args:
        activity: Parent ForEach activity definition.
        inner_activity_defs: List of inner activity definitions.
        context: Current translation context.

    Returns:
        A tuple with the inner task and the updated context.
    """
    if len(inner_activity_defs) == 1:
        return _translate_single_inner(inner_activity_defs[0], context)

    return _build_inner_pipeline(activity, inner_activity_defs), context


def _translate_single_inner(
    task_def: dict,
    context: TranslationContext,
) -> tuple[Activity, TranslationContext]:
    """
    Translates a single inner activity within a ForEach.

    Args:
        task_def: Inner activity definition.
        context: Current translation context.

    Returns:
        A tuple with the translated activity and the updated context.
    """
    activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
    visit_activity = activity_translator.visit_activity

    filtered = _filter_parameters(task_def)
    if isinstance(filtered, UnsupportedValue):
        filtered = task_def
    return visit_activity(filtered, False, context)


def _build_inner_pipeline(activity: dict, inner_activity_defs: list[dict]) -> RunJobActivity:
    """
    Wraps multiple inner activities into a ``RunJobActivity`` backed by a synthetic pipeline.

    Inner pipelines use a fresh translation context since they represent an independent
    job that will be executed separately.

    Args:
        activity: Parent ForEach activity definition.
        inner_activity_defs: List of inner activity definitions.

    Returns:
        ``RunJobActivity`` wrapping the inner pipeline.
    """
    activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
    translate_activities = activity_translator.translate_activities

    activity_name = activity.get("name") or "FOR_EACH"
    inner_job_name = f"{activity_name}_inner_activities"
    inner_tasks = translate_activities(inner_activity_defs)

    inner_pipeline = Pipeline(
        name=inner_job_name,
        parameters=None,
        schedule=None,
        tasks=inner_tasks or [],
        tags={},
    )
    return RunJobActivity(
        name=inner_job_name,
        task_key=inner_job_name,
        pipeline=inner_pipeline,
    )


def _parse_for_each_items(items: dict) -> str | UnsupportedValue:
    """
    Parses a list of items passed to a ForEach task into a serialized list expression.

    Args:
        items: Expression describing ForEach items.

    Returns:
        Serialized list expression understood by Databricks Jobs.
    """
    if "value" not in items:
        return UnsupportedValue(value=items, message="Missing property 'value' in ForEach activity 'items'")
    value = items.get("value")
    if value is None:
        return UnsupportedValue(value=items, message="Missing property 'value' in ForEach activity 'items'")
    # TODO: Move all dynamic function patterns to a common enum list
    array_pattern = r"@array\(\[(.+)\]\)"
    match = re.match(string=value, pattern=array_pattern)
    if match:
        matched_item = match.group(1)
        return _parse_array_string(matched_item)

    create_array_pattern = r"@createArray\((.+)\)"
    match = re.match(string=value, pattern=create_array_pattern)
    if match:
        matched_item = match.group(1)
        list_items = ast.literal_eval(matched_item)
        quoted_items = ",".join([f'"{item}"' for item in list_items])
        return _parse_array_string(quoted_items)
    return UnsupportedValue(value=items, message=f"Unsupported array expression '{value}' in ForEach activity 'items'")


def _parse_array_string(array_string: str) -> str:
    """
    Parses an array string into a JSON-safe format.

    Args:
        array_string: Raw array expression emitted by ADF.

    Returns:
        JSON-safe representation of the array.
    """
    items = [item.replace("'", "").replace('"', "") for item in array_string.split(",")]
    return '["' + '","'.join(items) + '"]'


def _filter_parameters(activity: dict) -> dict | UnsupportedValue:
    """
    Filters redundant parameters from an activity definition.

    Args:
        activity: Activity definition as a dictionary.

    Returns:
        Filtered activity definition with redundant parameters removed.
    """
    if "base_parameters" not in activity:
        return UnsupportedValue(value=activity, message="Missing property 'base_parameters' for ForEach inner activity")
    base_parameters = activity.get("base_parameters")
    if base_parameters is None:
        return UnsupportedValue(value=activity, message="Property 'base_parameters' is None for ForEach inner activity")
    filtered_parameters = {}
    for name, expression in base_parameters.items():
        if expression is not None and expression.get("value") == "@item()":
            warnings.warn(
                f"Removing redundant parameter {name} with value {expression.get('value')}",
                stacklevel=3,
            )
            continue
        filtered_parameters.update({name: expression})
    return {**activity, "base_parameters": filtered_parameters}

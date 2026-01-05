"""This module defines methods for translating If Condition activities."""

import warnings
from importlib import import_module

from wkmigrate.activity_translators.parsers import parse_condition_expression
from wkmigrate.models.ir.activities import Activity, IfConditionActivity
from wkmigrate.not_translatable import NotTranslatableWarning


def translate_if_condition_activity(
    activity: dict,
    base_kwargs: dict,
) -> IfConditionActivity | tuple[IfConditionActivity, list[Activity]]:
    """
    Translates an ADF IfCondition activity into a ``IfConditionActivity`` object.

    Args:
        activity: IfCondition activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``IfConditionActivity`` representation of the IfCondition task and an optional list of nested activities (for child activities).
    """
    source_expression = activity.get("expression")
    if source_expression is None:
        raise ValueError("If Condition activity must include a valid conditional expression")
    expression = parse_condition_expression(source_expression)
    child_activities: list[Activity] = []
    parent_task_name = activity.get("name")
    if parent_task_name is None:
        warnings.warn(
            NotTranslatableWarning("if_condition.name", "If Condition activity missing name"),
            stacklevel=2,
        )
        parent_task_name = "IF_CONDITION"
    if_false = activity.get("if_false_activities")
    if if_false:
        child_activities.extend(
            _parse_child_activities(if_false, parent_task_name, "false"),
        )
    if_true = activity.get("if_true_activities")
    if if_true:
        child_activities.extend(
            _parse_child_activities(if_true, parent_task_name, "true"),
        )
    if not child_activities:
        warnings.warn(
            NotTranslatableWarning("if_condition.activities", "No child activities of if-else condition activity"),
            stacklevel=2,
        )
    activity_ir = IfConditionActivity(
        **base_kwargs,
        op=expression.get("op"),
        left=expression.get("left"),
        right=expression.get("right"),
        child_activities=child_activities,
    )
    if child_activities:
        return activity_ir, child_activities
    return activity_ir


def _parse_child_activities(
    child_activities: list[dict],
    parent_task_name: str,
    parent_task_outcome: str,
) -> list[Activity]:
    """
    Translates child activities referenced by IfCondition tasks.

    Args:
        child_activities: Child activity definitions attached to the IfCondition.
        parent_task_name: Name of the parent IfCondition task.
        parent_task_outcome: Expected outcome ('true'/'false').

    Returns:
        List of translated child activities with dependency wiring applied as a ``list[Activity]``.
    """
    translated: list[Activity] = []
    translator = import_module("wkmigrate.activity_translators.activity_translator")
    for activity in child_activities:
        depends_on = activity.setdefault("depends_on", [])
        depends_on.append({"activity": parent_task_name, "outcome": parent_task_outcome})
        result = getattr(translator, "translate_activity")(activity)
        if result is None:
            continue
        if isinstance(result, tuple):
            translated.append(result[0])
            translated.extend(result[1])
            continue
        translated.append(result)
    return translated

"""This module defines methods for translating If Condition activities."""

import importlib
import re
import warnings

from wkmigrate.enums.condition_operation_pattern import ConditionOperationPattern
from wkmigrate.models.ir.activities import Activity, IfConditionActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate_if_condition_activity(
    activity: dict,
    base_kwargs: dict,
) -> IfConditionActivity | UnsupportedValue:
    """
    Translates an ADF IfCondition activity into a ``IfConditionActivity`` object. IfCondition activities are translated into If-Else tasks in Databricks Lakeflow Jobs.

    This method returns an ``UnsuportedValue`` if the activity cannot be translated. This can be due to:
    * Missing conditional expression
    * Unparseable conditional expression

    Args:
        activity: IfCondition activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``IfConditionActivity`` representation of the IfCondition task and an optional list of nested activities (for child activities).
    """
    source_expression = activity.get("expression")
    if source_expression is None:
        return UnsupportedValue(value=activity, message="Missing property 'expression' in IfCondition activity")

    expression = _parse_condition_expression(source_expression)
    if isinstance(expression, UnsupportedValue):
        return UnsupportedValue(value=activity, message=expression.message)

    child_activities: list[Activity] = []
    parent_task_name = activity.get("name")
    if parent_task_name is None:
        parent_task_name = "IF_CONDITION_PARENT_TASK"

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
            "No child activities of if-else condition activity",
            stacklevel=3,
        )
    return IfConditionActivity(
        **base_kwargs,
        op=expression.get("op"),
        left=expression.get("left"),
        right=expression.get("right"),
        child_activities=child_activities,
    )


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
    for activity in child_activities:
        depends_on = activity.setdefault("depends_on", [])
        depends_on.append({"activity": parent_task_name, "outcome": parent_task_outcome})
        activity_translator = importlib.import_module("wkmigrate.activity_translators.activity_translator")
        result = activity_translator.translate_activity(activity)
        if isinstance(result, tuple):
            translated.append(result[0])
            translated.extend(result[1])
            continue
        translated.append(result)
    return translated


def _parse_condition_expression(condition: dict) -> dict | UnsupportedValue:
    """
    Parses a condition expression in an If Condition activity definition.

    Args:
        condition: Condition expression dictionary from ADF.

    Returns:
        Dictionary describing the parsed operator and its operands.

    Raises:
        ValueError: If a valid condition expression cannot be parsed.
    """
    # Match a boolean operator:
    condition_value = str(condition.get("value"))
    if not condition_value:
        return UnsupportedValue(
            value=condition, message="Missing property 'value' in IfCondition activity 'expression'"
        )
    for operation in ConditionOperationPattern:
        match = re.match(string=condition_value, pattern=operation.value)
        if match is not None:
            return {
                "op": operation.name,
                "left": match.group(1).replace('"', "").replace("'", ""),
                "right": match.group(2).replace('"', "").replace("'", ""),
            }
    return UnsupportedValue(
        value=condition,
        message=f"Unsupported conditional expression '{condition_value}' in IfCondition activity 'expression'",
    )

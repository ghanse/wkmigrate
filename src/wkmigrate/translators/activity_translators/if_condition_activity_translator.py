"""This module defines a translator for translating If Condition activities.

Translators in this module normalize If Condition activity payloads into internal representations.
Each translator must validate required fields, parse the activity's condition expression, and emit
``UnsupportedValue`` objects for any unparsable inputs.
"""

from __future__ import annotations
from importlib import import_module

import re
import warnings

from wkmigrate.enums.condition_operation_pattern import ConditionOperationPattern
from wkmigrate.models.ir.pipeline import Activity, IfConditionActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.supported_types import translates_activity


@translates_activity("IfCondition")
def translate_if_condition_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    """
    Translates an ADF IfCondition activity into a ``IfConditionActivity`` object.

    The context is threaded through each child activity translation so that the
    activity cache accumulates across branches.

    This method returns an ``UnsupportedValue`` as the first element if the activity
    cannot be translated due to a missing or unparseable conditional expression.

    Args:
        activity: IfCondition activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is created.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    source_expression = activity.get("expression")
    if source_expression is None:
        return (
            UnsupportedValue(value=activity, message="Missing property 'expression' in IfCondition activity"),
            context,
        )

    parsed = _parse_condition_expression(source_expression)
    if isinstance(parsed, UnsupportedValue):
        return UnsupportedValue(value=activity, message=parsed.message), context

    validation_error = _validate_condition_expression(parsed)
    if validation_error:
        return (
            UnsupportedValue(
                value=activity,
                message=f"Unsupported condition expression in IfCondition activity; {validation_error.message}",
            ),
            context,
        )

    parent_task_name = activity.get("name") or "IF_CONDITION_PARENT_TASK"

    child_activities: list[Activity] = []
    for branch_key, outcome in (("if_false_activities", "false"), ("if_true_activities", "true")):
        branch = activity.get(branch_key)
        if branch:
            children, context = _translate_child_activities(branch, parent_task_name, outcome, context)
            child_activities.extend(children)

    if not child_activities:
        warnings.warn(
            "No child activities of if-else condition activity",
            stacklevel=3,
        )

    result = IfConditionActivity(
        **base_kwargs,
        op=parsed["op"],
        left=parsed["left"],
        right=parsed["right"],
        child_activities=child_activities,
    )
    return result, context


def _translate_child_activities(
    child_activities: list[dict],
    parent_task_name: str,
    parent_task_outcome: str,
    context: TranslationContext,
) -> tuple[list[Activity], TranslationContext]:
    """
    Translates child activities referenced by IfCondition tasks.

    The context is threaded through each child so that the activity cache is shared
    across all branches.

    Args:
        child_activities: Child activity definitions attached to the IfCondition.
        parent_task_name: Name of the parent IfCondition task.
        parent_task_outcome: Expected outcome (``'true'``/``'false'``).
        context: Current translation context.

    Returns:
        A tuple with the translated children and the updated context.
    """
    activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
    visit_activity = activity_translator.visit_activity
    parent_dependency = {"activity": parent_task_name, "outcome": parent_task_outcome}

    translated: list[Activity] = []
    for activity in child_activities:
        _activity = activity.copy()
        _activity["depends_on"] = [*(activity.get("depends_on") or []), parent_dependency]
        result, context = visit_activity(_activity, True, context)
        translated.append(result)
    return translated, context


def _parse_condition_expression(condition: dict) -> dict | UnsupportedValue:
    """
    Parses a condition expression in an If Condition activity definition.

    Args:
        condition: Condition expression dictionary from ADF.

    Returns:
        Dictionary describing the parsed operator and its operands, or ``UnsupportedValue``
        when the expression cannot be parsed.
    """
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


def _validate_condition_expression(expression: dict) -> UnsupportedValue | None:
    """
    Validates that a parsed condition expression contains the required fields.

    Args:
        expression: Parsed condition expression dictionary.

    Returns:
        ``UnsupportedValue`` describing the validation failure, or ``None`` when valid.
    """
    if not expression.get("op"):
        return UnsupportedValue(value=expression, message="Missing field 'op' in if condition expression")
    if not expression.get("left"):
        return UnsupportedValue(value=expression, message="Missing field 'left' in if condition expression")
    if not expression.get("right"):
        return UnsupportedValue(value=expression, message="Missing field 'right' in if condition expression")
    return None

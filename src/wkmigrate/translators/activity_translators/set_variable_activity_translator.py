"""
This module defines a translator for translating Set Variable activities.

Translators in this module normalize Set Variable activity payloads into internal
representations. The variable name and value are pulled form the Set Variable activity.

If the Set Variable activity references a complex expression (e.g. '@activity("activity_name").output.output_key'),
the expression is parsed into an equivalent Python expression.
"""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import SetVariableActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_parsers import parse_variable_value


# Supported @pipeline() system variables mapped to Python expressions
_PIPELINE_VARS: dict[str, str] = {
    "Pipeline": "spark.conf.get('spark.databricks.job.parentName', '')",
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('startTime', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
}
_SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES: set[str] = {"firstRow", "value"}


def translate_set_variable_activity(
    activity: dict,
    base_kwargs: dict,
) -> SetVariableActivity | UnsupportedValue:
    """
    Translates an ADF Set Variable activity into a ``SetVariableActivity`` object.

    The activity's ``value`` field may be a static string or an ADF expression object. Supported
    expressions are translated into Python code snippets. Any expression that cannot be translated
    produces an ``UnsupportedValue``.

    Args:
        activity: SetVariable activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``SetVariableActivity`` representation of the SetVariable task.
    """
    variable_name = activity.get("variable_name")
    if not variable_name:
        return UnsupportedValue(
            value=activity,
            message="Missing property 'variable_name' for Set Variable activity",
        )

    raw_value = activity.get("value")
    if raw_value is None:
        return UnsupportedValue(
            value=activity,
            message="Missing property 'value' for Set Variable activity",
        )

    variable_value = parse_variable_value(raw_value)
    if isinstance(variable_value, UnsupportedValue):
        return UnsupportedValue(
            value=activity, message="Unsupported variable value '{raw_value}' for Set Variable activity"
        )

    return SetVariableActivity(
        **base_kwargs,
        variable_name=variable_name,
        variable_value=variable_value,
    )

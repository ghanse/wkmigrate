"""Translator for ADF SetVariable activities."""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import SetVariableActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.activity_translators.expression_utils import parse_variable_value


def translate_set_variable_activity(
    activity: dict,
    base_kwargs: dict,
) -> SetVariableActivity | UnsupportedValue:
    """
    Translates an ADF ``SetVariable`` activity into a :class:`SetVariableActivity` IR.

    The ``value`` field may be a static string or an ADF expression object.
    Supported expressions are translated into Python code snippets by
    :func:`~wkmigrate.translators.activity_translators.expression_utils.parse_variable_value`.
    Any expression that cannot be translated produces an :class:`UnsupportedValue`.

    Args:
        activity: Raw ADF activity dict.
        base_kwargs: Shared task metadata produced by the dispatcher.

    Returns:
        :class:`SetVariableActivity` on success, or :class:`UnsupportedValue` when
        ``variable_name`` is absent or the value expression is not supported.
    """
    variable_name = activity.get("variable_name")
    if not variable_name:
        return UnsupportedValue(
            value=activity,
            message="Missing value 'variable_name' for Set Variable activity",
        )

    raw_value = activity.get("value")
    if raw_value is None:
        return UnsupportedValue(
            value=activity,
            message="Missing value 'value' for Set Variable activity",
        )

    variable_value = parse_variable_value(raw_value)
    if isinstance(variable_value, UnsupportedValue):
        return variable_value

    return SetVariableActivity(
        **base_kwargs,
        variable_name=variable_name,
        variable_value=variable_value,
    )

import re

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue

# Supported @pipeline() system variables mapped to Python expressions
_PIPELINE_VARS: dict[str, str] = {
    "Pipeline": "spark.conf.get('spark.databricks.job.parentName', '')",
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('startTime', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
}
_SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES: set[str] = {"firstRow", "value"}
_ACTIVITY_OUTPUT_PATTERN = r"activity\(['\"]([^'\"]+)['\"]\)\.output\.([\w.]+)$"
_NAMED_VARIABLE_PATTERN = r"variables\(['\"]([^'\"]+)['\"]\)$"


def parse_variable_value(value: str | dict | int | float | bool, context: TranslationContext) -> str | UnsupportedValue:
    """
    Parses an ADF variable value or expression into a Python code snippet. Unsupported dynamic expressions return
    `UnsupportedValue`.

    The following cases are supported:

    * Static string values -> Python string literal (e.g. ``'hello'``).
    * Numeric / boolean literals -> Python literal (e.g. ``42``, ``True``).
    * Expressions (e.g. ``{"value": "@...", "type": "Expression"}``) -> inner expression is extracted and parsed.
    * Activity output references (e.g. ``@activity('X').output.Y``) -> ``dbutils.jobs.taskValues.get(taskKey='X', key='result')``.
    * Pipeline system variables (e.g. ``@pipeline().Pipeline`` or ``@pipeline().RunId``) -> ``spark.conf`` or ``dbutils.jobs.getContext()`` lookups.
    * Variables (e.g. ``@variables('X')``) -> ``dbutils.jobs.taskValues.get(taskKey='set_my_variable', key='X')``.

    Args:
        value: Variable value. Can be a plain string, a numeric/boolean literal, or an expression object with ``"type": "Expression"``.
        context: Translation context.

    Returns:
        A Python expression string suitable for embedding in a generated notebook, or an `UnsupportedValue` when the
        expression cannot be translated.
    """
    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(value=value, message=f"Unsupported variable value type '{value.get('type')}'")
        expression = value.get("value", "")
        if not expression:
            return UnsupportedValue(value=value, message="Missing property 'value' of expression")
        return _parse_expression_string(expression, context)

    if not isinstance(value, str):
        return repr(value)

    return _parse_expression_string(value, context)


def _parse_expression_string(expression: str, context: TranslationContext) -> str | UnsupportedValue:
    """
    Parses an expression string into a Python code snippet.

    Args:
        expression: ADF expression string.
        context: Translation context.

    Returns:
        Python expression string or :class:`UnsupportedValue`.
    """

    if not expression.startswith("@"):
        return repr(expression)

    expression = expression[1:].strip()
    if expression.startswith("{") and expression.endswith("}"):
        expression = expression[1:-1].strip()

    if match := re.match(_ACTIVITY_OUTPUT_PATTERN, expression):
        task_key, output_key = match.group(1), match.group(2)
        output_parts = output_key.split(".")
        output_root = output_parts[0]
        if output_root in _SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES:
            base = f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key='result')"
            if len(output_parts) > 1:
                property_path = output_parts[1:]
                accessors = "".join(f"[{p!r}]" for p in property_path)
                return f"json.loads({base}){accessors}"
            return base
        return UnsupportedValue(
            value=expression,
            message=f"Unsupported activity output reference type '@activity('{task_key}').output.{output_key}'",
        )

    if match := re.match(r"pipeline\(\)\.(\w+)$", expression):
        var_name = match.group(1)
        if var_name in _PIPELINE_VARS:
            return _PIPELINE_VARS[var_name]
        return UnsupportedValue(
            value=expression,
            message=f"Unsupported pipeline system variable '@pipeline().{var_name}'",
        )

    if match := re.match(_NAMED_VARIABLE_PATTERN, expression):
        variable_name = match.group(1)
        task_key = context.get_variable_task_key(variable_name)
        if task_key is not None:
            return f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key={variable_name!r})"
        return UnsupportedValue(
            value=expression,
            message=f"Variable '{variable_name}' not set by a previous activity",
        )

    return UnsupportedValue(
        value=expression,
        message=f"Unsupported expression '{expression}'",
    )

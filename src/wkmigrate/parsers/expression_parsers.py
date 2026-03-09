import re
from wkmigrate.models.ir.unsupported import UnsupportedValue

# Supported @pipeline() system variables mapped to Python expressions
_PIPELINE_VARS: dict[str, str] = {
    "Pipeline": "spark.conf.get('spark.databricks.job.parentName', '')",
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('startTime', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
}
_SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES: set[str] = {"firstRow", "value"}


def parse_variable_value(value: str | dict) -> str | UnsupportedValue:
    """
    Parses an ADF variable value or expression into a Python code snippet. Unsupported dynamic expressions return
    `UnsupportedValue`.

    The following cases are supported:

    * Static string (no leading ``@``) → Python string literal (e.g. ``'hello'``).
    * ADF expression object ``{"value": "@...", "type": "Expression"}`` → inner expression is extracted and parsed.
    * ``@activity('X').output.Y`` → ``dbutils.jobs.taskValues.get(taskKey='X', key='Y')``.
    * ``@pipeline().Pipeline`` / ``@pipeline().RunId`` / other supported system variables → ``spark.conf`` or
        ``dbutils.jobs.getContext()`` lookups.

    Args:
        value: ADF variable value. Can be a plain string or an expression object with ``"type": "Expression"``.

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
        return _parse_expression_string(str(expression))

    if isinstance(value, str):
        return _parse_expression_string(value)

    return UnsupportedValue(
        value=value,
        message=f"Unsupported variable type '{type(value).__name__}'",
    )


def _parse_expression_string(expression: str) -> str | UnsupportedValue:
    """
    Parses an expression string into a Python code snippet.

    Args:
        expression: ADF expression string, optionally prefixed with ``@``.

    Returns:
        Python expression string or :class:`UnsupportedValue`.
    """
    if not expression.startswith("@"):
        return repr(expression)

    expression = expression[1:].strip()
    if expression.startswith("{") and expression.endswith("}"):
        expression = expression[1:-1].strip()

    if match := re.match(r"activity\('([\w\s-]+)'\)\.output\.([\w.]+)", expression):
        task_key, output_key = match.group(1), match.group(2)
        if output_key in _SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES:
            return f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key='result')"
        return UnsupportedValue(
            value=expression,
            message=f"Unsupported activity output reference type '@activity({task_key!r}).output.{output_key!r}'",
        )

    if match := re.match(r"pipeline\(\)\.(\w+)$", expression):
        var_name = match.group(1)
        if var_name in _PIPELINE_VARS:
            return _PIPELINE_VARS[var_name]
        return UnsupportedValue(
            value=expression,
            message=f"Unsupported pipeline system variable '@pipeline().{var_name}'",
        )

    return UnsupportedValue(
        value=expression,
        message=f"Unsupported expression '{expression}'",
    )

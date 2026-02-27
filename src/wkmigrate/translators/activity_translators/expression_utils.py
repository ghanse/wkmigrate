"""Utilities for parsing ADF expression values into Python code snippets."""

from __future__ import annotations

import re

from wkmigrate.models.ir.unsupported import UnsupportedValue

# Supported @pipeline() system variables mapped to Python expressions
_PIPELINE_VARS: dict[str, str] = {
    "Pipeline": "spark.conf.get('spark.databricks.job.parentName', '')",
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('startTime', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
}


def parse_variable_value(value: str | dict) -> str | UnsupportedValue:
    """
    Parses an ADF variable value or expression into a Python code snippet.

    Handles the following cases:

    - Static string (no leading ``@``) → Python string literal (e.g. ``'hello'``).
    - ADF expression object ``{"value": "@...", "type": "Expression"}`` → inner
      expression is extracted and parsed.
    - ``@activity('X').output.Y`` → ``dbutils.jobs.taskValues.get(taskKey='X', key='Y')``.
    - ``@pipeline().Pipeline`` / ``@pipeline().RunId`` / other supported system
      variables → ``spark.conf`` or ``dbutils.jobs.getContext()`` lookups.
    - All other dynamic expressions → :class:`UnsupportedValue`.

    Args:
        value: Raw ADF variable value — either a plain string or an ADF expression
            object with ``"type": "Expression"``.

    Returns:
        A Python expression string suitable for embedding in a generated notebook,
        or an :class:`UnsupportedValue` when the expression cannot be translated.
    """
    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(value=value, message=f"Unsupported value type: {value.get('type')}")
        inner = value.get("value", "")
        if not inner:
            return UnsupportedValue(value=value, message="Empty ADF expression value")
        return _parse_expression_string(str(inner))

    if not isinstance(value, str):
        return UnsupportedValue(
            value=value,
            message=f"Unsupported variable value type '{type(value).__name__}'",
        )

    return _parse_expression_string(value)


def _parse_expression_string(expr: str) -> str | UnsupportedValue:
    """
    Parses an ADF expression string into a Python code snippet.

    Args:
        expr: ADF expression string, optionally prefixed with ``@``.

    Returns:
        Python expression string or :class:`UnsupportedValue`.
    """
    if not expr.startswith("@"):
        return repr(expr)

    # Strip the leading '@' and optional surrounding braces
    inner = expr[1:].strip()
    if inner.startswith("{") and inner.endswith("}"):
        inner = inner[1:-1].strip()

    # @activity('X').output.Y
    # TODO: Known limitation: @activity(...).output.<key> references assume the upstream task
    # publishes that exact key. The Lookup preparer publishes under key="result", so
    # @activity('X').output.firstRow will fail at runtime. Consider mapping well-known
    # output paths or returning UnsupportedValue for @activity references until resolved.
    if match := re.match(r"activity\('([\w\s-]+)'\)\.output\.([\w.]+)", inner):
        task_key, output_key = match.group(1), match.group(2)
        return f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key={output_key!r})"

    # @pipeline().<VarName>
    if match := re.match(r"pipeline\(\)\.(\w+)$", inner):
        var_name = match.group(1)
        if var_name in _PIPELINE_VARS:
            return _PIPELINE_VARS[var_name]
        return UnsupportedValue(
            value=expr,
            message=f"Unsupported pipeline system variable '@pipeline().{var_name}'",
        )

    return UnsupportedValue(
        value=expr,
        message=f"Unsupported ADF expression '{expr}'",
    )

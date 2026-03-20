"""
This module defines a preparer for Lookup activities.

The preparer builds a Databricks notebook task that reads data from a dataset
using Spark (either a native file source or a database via JDBC), optionally
limits the result to the first row, collects the rows, and publishes them as
a Databricks task value using ``dbutils.jobs.taskValues.set()``.
"""

from __future__ import annotations

import autopep8  # type: ignore

from wkmigrate.parsers.dataset_parsers import collect_data_source_secrets, merge_dataset_definition
from wkmigrate.code_generator import get_option_expressions, get_read_expression
from wkmigrate.models.ir.pipeline import LookupActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_lookup_activity(activity: LookupActivity) -> PreparedActivity:
    """
    Builds tasks and artifacts for a Lookup activity.

    The resulting notebook:

    1. Configures credentials and read options.
    2. Reads the source data with Spark.
    3. Optionally limits the result to the first row (``first_row_only``).
    4. Collects the rows and publishes them as a task value via
       ``dbutils.jobs.taskValues.set()``.

    Args:
        activity: Activity definition emitted by the translators.

    Returns:
        PreparedActivity containing the notebook task configuration and artifacts.
    """
    source_definition = merge_dataset_definition(activity.source_dataset, activity.source_properties)

    data_source_secrets = collect_data_source_secrets(source_definition)
    secrets_to_collect = data_source_secrets if data_source_secrets else None

    notebook_path, notebook = _create_lookup_notebook(
        source_definition=source_definition,
        first_row_only=activity.first_row_only,
        source_query=activity.source_query,
    )

    base_task = get_base_task(activity)
    task = parse_mapping(
        {
            **base_task,
            "notebook_task": {"notebook_path": notebook_path},
        }
    )

    return PreparedActivity(
        task=task,
        notebooks=[notebook],
        secrets=secrets_to_collect,
    )


def _create_lookup_notebook(
    source_definition: dict,
    first_row_only: bool,
    source_query: str | None,
) -> tuple[str, NotebookArtifact]:
    """
    Generates a Python notebook that reads data with Spark and sets a task value.

    Args:
        source_definition: Merged source dataset + properties dictionary.
        first_row_only: Whether to limit results to the first row.
        source_query: Optional SQL query for database sources.

    Returns:
        Tuple of ``(notebook_path, NotebookArtifact)``.
    """
    script_lines = [
        "# Databricks notebook source",
        "import json",
        "",
        "# Set the source options:",
    ]
    script_lines.extend(get_option_expressions(source_definition))

    script_lines.append("")
    script_lines.append("# Read from the source:")
    script_lines.append(get_read_expression(source_definition, source_query))

    script_lines.append("")
    if first_row_only:
        script_lines.append("# Collect first row only:")
        script_lines.append(_get_first_row_collect(source_definition))
    else:
        script_lines.append("# Collect all rows:")
        script_lines.append(_get_all_rows_collect(source_definition))

    script_lines.append("")
    script_lines.append("# Publish as a Databricks task value:")
    script_lines.append(_get_task_value_expression(first_row_only))

    notebook_content = autopep8.fix_code("\n".join(script_lines))
    source_dataset_name = source_definition.get("dataset_name", "unnamed_dataset")
    notebook_path = f"/wkmigrate/lookup_notebooks/lookup_{source_dataset_name}"
    notebook_artifact = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    return notebook_path, notebook_artifact


def _get_first_row_collect(source_definition: dict) -> str:
    """
    Produces a Spark expression that collects only the first row from the source DataFrame.

    Args:
        source_definition: Flat dataset definition dictionary.

    Returns:
        Python source fragment that limits the DataFrame to one row and collects the result.
    """
    source_name = source_definition.get("dataset_name")
    return f"result_rows = [row.asDict() for row in {source_name}_df.limit(1).collect()]\n"


def _get_all_rows_collect(source_definition: dict) -> str:
    """
    Produces a Spark expression that collects all rows from the source DataFrame.

    Args:
        source_definition: Flat dataset definition dictionary.

    Returns:
        Python source fragment that collects all rows from the DataFrame.
    """
    source_name = source_definition.get("dataset_name")
    return f"result_rows = [row.asDict() for row in {source_name}_df.collect()]\n"


def _get_task_value_expression(first_row_only: bool) -> str:
    """
    Generates ``dbutils.jobs.taskValues.set()`` calls for the lookup result.

    The task value is published under the key ``"result"`` so downstream tasks
    can reference it with ``{{tasks.<task_key>.values.result}}``.

    Values are converted to strings to satisfy the 48 KiB JSON constraint
    imposed by Databricks task values.

    Args:
        first_row_only: Whether the lookup returns only the first row.

    Returns:
        Python source fragment that publishes the lookup output as a task value.
    """
    lines: list[str] = []
    if first_row_only:
        lines.append("lookup_output = {k: str(v) for k, v in (result_rows[0].items() if result_rows else {}.items())}")
    else:
        lines.append("lookup_output = [{k: str(v) for k, v in row.items()} for row in result_rows]")
    lines.append('dbutils.jobs.taskValues.set(key="result", value=json.dumps(lookup_output))')
    return "\n".join(lines) + "\n"

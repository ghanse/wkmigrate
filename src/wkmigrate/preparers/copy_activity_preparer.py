"""
This module defines a preparer for Copy activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Copy activity. This includes a notebook or pipeline task
definition, notebook artifacts, and secrets to be created in the target workspace.
"""

from __future__ import annotations

from dataclasses import asdict

import autopep8  # type: ignore

from wkmigrate.parsers.dataset_parsers import (
    collect_data_source_secrets,
    merge_dataset_definition,
    parse_spark_data_type,
)
from wkmigrate.code_generator import (
    get_file_uri,
    get_option_expressions,
    get_read_expression,
)
from wkmigrate.models.ir.pipeline import CopyActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.models.workflows.instructions import PipelineInstruction
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_copy_activity(
    activity: CopyActivity,
    default_files_to_delta_sinks: bool | None,
) -> PreparedActivity:
    """
    Builds tasks and artifacts for a Copy activity.

    Args:
        activity: Activity definition emitted by the translators.
        default_files_to_delta_sinks: Optional override for DLT generation.

    Returns:
        PreparedActivity containing task configuration and artifacts.
    """
    source_definition = merge_dataset_definition(activity.source_dataset, activity.source_properties)
    sink_definition = merge_dataset_definition(activity.sink_dataset, activity.sink_properties)
    column_mapping = [asdict(mapping) for mapping in (activity.column_mapping or [])]
    if not column_mapping:
        raise ValueError("No column mapping provided for copy data task")

    data_source_secrets = collect_data_source_secrets(source_definition)
    data_sink_secrets = collect_data_source_secrets(sink_definition)
    secrets_to_collect = data_source_secrets + data_sink_secrets

    files_to_delta_sinks = sink_definition.get("type") == "delta"
    if default_files_to_delta_sinks is not None:
        files_to_delta_sinks = default_files_to_delta_sinks

    notebook_path, notebook = _create_copy_data_notebook(
        source_definition,
        sink_definition,
        column_mapping,
        files_to_delta_sinks,
    )

    base_task = get_base_task(activity)

    if not files_to_delta_sinks:
        # Standard notebook execution
        task = parse_mapping(
            {
                **base_task,
                "notebook_task": {"notebook_path": notebook_path},
            }
        )
        return PreparedActivity(
            task=task,
            notebooks=[notebook],
            secrets=secrets_to_collect if secrets_to_collect else None,
        )

    # DLT pipeline execution - pipeline_id will be resolved later
    pipeline_name = f"{activity.task_key}_pipeline"
    task = parse_mapping(
        {
            **base_task,
            "pipeline_task": {"pipeline_id": "__PIPELINE_ID__"},
        }
    )
    return PreparedActivity(
        task=task,
        notebooks=[notebook],
        secrets=secrets_to_collect if secrets_to_collect else None,
        pipelines=[
            PipelineInstruction(
                task_ref=task,
                file_path=notebook.file_path,
                name=pipeline_name,
            )
        ],
    )


def _create_copy_data_notebook(
    source_definition: dict,
    sink_definition: dict,
    column_mapping: list[dict],
    files_to_delta_sinks: bool,
) -> tuple[str, NotebookArtifact]:
    """
    Generates a Python notebook that copies data between datasets.

    Args:
        source_definition: Merged source dataset definition dictionary.
        sink_definition: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.
        files_to_delta_sinks: Whether to generate a DLT materialised-view definition.

    Returns:
        Tuple of ``(notebook_path, NotebookArtifact)``.
    """
    script_lines = [
        "# Databricks notebook source",
        "import pyspark.sql.types as T",
        "import pyspark.sql.functions as F",
        "",
        "# Set the source options:",
    ]
    script_lines.extend(get_option_expressions(source_definition))
    if not files_to_delta_sinks:
        script_lines.append("# Set the target options:")
        script_lines.extend(get_option_expressions(sink_definition))
        script_lines.append("# Read from the source:")
        script_lines.append(get_read_expression(source_definition))
        script_lines.append("# Map the source columns to the target columns:")
        script_lines.append(_get_mapping(source_definition, sink_definition, column_mapping, True))
        script_lines.append("# Write to the target:")
        script_lines.append(_get_write_expression(sink_definition))
    else:
        script_lines.append("# Load the data with DLT as a materialized view:")
        script_lines.append(
            _get_dlt_definition(
                source_definition,
                sink_definition,
                column_mapping,
            )
        )
    notebook_content = autopep8.fix_code("\n".join(script_lines))
    source_dataset_name = source_definition.get("dataset_name")
    sink_dataset_name = sink_definition.get("dataset_name")
    notebook_path = f"/wkmigrate/copy_data_notebooks/copy_{source_dataset_name}_to_{sink_dataset_name}"
    notebook_artifact = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    return notebook_path, notebook_artifact


def _get_dlt_definition(source_dataset: dict, sink_dataset: dict, column_mapping: list[dict]) -> str:
    """
    Generates a DLT materialised-view definition for a copy activity.

    Args:
        source_dataset: Merged source dataset definition dictionary.
        sink_dataset: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.

    Returns:
        Python source fragment defining a DLT table.
    """
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    return f"""@dlt.table(
                        name="{sink_name}",
                        comment="Data copied from {source_name}; Previously targeted {sink_name}."
                        tbl_properties={{'delta.createdBy.wkmigrate': 'true'}}
                    )
                    def {sink_name}():
                        {get_read_expression(source_dataset)}
                        {_get_mapping(source_dataset, sink_dataset, column_mapping, True)}
                        return {sink_name}_df
                """


def _get_mapping(
    source_dataset: dict,
    sink_dataset: dict,
    column_mapping: list[dict],
    cast_column_types: bool,
) -> str:
    """
    Generates a ``selectExpr`` statement that maps source columns to sink columns.

    Args:
        source_dataset: Merged source dataset definition dictionary.
        sink_dataset: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.
        cast_column_types: Whether to wrap each expression in a ``CAST``.

    Returns:
        Python source fragment that maps a source DataFrame to a sink DataFrame.
    """
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    expressions = []
    for mapping in column_mapping:
        source_col = mapping["source_column_name"]
        sink_col = mapping["sink_column_name"]
        sink_type = parse_spark_data_type(mapping["sink_column_type"], sink_dataset["type"])
        if cast_column_types:
            expressions.append(f'"cast({source_col} as {sink_type}) as {sink_col}"')
        else:
            expressions.append(f'"{source_col} as {sink_col}"')
    newline_characters = ", \n\t"
    return f"{sink_name}_df = {source_name}_df.selectExpr(\n\t{newline_characters.join(expressions)}\n)"


def _get_write_expression(sink_definition: dict) -> str:
    """
    Generates a Spark write statement for the sink dataset.

    Args:
        sink_definition: Merged sink dataset definition dictionary.

    Returns:
        Python source fragment that writes a DataFrame to the sink.

    Raises:
        ValueError: If the sink type is not supported for writing.
    """
    sink_name = sink_definition.get("dataset_name")
    sink_type = sink_definition.get("type")
    if sink_type == "avro":
        return rf"""{sink_name}_df.write.format("avro")  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "csv":
        return rf"""{sink_name}_df.write.format("csv")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "delta":
        database_name = sink_definition.get("database_name")
        table_name = sink_definition.get("table_name")
        return rf"""{sink_name}_df.write.format("delta")  \
                        .mode("overwrite")  \
                        .saveAsTable("hive_metastore.{database_name}.{table_name}")
                    """
    if sink_type == "json":
        return rf"""{sink_name}_df.write.format("json")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "orc":
        return rf"""{sink_name}_df.write.format("orc")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "parquet":
        return rf"""{sink_name}_df.write.format("parquet")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type in {"sqlserver", "postgresql", "mysql", "oracle"}:
        return rf"""{sink_name}_df.write.format("jdbc")  \
                        .options(**{sink_name}_options)  \
                        .save()
                    """
    raise ValueError(f'Writing data to "{sink_type}" not supported')

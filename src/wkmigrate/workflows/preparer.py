"""Utilities for preparing Databricks workflow artifacts."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

import autopep8  # type: ignore

from wkmigrate.datasets import options, secrets
from wkmigrate.datasets.data_type_mapping import parse_spark_data_type
from wkmigrate.models.ir.activities import (
    Activity,
    CopyActivity,
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    SparkJarActivity,
    SparkPythonActivity,
)
from wkmigrate.models.ir.datasets import Dataset, DatasetProperties
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedWorkflow
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction

PreparedTaskResult = tuple[list[NotebookArtifact], list[PipelineInstruction], list[SecretInstruction]]


def prepare_workflow(pipeline_definition: Pipeline, files_to_delta_sinks: bool | None = None) -> PreparedWorkflow:
    """
    Translates a pipeline definition into notebook, pipeline, and secret artifacts.

    Args:
        pipeline_definition: Parsed pipeline IR produced by the translator.
        files_to_delta_sinks: Overrides the inferred Files-to-Delta behavior when set.

    Returns:
        Prepared workflow containing the Databricks job payload and supporting artifacts.
    """
    tasks = [_activity_to_task_dict(task.activity) for task in pipeline_definition.tasks]
    job_settings = {
        "name": pipeline_definition.name,
        "parameters": pipeline_definition.parameters,
        "schedule": pipeline_definition.schedule,
        "tags": pipeline_definition.tags,
        "tasks": tasks,
        "not_translatable": list(pipeline_definition.not_translatable),
    }

    notebooks, pipelines, secrets_to_collect = _prepare_tasks(tasks, files_to_delta_sinks)
    unsupported = list(pipeline_definition.not_translatable)
    return PreparedWorkflow(
        job_settings=job_settings,
        notebooks=notebooks,
        pipelines=pipelines,
        secrets=secrets_to_collect,
        unsupported=unsupported,
    )


def _activity_to_task_dict(activity: Activity) -> dict:
    """
    Converts an activity IR object into a Databricks task definition.

    Args:
        activity: Activity instance emitted by the translator.

    Returns:
        dict: Databricks Jobs task configuration.
    """
    task = _get_base_task(activity)
    if isinstance(activity, DatabricksNotebookActivity):
        task["notebook_task"] = {
            "notebook_path": activity.notebook_path,
            "base_parameters": activity.base_parameters,
        }
    if isinstance(activity, SparkJarActivity):
        task["spark_jar_task"] = {
            "main_class_name": activity.main_class_name,
            "parameters": activity.parameters,
            "libraries": activity.libraries,
        }
    if isinstance(activity, SparkPythonActivity):
        task["spark_python_task"] = {
            "python_file": activity.python_file,
            "parameters": activity.parameters,
        }
    if isinstance(activity, IfConditionActivity):
        task["condition_task"] = {
            "op": activity.op,
            "left": activity.left,
            "right": activity.right,
        }
    if isinstance(activity, ForEachActivity):
        task["for_each_task"] = {
            "inputs": activity.items_string,
            "concurrency": activity.concurrency,
            "task": [_activity_to_task_dict(inner) for inner in activity.inner_activities],
        }
    if isinstance(activity, CopyActivity):
        task["copy_data_task"] = {
            "source_dataset": activity.source_dataset,
            "sink_dataset": activity.sink_dataset,
            "source_properties": activity.source_properties,
            "sink_properties": activity.sink_properties,
            "column_mapping": [asdict(mapping) for mapping in (activity.column_mapping or [])],
        }
    return task


def _get_base_task(activity: Activity) -> dict:
    """
    Returns the fields common to every task.

    Args:
        activity: Activity instance emitted by the translator.

    Returns:
        Dictionary containing the common task fields.
    """
    return {
        "task_key": activity.task_key,
        "type": activity.activity_type,
        "description": activity.description,
        "timeout_seconds": activity.timeout_seconds,
        "max_retries": activity.max_retries,
        "min_retry_interval_millis": activity.min_retry_interval_millis,
        "depends_on": [asdict(dep) for dep in activity.depends_on] if activity.depends_on else None,
        "new_cluster": activity.new_cluster,
    }


def _prepare_tasks(
    tasks: list[dict],
    default_files_to_delta_sinks: bool | None,
) -> PreparedTaskResult:
    """
    Analyzes the job graph and collects notebook, pipeline, and secret artifacts.

    Args:
        tasks: List of Databricks task definitions.
        default_files_to_delta_sinks: Optional override for DLT generation.

    Returns:
        Tuple containing notebooks, pipelines, and secrets to materialize.
    """
    output_notebooks: list[NotebookArtifact] = []
    output_pipelines: list[PipelineInstruction] = []
    output_secrets: list[SecretInstruction] = []
    for task in tasks:
        task_type = task.get("type")
        if task_type == "Copy":
            copy_activity_notebooks, copy_activity_pipelines, copy_activity_secrets = _prepare_copy_task(
                task, default_files_to_delta_sinks
            )
            output_notebooks.extend(copy_activity_notebooks)
            output_pipelines.extend(copy_activity_pipelines)
            output_secrets.extend(copy_activity_secrets)
        elif task_type == "ForEach":
            foreach_activity_notebooks, foreach_activity_pipelines, foreach_activity_secrets = _prepare_for_each_task(
                task, default_files_to_delta_sinks
            )
            output_notebooks.extend(foreach_activity_notebooks)
            output_pipelines.extend(foreach_activity_pipelines)
            output_secrets.extend(foreach_activity_secrets)
    return output_notebooks, output_pipelines, output_secrets


def _prepare_for_each_task(
    task: dict,
    default_files_to_delta_sinks: bool | None,
) -> PreparedTaskResult:
    """
    Prepares nested tasks that belong to a ForEach activity.

    Args:
        task: Databricks task dictionary representing the parent ForEach.
        default_files_to_delta_sinks: Optional override for DLT generation.

    Returns:
        Tuple containing notebooks, pipelines, and secrets to materialize.

    Raises:
        ValueError: If the ForEach task is not found.
    """
    for_each_task = task.get("for_each_task")
    if for_each_task is None:
        raise ValueError('No "for_each_task" value for for each task type')
    inner_tasks = for_each_task.get("task")
    if isinstance(inner_tasks, list):
        return _prepare_tasks(inner_tasks, default_files_to_delta_sinks)
    if isinstance(inner_tasks, dict):
        return _prepare_tasks([inner_tasks], default_files_to_delta_sinks)
    return [], [], []


def _prepare_copy_task(
    task: dict,
    default_files_to_delta_sinks: bool | None,
) -> PreparedTaskResult:
    """
    Prepares artifacts for a Copy activity.

    Args:
        task: Databricks task dictionary representing the copy activity.
        default_files_to_delta_sinks: Optional override for DLT generation.

    Returns:
        Tuple containing notebooks, pipelines, and secrets to materialize.

    Raises:
        ValueError: If the copy data task is not found.
        ValueError: If the column mapping is not found.
    """
    copy_data_task = task.get("copy_data_task")
    if copy_data_task is None:
        raise ValueError("No 'copy_data_task' found in task with type 'Copy'")

    source_definition = _merge_dataset_definition(
        copy_data_task.get("source_dataset"),
        copy_data_task.get("source_properties"),
    )
    sink_definition = _merge_dataset_definition(
        copy_data_task.get("sink_dataset"),
        copy_data_task.get("sink_properties"),
    )
    column_mapping = copy_data_task.get("column_mapping")
    if column_mapping is None:
        raise ValueError("No column mapping provided for copy data task")

    data_source_secrets = _collect_data_source_secrets(source_definition)
    data_sink_secrets = _collect_data_source_secrets(sink_definition)
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
    notebooks = [notebook]
    task.pop("copy_data_task", None)
    if not files_to_delta_sinks:
        task["notebook_task"] = {"notebook_path": notebook_path}
        return notebooks, [], secrets_to_collect

    pipeline_name = f'{task.get("task_key")}_pipeline'
    task["pipeline_task"] = {"pipeline_id": None}
    pipeline_instruction = PipelineInstruction(
        task_ref=task,
        file_path=notebook_path,
        name=pipeline_name,
    )
    return notebooks, [pipeline_instruction], secrets_to_collect


def _merge_dataset_definition(dataset: Dataset | dict | None, properties: DatasetProperties | dict | None) -> dict:
    """
    Merges dataset metadata with parsed dataset properties.

    Args:
        dataset: Dataset IR instance or legacy dictionary.
        properties: Dataset properties IR instance or legacy dictionary.

    Returns:
        Combined dataset definition as a ``dict`` understood by the notebook generator.

    Raises:
        ValueError: If the dataset definition or properties are missing.
    """
    if dataset is None or properties is None:
        raise ValueError("Dataset definition or properties missing for copy task")
    dataset_dict = _dataset_to_dict(dataset)
    properties_dict = _dataset_properties_to_dict(properties)
    return {**dataset_dict, **properties_dict}


def _dataset_to_dict(dataset: Dataset | dict) -> dict:
    """
    Converts a dataset IR object into a serializable dictionary.

    Args:
        dataset: Dataset IR instance or raw dictionary.

    Returns:
        Normalized dictionary representation as a ``dict``.
    """
    if isinstance(dataset, dict):
        return dataset
    if is_dataclass(dataset):
        dataset_dict = asdict(dataset)
        dataset_type_value = dataset_dict.pop("dataset_type", None)
        if dataset_type_value is not None:
            dataset_dict["type"] = dataset_type_value
        format_options = dataset_dict.pop("format_options", None)
        if isinstance(format_options, dict):
            dataset_dict.update(_filter_none_dict(format_options))
        connection_options = dataset_dict.pop("connection_options", None)
        if isinstance(connection_options, dict):
            dataset_dict.update(_filter_none_dict(connection_options))
        return _filter_none_dict(dataset_dict)
    return {}


def _dataset_properties_to_dict(properties: DatasetProperties | dict | None) -> dict:
    """
    Converts dataset properties IR objects into dictionaries.

    Args:
        properties: Dataset properties IR instance or raw dictionary.

    Returns:
        Normalized dictionary representation as a ``dict``.
    """
    if properties is None:
        return {}
    if isinstance(properties, dict):
        return properties
    values = {"type": properties.dataset_type}
    values.update(_filter_none_dict(properties.options))
    return values


def _collect_data_source_secrets(definition: dict) -> list[SecretInstruction]:
    """
    Converts dataset secret references into ``SecretInstruction`` objects.

    Args:
        definition: Dataset metadata merged by ``_merge_dataset_definition``.

    Returns:
        List of secrets that must exist in Databricks as a ``list[SecretInstruction]``.
    """
    service_type = definition.get("type")
    service_name = definition.get("service_name")
    if service_type is None or service_name is None:
        return []
    collected: list[SecretInstruction] = []
    for secret in secrets.get(service_type, []):
        value = definition.get(secret)
        instruction = SecretInstruction(
            scope="wkmigrate_credentials_scope",
            key=f"{service_name}_{secret}",
            service_name=service_name,
            service_type=service_type,
            provided_value=value,
            user_input_required=value is None,
        )
        collected.append(instruction)
    return collected


def _create_copy_data_notebook(
    source_definition: dict,
    sink_definition: dict,
    column_mapping: dict,
    files_to_delta_sinks: bool,
) -> tuple[str, NotebookArtifact]:
    """
    Generates a notebook script for a Copy activity.

    Args:
        source_definition: Resolved source dataset definition.
        sink_definition: Resolved sink dataset definition.
        column_mapping: Column mapping metadata.
        files_to_delta_sinks: Whether to emit a DLT pipeline instead of a notebook task.

    Returns:
        Notebook workspace path and the artifact to upload as a ``tuple[str, NotebookArtifact]``.
    """
    script_lines = [
        "# Databricks notebook source",
        "import pyspark.sql.types as T",
        "import pyspark.sql.functions as F",
        "",
        "# Set the source options:",
    ]
    script_lines.extend(_get_option_expressions(source_definition))
    if not files_to_delta_sinks:
        script_lines.append("# Set the target options:")
        script_lines.extend(_get_option_expressions(sink_definition))
        script_lines.append("# Read from the source:")
        script_lines.append(_get_read_expression(source_definition))
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


def _get_dlt_definition(source_dataset: dict, sink_dataset: dict, column_mapping: dict) -> str:
    """
    Returns a templated DLT table definition for copy activities.

    Args:
        source_dataset: Resolved source dataset definition.
        sink_dataset: Resolved sink dataset definition.
        column_mapping: Column mapping metadata.

    Returns:
        Templated DLT table definition as a ``str``.
    """
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    return f"""@dlt.table(
                        name="{sink_name}",
                        comment="Data copied from {source_name}; Previously targeted {sink_name}."
                        tbl_properties={{'delta.createdBy.wkmigrate': 'true'}}
                    )
                    def {sink_name}:
                        {_get_read_expression(source_dataset)}
                        {_get_mapping(source_dataset, sink_dataset, column_mapping, True)}
                        return {sink_name}_df
                """


def _get_mapping(source_dataset: dict, sink_dataset: dict, column_mapping: dict, cast_column_types: bool) -> str:
    """
    Returns the SQL expression that performs column mapping between datasets.

    Args:
        source_dataset: Resolved source dataset definition.
        sink_dataset: Resolved sink dataset definition.
        column_mapping: Column mapping metadata.
        cast_column_types: Whether to cast column types.

    Returns:
        SQL expression that performs column mapping between datasets as a ``str``.
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
    Returns the PySpark expression that writes the transformed DataFrame.

    Args:
        sink_definition: Resolved sink dataset definition.

    Returns:
        PySpark expression that writes the transformed DataFrame as a ``str``.

    Raises:
        ValueError: If the sink dataset type is not supported.
    """
    sink_name = sink_definition.get("dataset_name")
    sink_type = sink_definition.get("type")
    if sink_type == "avro":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("avro")  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "csv":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("csv")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "delta":
        database_name = sink_definition.get("database_name")
        table_name = sink_definition.get("table_name")
        return rf"""{sink_name}_df.write.format("delta")  \
                        .mode("overwrite")  \
                        .saveAsTable("hive_metastore.{database_name}.{table_name}")
                    """
    if sink_type == "json":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("json")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "orc":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("orc")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "parquet":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("parquet")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "sqlserver":
        return rf"""{sink_name}_df.write.format("jdbc")  \
                        .options(**{sink_name}_options)  \
                        .save()
                    """
    raise ValueError(f'Writing data to "{sink_type}" not supported')


def _get_read_expression(source_definition: dict) -> str:
    """
    Returns the PySpark expression that reads the source dataset.

    Args:
        source_definition: Resolved source dataset definition.

    Returns:
        PySpark expression that reads the source dataset as a ``str``.

    Raises:
        ValueError: If the source dataset type is not supported.
    """
    source_name = source_definition.get("dataset_name")
    source_type = source_definition.get("type")
    if source_type == "avro":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("avro")
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    )
                    """
    if source_type == "csv":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("csv")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "delta":
        database_name = source_definition.get("database_name")
        table_name = source_definition.get("table_name")
        return f'{source_name}_df = spark.read.table("hive_metastore.{database_name}.{table_name}'
    if source_type == "json":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("json")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "orc":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("orc")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "parquet":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("parquet")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "sqlserver":
        schema_name = source_definition.get("schema_name")
        table_name = source_definition.get("table_name")
        return f"""{source_name}_df = ( 
                    spark.read.format("sqlserver")
                        .options(**{source_name}_options)
                        .option("dbtable", "{schema_name}.{table_name}")
                        .load()
                    )
                    """
    raise ValueError(f'Reading data from "{source_type}" not supported')


def _get_option_expressions(dataset_definition: dict) -> list[str]:
    """
    Returns notebook snippets that configure dataset-specific Spark options.

    Args:
        dataset_definition: Resolved dataset definition.

    Returns:
        List of notebook snippets as a ``list[str]``.
    """
    dataset_type = dataset_definition.get("type")
    if dataset_type in {"avro", "csv", "json", "orc", "parquet"}:
        return _get_file_options(dataset_definition, dataset_type)
    if dataset_type == "sqlserver":
        return _get_database_options(dataset_definition, dataset_type)
    return []


def _get_file_options(dataset_definition: dict, file_type: str) -> list[str]:
    """
    Returns Spark configuration snippets for file-based datasets.

    Args:
        dataset_definition: Resolved dataset definition.
        file_type: File type.

    Returns:
        List of Spark configuration snippets as a ``list[str]``.
    """
    dataset_name = dataset_definition.get("dataset_name")
    service_name = dataset_definition.get("service_name")
    config_lines = [
        rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
        for option in options.get(file_type, [])
        if dataset_definition.get(option)
    ]
    if "records_per_file" in dataset_definition:
        records_per_file = dataset_definition.get("records_per_file")
        config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
    config_lines.append(
        f"""spark.conf.set(
                "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                    dbutils.secrets.get(
                        scope="wkmigrate_credentials_scope", 
                        key="{service_name}_storage_account_key"
                )
            )
            """
    )
    return [f"{dataset_name}_options = {{}}", *config_lines]


def _get_database_options(dataset_definition: dict, database_type: str) -> list[str]:
    """
    Returns Spark configuration snippets for JDBC datasets.

    Args:
        dataset_definition: Resolved dataset definition.
        database_type: Database type.

    Returns:
        List of Spark configuration snippets as a ``list[str]``.
    """
    dataset_name = dataset_definition.get("dataset_name")
    service_name = dataset_definition.get("service_name")
    secrets_lines = [
        f"""{dataset_name}_options["{secret}"] = dbutils.secrets.get(
                scope="wkmigrate_credentials_scope", 
                key="{service_name}_{secret}"
            )
            """
        for secret in secrets[database_type]
    ]
    options_lines = [
        f"""{dataset_name}_options["{option}"] = '{dataset_definition.get(option)}'"""
        for option in options[database_type]
    ]
    return [f"{dataset_name}_options = {{}}", *secrets_lines, *options_lines]


def _filter_none_dict(values: dict[str, Any] | None) -> dict[str, Any]:
    """
    Removes ``None`` values from a dictionary.

    Args:
        values: Dictionary to filter.

    Returns:
        Dictionary with ``None`` values removed as a ``dict[str, Any]``.
    """
    if values is None:
        return {}
    return {key: value for key, value in values.items() if value is not None}

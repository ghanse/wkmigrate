"""
This module defines a preparer for creating Databricks Lakeflow jobs from an ADF
pipeline which has been translated with wkmigrate.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed
to replicate the pipeline's functionality. This includes job settings, task definitions,
notebooks, pipelines, and secrets to be created in the target workspace.
"""

from __future__ import annotations
from typing import Any

from wkmigrate.models.ir.pipeline import (
    Activity,
    CopyActivity,
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
    WebActivity,
)
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity, PreparedWorkflow
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
from wkmigrate.preparers.for_each_activity_preparer import prepare_for_each_activity
from wkmigrate.preparers.if_condition_activity_preparer import prepare_if_condition_activity
from wkmigrate.preparers.lookup_activity_preparer import prepare_lookup_activity
from wkmigrate.preparers.notebook_activity_preparer import prepare_notebook_activity
from wkmigrate.preparers.run_job_activity_preparer import prepare_run_job_activity
from wkmigrate.preparers.spark_jar_activity_preparer import prepare_spark_jar_activity
from wkmigrate.preparers.spark_python_activity_preparer import prepare_spark_python_activity
from wkmigrate.preparers.web_activity_preparer import prepare_web_activity


def prepare_workflow(pipeline: Pipeline, files_to_delta_sinks: bool | None = None) -> PreparedWorkflow:
    """
    Prepares a pipeline internal representation for creation as a Databricks Lakeflow job.

    Args:
        pipeline: Pipeline internal representation to prepare.
        files_to_delta_sinks: Overrides the inferred Files-to-Delta behavior when set.

    Returns:
        Prepared workflow containing the Databricks job payload and supporting artifacts for the pipeline.
    """
    prepared_activities = prepare_activities(pipeline.tasks, files_to_delta_sinks)

    tasks: list[dict[str, Any]] = []
    notebooks: list[NotebookArtifact] = []
    pipelines: list[PipelineInstruction] = []
    secrets: list[SecretInstruction] = []
    inner_jobs: list[dict[str, Any]] = []

    for activity, workflow in prepared_activities:
        tasks.append(activity.task)
        if activity.notebooks:
            notebooks.extend(activity.notebooks)
        if activity.pipelines:
            pipelines.extend(activity.pipelines)
        if activity.secrets:
            secrets.extend(activity.secrets)
        if activity.inner_jobs:
            inner_jobs.extend(activity.inner_jobs)
        if workflow:
            inner_jobs.append(workflow.job_settings)

    job_settings = {
        "name": pipeline.name,
        "parameters": pipeline.parameters,
        "schedule": pipeline.schedule,
        "tags": pipeline.tags,
        "tasks": tasks,
        "not_translatable": list(pipeline.not_translatable),
        "inner_jobs": inner_jobs,
    }

    return PreparedWorkflow(
        job_settings=job_settings,
        notebooks=notebooks if notebooks else None,
        pipelines=pipelines if pipelines else None,
        secrets=secrets if secrets else None,
        unsupported=list(pipeline.not_translatable),
        inner_jobs=inner_jobs,
    )


def prepare_activities(
    activities: list[Activity],
    default_files_to_delta_sinks: bool | None,
) -> list[tuple[PreparedActivity, PreparedWorkflow | None]]:
    """
    Prepares a list of activity internal representations for creation as Databricks Lakeflow job tasks.

    Args:
        activities: List of activity internal representations to prepare.
        default_files_to_delta_sinks: Whether to use the default files-to-delta sinks behavior.

    Returns:
        List of tuples containing the prepared activity and workflow for each activity internal representation.
    """
    return [prepare_activity(activity, default_files_to_delta_sinks) for activity in activities]


def prepare_activity(
    activity: Activity,
    default_files_to_delta_sinks: bool | None,
) -> tuple[PreparedActivity, PreparedWorkflow | None]:
    """
    Prepares an activity internal representation for creation as a Databricks Lakeflow job task.

    Args:
        activity: Activity internal representation to prepare.
        default_files_to_delta_sinks: Whether to use the default files-to-delta sinks behavior.

    Returns:
        A tuple containing the prepared activity and workflow for the activity.
    """
    if isinstance(activity, DatabricksNotebookActivity):
        return prepare_notebook_activity(activity), None
    if isinstance(activity, SparkJarActivity):
        return prepare_spark_jar_activity(activity), None
    if isinstance(activity, SparkPythonActivity):
        return prepare_spark_python_activity(activity), None
    if isinstance(activity, IfConditionActivity):
        return prepare_if_condition_activity(activity), None
    if isinstance(activity, ForEachActivity):
        return prepare_for_each_activity(activity, default_files_to_delta_sinks)
    if isinstance(activity, RunJobActivity):
        return prepare_run_job_activity(activity, default_files_to_delta_sinks)
    if isinstance(activity, CopyActivity):
        return prepare_copy_activity(activity, default_files_to_delta_sinks), None
    if isinstance(activity, LookupActivity):
        return prepare_lookup_activity(activity), None
    if isinstance(activity, WebActivity):
        return prepare_web_activity(activity), None
    raise ValueError(f"Unsupported activity type '{type(activity)}'")

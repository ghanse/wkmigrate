"""
This module defines a preparer for creating Databricks Lakeflow jobs from an ADF
pipeline which has been translated with wkmigrate.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed
to replicate the pipeline's functionality. This includes job settings, task definitions,
notebooks, pipelines, and secrets to be created in the target workspace.
"""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import (
    Activity,
    CopyActivity,
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    WebActivity,
)
from wkmigrate.models.workflows.artifacts import PreparedActivity, PreparedWorkflow
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
from wkmigrate.preparers.for_each_activity_preparer import prepare_for_each_activity
from wkmigrate.preparers.if_condition_activity_preparer import prepare_if_condition_activity
from wkmigrate.preparers.lookup_activity_preparer import prepare_lookup_activity
from wkmigrate.preparers.notebook_activity_preparer import prepare_notebook_activity
from wkmigrate.preparers.run_job_activity_preparer import prepare_run_job_activity
from wkmigrate.preparers.set_variable_activity_preparer import prepare_set_variable_activity
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
    activities = [prepare_activity(task, files_to_delta_sinks) for task in pipeline.tasks]
    return PreparedWorkflow(pipeline=pipeline, activities=activities)


def prepare_activity(
    activity: Activity,
    default_files_to_delta_sinks: bool | None,
) -> PreparedActivity:
    """
    Prepares an activity internal representation for creation as a Databricks Lakeflow job task.

    Args:
        activity: Activity internal representation to prepare.
        default_files_to_delta_sinks: Whether to use the default files-to-delta sinks behavior.

    Returns:
        Prepared activity containing the task configuration and any associated artifacts.
    """
    if isinstance(activity, DatabricksNotebookActivity):
        return prepare_notebook_activity(activity)
    if isinstance(activity, SparkJarActivity):
        return prepare_spark_jar_activity(activity)
    if isinstance(activity, SparkPythonActivity):
        return prepare_spark_python_activity(activity)
    if isinstance(activity, IfConditionActivity):
        return prepare_if_condition_activity(activity)
    if isinstance(activity, ForEachActivity):
        return prepare_for_each_activity(activity, default_files_to_delta_sinks)
    if isinstance(activity, RunJobActivity):
        return prepare_run_job_activity(activity, default_files_to_delta_sinks)
    if isinstance(activity, CopyActivity):
        return prepare_copy_activity(activity, default_files_to_delta_sinks)
    if isinstance(activity, LookupActivity):
        return prepare_lookup_activity(activity)
    if isinstance(activity, WebActivity):
        return prepare_web_activity(activity)
    if isinstance(activity, SetVariableActivity):
        return prepare_set_variable_activity(activity)
    raise ValueError(f"Unsupported activity type '{type(activity)}'")

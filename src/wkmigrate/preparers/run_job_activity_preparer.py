"""
This module defines a preparer for Run Job activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Run Job activity. This includes all nested properties
and tasks of the job to be run.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from wkmigrate.models.ir.pipeline import RunJobActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity, PreparedWorkflow
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_run_job_activity(
    activity: RunJobActivity,
    default_files_to_delta_sinks: bool | None,
) -> tuple[PreparedActivity, PreparedWorkflow | None]:
    """
    Builds the task payload for a Run Job activity.

    Args:
        activity: Activity definition emitted by the translators
        default_files_to_delta_sinks: Optional override for DLT generation of inner activities.

    Returns:
        Spark Run Job task configuration
    """
    if activity.existing_job_id:
        run_job_task = parse_mapping({"job_id": activity.existing_job_id, "job_parameters": activity.job_parameters})
        task = parse_mapping({**get_base_task(activity), "run_job_task": run_job_task})
        return PreparedActivity(task=task), None

    if not activity.pipeline:
        raise ValueError(f"RunJobActivity '{activity.name}' must specify 'pipeline' or 'existing_job_id'")

    preparer = import_module("wkmigrate.preparers.preparer")
    inner_results = preparer.prepare_activities(
        activity.pipeline.tasks,
        default_files_to_delta_sinks,
    )

    inner_tasks: list[dict[str, Any]] = []
    inner_notebooks: list[NotebookArtifact] = []
    inner_pipelines: list[PipelineInstruction] = []
    inner_secrets: list[SecretInstruction] = []
    nested_inner_jobs: list[dict[str, Any]] = []

    for inner_activity, inner_workflow in inner_results:
        inner_tasks.append(inner_activity.task)
        if inner_activity.notebooks:
            inner_notebooks.extend(inner_activity.notebooks)
        if inner_activity.pipelines:
            inner_pipelines.extend(inner_activity.pipelines)
        if inner_activity.secrets:
            inner_secrets.extend(inner_activity.secrets)
        if inner_workflow:
            nested_inner_jobs.append(inner_workflow.job_settings)

    inner_job_settings = {
        "name": activity.name,
        "parameters": activity.pipeline.parameters,
        "schedule": activity.pipeline.schedule,
        "tags": activity.pipeline.tags,
        "tasks": inner_tasks,
        "not_translatable": list(activity.pipeline.not_translatable),
    }

    prepared_workflow = PreparedWorkflow(
        job_settings=inner_job_settings,
        notebooks=inner_notebooks if inner_notebooks else None,
        pipelines=inner_pipelines if inner_pipelines else None,
        secrets=inner_secrets if inner_secrets else None,
        inner_jobs=nested_inner_jobs if nested_inner_jobs else None,
    )

    prepared_activity = PreparedActivity(
        task=parse_mapping({**get_base_task(activity), "run_job_task": f"__INNER_JOB__:{activity.name}"})
    )
    return prepared_activity, prepared_workflow

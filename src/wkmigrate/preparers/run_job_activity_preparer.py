"""Preparer for Run Job and Execute Pipeline activities.

Both activity types produce a Databricks Run Job task that invokes a
workflow.  ``RunJobActivity`` may reference an existing job by ID or embed
a child pipeline; ``ExecutePipelineActivity`` always references a child
pipeline (resolved or unresolved).
"""

from __future__ import annotations

from importlib import import_module

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.models.ir.pipeline import ExecutePipelineActivity, RunJobActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_run_job_activity(
    activity: RunJobActivity | ExecutePipelineActivity,
    default_files_to_delta_sinks: bool | None,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> PreparedActivity:
    """Builds the task payload for a Run Job or Execute Pipeline activity.

    For ``RunJobActivity`` with an ``existing_job_id``, a simple run-job
    reference is emitted.  When a child ``pipeline`` IR is present on either
    activity type, it is prepared as an inner workflow.  For
    ``ExecutePipelineActivity`` whose child pipeline could not be resolved, a
    placeholder job reference keyed by pipeline name is emitted.

    Args:
        activity: ``RunJobActivity`` or ``ExecutePipelineActivity`` emitted by the translators.
        default_files_to_delta_sinks: Optional override for DLT generation of inner activities.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        Prepared activity containing the Run Job task configuration and optional inner workflow.
    """
    if isinstance(activity, RunJobActivity) and activity.existing_job_id:
        run_job_task = parse_mapping({"job_id": activity.existing_job_id, "job_parameters": activity.job_parameters})
        task = parse_mapping({**get_base_task(activity), "run_job_task": run_job_task})
        return PreparedActivity(task=task)

    if activity.pipeline is not None:
        return _prepare_inner_workflow(activity, default_files_to_delta_sinks, credentials_scope)

    if isinstance(activity, ExecutePipelineActivity):
        return _prepare_unresolved_placeholder(activity)

    raise ValueError(f"RunJobActivity '{activity.name}' must specify 'pipeline' or 'existing_job_id'")


def _prepare_inner_workflow(
    activity: RunJobActivity | ExecutePipelineActivity,
    default_files_to_delta_sinks: bool | None,
    credentials_scope: str,
) -> PreparedActivity:
    """Prepares a resolved child pipeline as an inner workflow.

    Args:
        activity: Activity whose ``pipeline`` field is not ``None``.
        default_files_to_delta_sinks: Optional override for DLT generation of inner activities.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        Prepared activity with inner workflow attached.
    """
    preparer = import_module("wkmigrate.preparers.preparer")
    inner_workflow = preparer.prepare_workflow(activity.pipeline, default_files_to_delta_sinks, credentials_scope)

    job_name = activity.pipeline_name if isinstance(activity, ExecutePipelineActivity) else activity.name
    job_parameters = activity.parameters if isinstance(activity, ExecutePipelineActivity) else activity.job_parameters

    run_job_task_dict: dict = {"job_id": f"__INNER_JOB__:{job_name}"}
    if job_parameters:
        run_job_task_dict["job_parameters"] = job_parameters

    return PreparedActivity(
        task=parse_mapping({**get_base_task(activity), "run_job_task": run_job_task_dict}),
        inner_workflow=inner_workflow,
    )


def _prepare_unresolved_placeholder(activity: ExecutePipelineActivity) -> PreparedActivity:
    """Emits a placeholder run-job reference for an unresolved child pipeline.

    Args:
        activity: Execute Pipeline activity whose child pipeline was not resolved.

    Returns:
        Prepared activity with a template job ID the user can replace manually.
    """
    run_job_task_dict: dict = {"job_id": f"{{{{job_id_for_{activity.pipeline_name}}}}}"}
    if activity.parameters:
        run_job_task_dict["job_parameters"] = activity.parameters
    task = parse_mapping({**get_base_task(activity), "run_job_task": run_job_task_dict})
    return PreparedActivity(task=task)

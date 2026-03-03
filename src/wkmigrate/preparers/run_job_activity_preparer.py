"""
This module defines a preparer for Run Job activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Run Job activity. This includes all nested properties
and tasks of the job to be run.
"""

from __future__ import annotations

from importlib import import_module

from wkmigrate.models.ir.pipeline import RunJobActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_run_job_activity(
    activity: RunJobActivity,
    default_files_to_delta_sinks: bool | None,
) -> PreparedActivity:
    """
    Builds the task payload for a Run Job activity.

    Args:
        activity: Activity definition emitted by the translators
        default_files_to_delta_sinks: Optional override for DLT generation of inner activities.

    Returns:
        Prepared activity containing the Run Job task configuration.
    """
    if activity.existing_job_id:
        run_job_task = parse_mapping({"job_id": activity.existing_job_id, "job_parameters": activity.job_parameters})
        task = parse_mapping({**get_base_task(activity), "run_job_task": run_job_task})
        return PreparedActivity(task=task)

    if not activity.pipeline:
        raise ValueError(f"RunJobActivity '{activity.name}' must specify 'pipeline' or 'existing_job_id'")

    preparer = import_module("wkmigrate.preparers.preparer")
    inner_workflow = preparer.prepare_workflow(activity.pipeline, default_files_to_delta_sinks)

    return PreparedActivity(
        task=parse_mapping({**get_base_task(activity), "run_job_task": f"__INNER_JOB__:{activity.name}"}),
        inner_workflow=inner_workflow,
    )

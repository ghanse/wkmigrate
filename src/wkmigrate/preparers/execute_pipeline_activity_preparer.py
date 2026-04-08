"""Preparer for Execute Pipeline activities.

Builds a Databricks Run Job task that invokes the translated child pipeline
as a nested workflow, passing pipeline parameters as job parameters.  When
the child pipeline IR is available the preparer delegates to
``prepare_workflow``; otherwise it emits a placeholder job reference keyed
by pipeline name.
"""

from __future__ import annotations

from importlib import import_module

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.models.ir.pipeline import ExecutePipelineActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_execute_pipeline_activity(
    activity: ExecutePipelineActivity,
    default_files_to_delta_sinks: bool | None,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> PreparedActivity:
    """Builds the task payload for an Execute Pipeline activity.

    When the child pipeline was resolved and translated, a full inner workflow
    is prepared.  Otherwise a placeholder ``run_job_task`` referencing the
    pipeline by name is emitted so the user can wire it up manually.

    Args:
        activity: Activity definition emitted by the translators.
        default_files_to_delta_sinks: Optional override for DLT generation of inner activities.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        Prepared activity containing the Run Job task configuration and optional inner workflow.
    """
    base_task = get_base_task(activity)

    if activity.pipeline is not None:
        preparer = import_module("wkmigrate.preparers.preparer")
        inner_workflow = preparer.prepare_workflow(activity.pipeline, default_files_to_delta_sinks, credentials_scope)

        run_job_task_dict: dict = {"job_id": f"__INNER_JOB__:{activity.pipeline_name}"}
        if activity.parameters:
            run_job_task_dict["job_parameters"] = activity.parameters
        task = parse_mapping({**base_task, "run_job_task": run_job_task_dict})
        return PreparedActivity(task=task, inner_workflow=inner_workflow)

    # Child pipeline was not resolved -- emit a placeholder reference.
    run_job_task = parse_mapping(
        {"job_id": f"{{{{job_id_for_{activity.pipeline_name}}}}}", "job_parameters": activity.parameters}
    )
    task = parse_mapping({**base_task, "run_job_task": run_job_task})
    return PreparedActivity(task=task)

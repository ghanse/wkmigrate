"""
This module defines a preparer for ForEach activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a ForEach activity. This includes For Each task configuration,
and nested activity tasks and artifacts.
"""

from __future__ import annotations
from importlib import import_module
from wkmigrate.models.ir.pipeline import ForEachActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_for_each_activity(
    activity: ForEachActivity,
    default_files_to_delta_sinks: bool | None,
) -> PreparedActivity:
    """
    Builds the task payload for a ForEach activity.

    Args:
        activity: Activity definition emitted by the translators
        default_files_to_delta_sinks: Optional override for DLT generation

    Returns:
        Prepared activity containing the ForEach task configuration.
    """
    preparer = import_module("wkmigrate.preparers.preparer")
    inner_prepared = preparer.prepare_activity(
        activity.for_each_task,
        default_files_to_delta_sinks,
    )

    for_each_task = parse_mapping(
        {
            "task": inner_prepared.task,
            "inputs": activity.items_string,
            "concurrency": activity.concurrency,
        }
    )

    return PreparedActivity(
        task=parse_mapping({**get_base_task(activity), "for_each_task": for_each_task}),
        notebooks=inner_prepared.notebooks,
        pipelines=inner_prepared.pipelines,
        secrets=inner_prepared.secrets,
        inner_workflow=inner_prepared.inner_workflow,
    )

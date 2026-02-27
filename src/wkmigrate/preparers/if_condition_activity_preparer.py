"""
This module defines a preparer for If Condition activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of an If Condition activity. This includes an If/Else Condition
task definition and any nested activity tasks or artifacts.
"""

from __future__ import annotations
from databricks.sdk.service.jobs import ConditionTaskOp
from wkmigrate.models.ir.pipeline import IfConditionActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_if_condition_activity(activity: IfConditionActivity) -> PreparedActivity:
    """
    Builds the task payload for an If Condition activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Databricks condition task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "condition_task": {
                "op": ConditionTaskOp(activity.op),
                "left": activity.left,
                "right": activity.right,
            },
        }
    )
    return PreparedActivity(task=task)

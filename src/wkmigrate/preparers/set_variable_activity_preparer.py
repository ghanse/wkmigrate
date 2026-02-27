"""Preparer for SetVariable activities.

The preparer emits a single notebook task that evaluates the translated
variable expression and publishes the result as a Databricks task value via
``dbutils.jobs.taskValues.set()``.  Downstream tasks can retrieve the value
with ``dbutils.jobs.taskValues.get(taskKey=<this_task_key>, key=<variable_name>)``.
"""

from __future__ import annotations

from wkmigrate.code_generator import get_set_variable_notebook_content
from wkmigrate.models.ir.pipeline import SetVariableActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_set_variable_activity(activity: SetVariableActivity) -> PreparedActivity:
    """
    Builds tasks and artifacts for a SetVariable activity.

    The resulting notebook:

    1. Evaluates the translated variable expression (static literal, task-value
       lookup, or pipeline-context call).
    2. Publishes the result as a Databricks task value under ``key=variable_name``
       so that downstream tasks can reference it.

    Args:
        activity: :class:`SetVariableActivity` IR instance produced by the translator.

    Returns:
        :class:`PreparedActivity` containing the notebook task configuration and
        the generated notebook artifact.
    """
    notebook_content = get_set_variable_notebook_content(activity.variable_name, activity.variable_value)
    notebook_path = f"/wkmigrate/set_variable_notebooks/{activity.task_key}/set_{activity.variable_name}"
    notebook = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    base_task = get_base_task(activity)
    task = parse_mapping(
        {
            **base_task,
            "notebook_task": {"notebook_path": notebook_path},
        }
    )
    return PreparedActivity(task=task, notebooks=[notebook])

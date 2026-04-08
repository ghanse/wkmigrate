"""This module defines a preparer for Delete activities.

The preparer builds a Databricks notebook task that removes files or folders
from cloud storage using ``dbutils.fs.rm()``.
"""

from __future__ import annotations

import autopep8  # type: ignore

from wkmigrate.models.ir.pipeline import DeleteActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_delete_activity(activity: DeleteActivity) -> PreparedActivity:
    """Builds the task payload for a Delete activity.

    The resulting notebook removes files or folders from cloud storage
    using ``dbutils.fs.rm()``.

    Args:
        activity: Activity definition emitted by the translators.

    Returns:
        PreparedActivity containing the notebook task configuration and artifacts.
    """
    notebook_content = _get_delete_activity_notebook_content(
        activity_name=activity.name,
        dataset_name=activity.dataset_name,
        folder_path=activity.folder_path,
        recursive=activity.recursive,
        wildcard_file_name=activity.wildcard_file_name,
        wildcard_folder_path=activity.wildcard_folder_path,
    )
    notebook_path = f"/wkmigrate/delete_activity_notebooks/{activity.task_key}"
    notebook = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    base_task = get_base_task(activity)
    task = parse_mapping({**base_task, "notebook_task": {"notebook_path": notebook_path}})
    return PreparedActivity(task=task, notebooks=[notebook])


def _get_delete_activity_notebook_content(
    activity_name: str,
    dataset_name: str,
    folder_path: str | None,
    recursive: bool,
    wildcard_file_name: str | None,
    wildcard_folder_path: str | None,
) -> str:
    """Generates notebook source for a Delete activity.

    Args:
        activity_name: Logical name of the activity being translated.
        dataset_name: Reference name of the dataset that identifies the storage location.
        folder_path: Optional folder path within the dataset to delete.
        recursive: When ``True`` the delete operation removes contents recursively.
        wildcard_file_name: Optional wildcard pattern to match file names for deletion.
        wildcard_folder_path: Optional wildcard pattern to match folder paths for deletion.

    Returns:
        Formatted Python notebook source as a ``str``.
    """
    path = folder_path or dataset_name

    script_lines = [
        "# Databricks notebook source",
        "",
        f"# Delete activity: {activity_name}",
        f"# Dataset: {dataset_name}",
        f"path = {path!r}",
    ]

    if wildcard_file_name:
        script_lines.append(f"wildcard_file_name = {wildcard_file_name!r}")
        script_lines.append("# NOTE: Wildcard deletion requires listing and filtering files manually.")
        script_lines.append("import fnmatch")
        if wildcard_folder_path:
            script_lines.append(f"wildcard_folder_path = {wildcard_folder_path!r}")
            script_lines.append(
                "# Filter folders matching the wildcard folder path, then files matching the wildcard file name."
            )
        script_lines.extend(
            [
                "files = dbutils.fs.ls(path)",
                "for f in files:",
                "    if fnmatch.fnmatch(f.name, wildcard_file_name):",
                f"        dbutils.fs.rm(f.path, recurse={recursive})",
            ]
        )
    else:
        script_lines.append(f"dbutils.fs.rm(path, recurse={recursive})")

    return autopep8.fix_code("\n".join(script_lines))

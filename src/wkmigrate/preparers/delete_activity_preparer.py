"""This module defines a preparer for Delete activities.

The preparer builds a Databricks notebook task that removes files or folders
from cloud storage using ``dbutils.fs.rm()``.
"""

from __future__ import annotations

import warnings

import autopep8  # type: ignore

from wkmigrate.models.ir.pipeline import DeleteActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.not_translatable import NotTranslatableWarning
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
    script_lines = [
        "# Databricks notebook source",
        "",
        f"# Delete activity: {activity_name}",
        f"# Dataset: {dataset_name}",
    ]

    if folder_path:
        script_lines.append(f"path = {folder_path!r}")
    else:
        warnings.warn(
            NotTranslatableWarning(
                "folder_path",
                f"Storage path could not be resolved from dataset reference '{dataset_name}'. "
                "Replace the placeholder path below with the actual storage location.",
            ),
            stacklevel=2,
        )
        script_lines.append(f"# TODO: Resolve storage path for dataset '{dataset_name}'")
        script_lines.append(f"path = '<UNRESOLVED_PATH_FOR_{dataset_name}>'")

    if wildcard_folder_path and wildcard_file_name:
        # Two-level listing: filter folders by wildcard_folder_path, then files by wildcard_file_name.
        script_lines.extend(
            [
                "import fnmatch",
                f"wildcard_folder_path = {wildcard_folder_path!r}",
                f"wildcard_file_name = {wildcard_file_name!r}",
                "folders = dbutils.fs.ls(path)",
                "for folder in folders:",
                "    if fnmatch.fnmatch(folder.name, wildcard_folder_path):",
                "        files = dbutils.fs.ls(folder.path)",
                "        for f in files:",
                "            if fnmatch.fnmatch(f.name, wildcard_file_name):",
                f"                dbutils.fs.rm(f.path, recurse={recursive})",
            ]
        )
    elif wildcard_file_name:
        # Single-level listing: filter files at path by wildcard_file_name.
        script_lines.extend(
            [
                "import fnmatch",
                f"wildcard_file_name = {wildcard_file_name!r}",
                "files = dbutils.fs.ls(path)",
                "for f in files:",
                "    if fnmatch.fnmatch(f.name, wildcard_file_name):",
                f"        dbutils.fs.rm(f.path, recurse={recursive})",
            ]
        )
    elif wildcard_folder_path:
        # Folder-only wildcard: filter folders at path by wildcard_folder_path and delete each match.
        script_lines.extend(
            [
                "import fnmatch",
                f"wildcard_folder_path = {wildcard_folder_path!r}",
                "folders = dbutils.fs.ls(path)",
                "for folder in folders:",
                "    if fnmatch.fnmatch(folder.name, wildcard_folder_path):",
                f"        dbutils.fs.rm(folder.path, recurse={recursive})",
            ]
        )
    else:
        script_lines.append(f"dbutils.fs.rm(path, recurse={recursive})")

    return autopep8.fix_code("\n".join(script_lines))

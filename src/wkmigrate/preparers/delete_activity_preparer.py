"""Preparer for Delete activities.

Builds a Databricks notebook task that removes files or folders from cloud
storage using ``dbutils.fs.rm()``.
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
    notebook_content = _build_notebook_content(activity)
    notebook_path = f"/wkmigrate/delete_activity_notebooks/{activity.task_key}"
    notebook = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    base_task = get_base_task(activity)
    task = parse_mapping({**base_task, "notebook_task": {"notebook_path": notebook_path}})
    return PreparedActivity(task=task, notebooks=[notebook])


def _build_notebook_content(activity: DeleteActivity) -> str:
    """Generates formatted Python notebook source for a Delete activity.

    Produces a self-contained Databricks notebook that deletes files or folders
    via ``dbutils.fs.rm()``. When wildcard patterns are present the notebook
    lists directory contents and filters with ``fnmatch``.

    Args:
        activity: Fully populated ``DeleteActivity`` IR object.

    Returns:
        Formatted Python notebook source.
    """
    lines: list[str] = [
        "# Databricks notebook source",
        "",
        f"# Delete activity: {activity.name}",
        f"# Dataset: {activity.dataset_name}",
    ]

    _append_path_assignment(lines, activity.dataset_name, activity.folder_path)
    _append_delete_logic(lines, activity.recursive, activity.wildcard_file_name, activity.wildcard_folder_path)

    return autopep8.fix_code("\n".join(lines))


def _append_path_assignment(lines: list[str], dataset_name: str, folder_path: str | None) -> None:
    """Appends the ``path`` variable assignment to notebook lines.

    Emits a ``NotTranslatableWarning`` and a TODO placeholder when the
    folder path cannot be resolved from the dataset reference.
    """
    if folder_path:
        lines.append(f"path = {folder_path!r}")
    else:
        warnings.warn(
            NotTranslatableWarning(
                "folder_path",
                f"Storage path could not be resolved from dataset reference '{dataset_name}'. "
                "Replace the placeholder path below with the actual storage location.",
            ),
            stacklevel=2,
        )
        lines.append(f"# TODO: Resolve storage path for dataset '{dataset_name}'")
        lines.append(f"path = '<UNRESOLVED_PATH_FOR_{dataset_name}>'")


def _append_delete_logic(
    lines: list[str],
    recursive: bool,
    wildcard_file_name: str | None,
    wildcard_folder_path: str | None,
) -> None:
    """Appends the ``dbutils.fs.rm`` call(s) to notebook lines.

    Chooses the appropriate deletion strategy based on which wildcard
    patterns are set: two-level, file-only, folder-only, or direct.
    """
    if wildcard_folder_path and wildcard_file_name:
        lines.extend(
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
        lines.extend(
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
        lines.extend(
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
        lines.append(f"dbutils.fs.rm(path, recurse={recursive})")

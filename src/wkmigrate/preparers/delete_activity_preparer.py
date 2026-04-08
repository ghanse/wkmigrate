"""Preparer for Delete activities.

Builds a Databricks notebook task that removes files or folders from cloud
storage using ``dbutils.fs.rm()``.  When a folder path can be resolved the
preparer also emits a one-time setup task that creates a Unity Catalog
external volume, giving the delete notebook an easily-accessible
``/Volumes/<catalog>/<schema>/<volume>`` path.
"""

from __future__ import annotations

import warnings

import autopep8  # type: ignore

from wkmigrate.models.ir.pipeline import DeleteActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping

# Default UC coordinates used when no catalog/schema is provided.
_DEFAULT_CATALOG = 'main'
_DEFAULT_SCHEMA = 'default'


def prepare_delete_activity(
    activity: DeleteActivity,
    *,
    catalog: str | None = None,
    schema: str | None = None,
) -> PreparedActivity:
    """Builds the task payload for a Delete activity.

    The resulting notebook removes files or folders from cloud storage
    using ``dbutils.fs.rm()``.  A one-time setup task is attached when
    the folder path is available so that a Unity Catalog external volume
    can be created prior to the first run.

    Args:
        activity: Activity definition emitted by the translators.
        catalog: Unity Catalog catalog for the external volume. Falls back to ``'main'``.
        schema: Unity Catalog schema for the external volume. Falls back to ``'default'``.

    Returns:
        PreparedActivity containing the notebook task configuration and artifacts.
    """
    notebook_content = _build_notebook_content(activity)
    notebook_path = f"/wkmigrate/delete_activity_notebooks/{activity.task_key}"
    notebook = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    base_task = get_base_task(activity)
    task = parse_mapping({**base_task, "notebook_task": {"notebook_path": notebook_path}})
    setup_tasks = _build_setup_tasks(activity, catalog=catalog, schema=schema)
    return PreparedActivity(task=task, notebooks=[notebook], setup_tasks=setup_tasks or None)


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


def _build_setup_tasks(
    activity: DeleteActivity,
    *,
    catalog: str | None = None,
    schema: str | None = None,
) -> list[PreparedActivity]:
    """Builds a one-time setup task that creates a UC external volume for the delete target.

    The external volume provides a ``/Volumes/<catalog>/<schema>/<volume>``
    path that maps to the cloud storage location used by the delete notebook.

    Args:
        activity: Fully populated ``DeleteActivity`` IR object.
        catalog: Unity Catalog catalog name. Falls back to ``_DEFAULT_CATALOG``.
        schema: Unity Catalog schema name. Falls back to ``_DEFAULT_SCHEMA``.

    Returns:
        List containing a single ``PreparedActivity`` for the setup notebook,
        or an empty list when the folder path cannot be resolved.
    """
    if not activity.folder_path:
        return []

    effective_catalog = catalog or _DEFAULT_CATALOG
    effective_schema = schema or _DEFAULT_SCHEMA
    volume_name = f"wkmigrate_{activity.task_key}"
    notebook_content = _build_volume_notebook_content(
        catalog=effective_catalog,
        schema=effective_schema,
        volume_name=volume_name,
        storage_location=activity.folder_path,
    )
    notebook_path = f"/wkmigrate/setup_notebooks/create_volume_{activity.task_key}"
    notebook = NotebookArtifact(file_path=notebook_path, content=notebook_content, language='python')
    task = parse_mapping(
        {
            "task_key": f"setup_volume_{activity.task_key}",
            "description": f"One-time setup: create external volume for delete activity '{activity.name}'.",
            "notebook_task": {"notebook_path": notebook_path},
        }
    )
    return [PreparedActivity(task=task, notebooks=[notebook])]


def _build_volume_notebook_content(
    *,
    catalog: str,
    schema: str,
    volume_name: str,
    storage_location: str,
) -> str:
    """Generates a Python notebook that creates a Unity Catalog external volume.

    The notebook executes ``CREATE EXTERNAL VOLUME IF NOT EXISTS`` so it is
    safe to run repeatedly.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Unity Catalog schema name.
        volume_name: Volume name to create.
        storage_location: Cloud storage URL that the volume will point to.

    Returns:
        Formatted Python notebook source.
    """
    lines: list[str] = [
        '# Databricks notebook source',
        '',
        '# Setup: create external volume for delete activity',
        f'# Volume path: /Volumes/{catalog}/{schema}/{volume_name}',
        '',
        f'catalog = {catalog!r}',
        f'schema = {schema!r}',
        f'volume_name = {volume_name!r}',
        f'storage_location = {storage_location!r}',
        '',
        'spark.sql(f"""',
        '    CREATE EXTERNAL VOLUME IF NOT EXISTS',
        '    `{catalog}`.`{schema}`.`{volume_name}`',
        "    LOCATION '{storage_location}'",
        "    COMMENT 'External volume created by wkmigrate for delete activity.'",
        '""")',
        '',
        'print(f"Volume /Volumes/{catalog}/{schema}/{volume_name} is ready.")',
    ]
    return autopep8.fix_code('\n'.join(lines))

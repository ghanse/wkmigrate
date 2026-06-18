"""Shared helpers for workflow preparers."""

from __future__ import annotations
from typing import Any
from databricks.sdk.service.compute import Library, MavenLibrary, PythonPyPiLibrary, RCranLibrary
from wkmigrate.models.ir.pipeline import Activity
from wkmigrate.utils import parse_mapping


def get_base_task(activity: Activity) -> dict[str, Any]:
    """
    Returns the fields common to every task.

    Args:
        activity: Activity instance emitted by the translator.

    Returns:
        Dictionary containing the common task fields.
    """
    depends_on = None
    libraries = None
    if activity.depends_on:
        depends_on = [
            parse_mapping(
                {
                    "task_key": dep.task_key,
                    "outcome": dep.outcome,
                }
            )
            for dep in activity.depends_on
        ]
    if activity.libraries:
        libraries = [_create_library(library) for library in activity.libraries]
    return parse_mapping(
        {
            "task_key": activity.task_key,
            "description": activity.description,
            "timeout_seconds": activity.timeout_seconds,
            "max_retries": activity.max_retries,
            "min_retry_interval_millis": activity.min_retry_interval_millis,
            "depends_on": depends_on,
            "run_if": activity.run_if,
            "new_cluster": activity.new_cluster,
            "libraries": libraries,
        }
    )


def _create_library(library: dict[str, Any]) -> Library:
    """
    Creates a library dictionary from a library dependency.

    Args:
        library: Library dependency.

    Returns:
        A Databricks library object
    """
    if "pypi" in library:
        properties = library["pypi"]
        return Library(
            pypi=PythonPyPiLibrary(
                package=properties.get("package", ""),
                repo=properties.get("repo"),
            )
        )
    if "maven" in library:
        properties = library["maven"]
        return Library(
            maven=MavenLibrary(
                coordinates=properties.get("coordinates", ""),
                repo=properties.get("repo"),
                exclusions=properties.get("exclusions"),
            )
        )
    if "cran" in library:
        properties = library["cran"]
        return Library(
            cran=RCranLibrary(
                package=properties.get("package", ""),
                repo=properties.get("repo"),
            )
        )
    if "jar" in library:
        return Library(jar=library.get("jar"))
    if "egg" in library:
        return Library(egg=library.get("egg"))
    if "whl" in library:
        return Library(whl=library.get("whl"))
    raise ValueError(f"Unsupported library type '{library}'")

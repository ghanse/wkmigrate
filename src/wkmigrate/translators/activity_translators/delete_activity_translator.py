"""This module defines a translator for translating Delete activities.

Translators in this module normalize ADF Delete activity payloads into internal
representations. The Delete activity removes files or folders from supported
storage stores.  Each translator must validate required fields, parse the
dataset reference and store settings, and emit ``NotTranslatableWarning``
objects for any features that cannot be migrated.
"""

import warnings

from wkmigrate.models.ir.pipeline import DeleteActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning


def translate_delete_activity(activity: dict, base_kwargs: dict) -> DeleteActivity | UnsupportedValue:
    """Translates an ADF Delete activity into a ``DeleteActivity`` object.

    Args:
        activity: Delete activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``DeleteActivity`` representation of the delete task.
    """
    dataset_ref = activity.get("dataset")
    if not isinstance(dataset_ref, dict):
        return UnsupportedValue(activity, "Missing value 'dataset' for Delete activity")

    dataset_name = dataset_ref.get("reference_name")
    if not dataset_name:
        return UnsupportedValue(activity, "Missing value 'dataset.referenceName' for Delete activity")

    recursive = activity.get("recursive", True)

    store_settings = activity.get("store_settings") or {}
    wildcard_file_name = store_settings.get("wildcard_file_name")
    wildcard_folder_path = store_settings.get("wildcard_folder_path")

    folder_path = activity.get("folder_path")

    if activity.get("enable_logging"):
        warnings.warn(
            NotTranslatableWarning(
                "enableLogging",
                "Delete activity logging is not supported in Databricks workflows.",
            ),
            stacklevel=2,
        )

    if activity.get("log_storage_settings"):
        warnings.warn(
            NotTranslatableWarning(
                "logStorageSettings",
                "Delete activity log storage settings are not supported in Databricks workflows.",
            ),
            stacklevel=2,
        )

    return DeleteActivity(
        **base_kwargs,
        dataset_name=dataset_name,
        folder_path=folder_path,
        recursive=bool(recursive),
        wildcard_file_name=wildcard_file_name,
        wildcard_folder_path=wildcard_folder_path,
    )

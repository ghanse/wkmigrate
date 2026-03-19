"""This module defines a translator for Databricks Job activities.

Translators in this module normalize Databricks Job activity payloads into internal
representations. Each translator must validate required fields and emit ``UnsupportedValue``
objects for any unparsable inputs.
"""

from wkmigrate.models.ir.pipeline import RunJobActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import parse_mapping


def translate_databricks_job_activity(activity: dict, base_kwargs: dict) -> RunJobActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Job activity into a ``RunJobActivity`` object.

    Args:
        activity: Databricks Job activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``RunJobActivity`` referencing the existing Databricks job, or an
        ``UnsupportedValue`` if ``existing_job_id`` is missing.
    """
    existing_job_id = activity.get("existing_job_id") or activity.get("job_id")
    if not existing_job_id:
        return UnsupportedValue(activity, "Missing field 'existing_job_id' for Databricks Job activity")

    job_parameters = parse_mapping(activity.get("job_parameters"))
    return RunJobActivity(
        **base_kwargs,
        existing_job_id=str(existing_job_id),
        job_parameters=job_parameters,
    )

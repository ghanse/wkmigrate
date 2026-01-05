"""This module defines methods for translating For Each activities."""

from wkmigrate.activity_translators.parsers import parse_for_each_items, parse_for_each_tasks
from wkmigrate.models.ir.activities import ForEachActivity


def translate_for_each_activity(activity: dict, base_kwargs: dict) -> ForEachActivity:
    """
    Translates an ADF ForEach activity into a ``ForEachActivity`` object.

    Args:
        activity: ForEach activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``ForEachActivity`` representation of the ForEach task.
    """
    return ForEachActivity(
        **base_kwargs,
        items_string=parse_for_each_items(activity.get("items")),
        inner_activities=parse_for_each_tasks(activity.get("activities")) or [],
        concurrency=activity.get("batch_count"),
    )

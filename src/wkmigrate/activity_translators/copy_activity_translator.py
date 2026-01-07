"""This module defines methods for translating Copy activities."""

from wkmigrate.activity_translators.parsers import (
    parse_dataset,
    parse_dataset_mapping,
    parse_dataset_properties,
)
from wkmigrate.models.ir.activities import CopyActivity


def translate_copy_activity(activity: dict, base_kwargs: dict) -> CopyActivity:
    """
    Translates an ADF Copy activity into a ``CopyActivity`` object.

    Args:
        activity: Copy activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``CopyActivity`` representation of the Copy task.
    """
    source_definition = activity.get("source")
    sink_definition = activity.get("sink")
    input_dataset_definitions = activity.get("input_dataset_definitions")
    output_dataset_definitions = activity.get("output_dataset_definitions")
    return CopyActivity(
        **base_kwargs,
        source_dataset=parse_dataset(input_dataset_definitions) if input_dataset_definitions else None,
        sink_dataset=parse_dataset(output_dataset_definitions) if output_dataset_definitions else None,
        source_properties=parse_dataset_properties(source_definition) if source_definition else None,
        sink_properties=parse_dataset_properties(sink_definition) if sink_definition else None,
        column_mapping=parse_dataset_mapping(activity.get("translator") or {}),
    )

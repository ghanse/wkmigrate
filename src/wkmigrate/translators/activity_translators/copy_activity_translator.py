"""This module defines a translator for translating Copy activities.

Translators in this module normalize Copy Data activity payloads into internal representations.
Each translator must validate required fields, coerce connection settings, source and sink dataset
properties, and column mappings.  Translators should emit ``UnsupportedValue`` objects for any
unparsable inputs.
"""

from wkmigrate.models.ir.pipeline import ColumnMapping, CopyActivity
from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.supported_types import translates_activity
from wkmigrate.utils import (
    get_data_source_definition,
    get_data_source_properties,
    get_value_or_unsupported,
    merge_unsupported_values,
)


@translates_activity("Copy")
def translate_copy_activity(activity: dict, base_kwargs: dict) -> CopyActivity | UnsupportedValue:
    """
    Translates an ADF Copy activity into a ``CopyActivity`` object. Copy activities are translated
    into Lakeflow Declarative Pipelines tasks or Notebook tasks depending on the source and target
    dataset types.

    This method returns an ``UnsupportedValue`` if the activity cannot be translated. This can be due to:
    * Missing or invalid dataset definitions
    * Missing required dataset properties
    * Unsupported dataset types
    * Unsupported dataset format settings

    Args:
        activity: Copy activity definition as a ``dict``
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``

    Returns:
        ``CopyActivity`` representation of the Copy task.
    """
    source_dataset = get_data_source_definition(get_value_or_unsupported(activity, "input_dataset_definitions"))
    if isinstance(source_dataset, UnsupportedValue):
        return UnsupportedValue(value=activity, message=f"Could not translate copy activity. {source_dataset.message}")

    sink_dataset = get_data_source_definition(get_value_or_unsupported(activity, "output_dataset_definitions"))
    if isinstance(sink_dataset, UnsupportedValue):
        return UnsupportedValue(value=activity, message=f"Could not translate copy activity. {sink_dataset.message}")

    source_properties = get_data_source_properties(get_value_or_unsupported(activity, "source"))
    sink_properties = get_data_source_properties(get_value_or_unsupported(activity, "sink"))
    column_mapping = _parse_type_translator(activity.get("translator") or {})
    if any(isinstance(mapping, UnsupportedValue) for mapping in column_mapping):
        unsupported_value_messages = [item.message for item in column_mapping if isinstance(item, UnsupportedValue)]
        return UnsupportedValue(
            activity,
            f"Could not parse property 'translator' of dataset. {'. '.join(unsupported_value_messages)}.".rstrip(),
        )

    if (
        isinstance(source_dataset, Dataset)
        and isinstance(sink_dataset, Dataset)
        and isinstance(source_properties, dict)
        and isinstance(sink_properties, dict)
    ):
        return CopyActivity(
            **base_kwargs,
            source_dataset=source_dataset,
            sink_dataset=sink_dataset,
            source_properties=source_properties,
            sink_properties=sink_properties,
            column_mapping=column_mapping,  # type: ignore
        )

    return merge_unsupported_values([source_dataset, sink_dataset, source_properties, sink_properties])


def _parse_type_translator(type_translator: dict) -> list[ColumnMapping | UnsupportedValue]:
    """
    Parses a type translator from one set of data columns to another, converting ADF column types
    to Spark equivalents using the sink system's type mapping.

    Args:
        type_translator: Tabular type translator with data column mappings.

    Returns:
        List of column mapping definitions as ``ColumnMapping`` or ``UnsupportedValue`` objects.
    """
    mappings = type_translator.get("mappings") or []
    return [_parse_dataset_mapping(mapping) for mapping in mappings]


def _parse_dataset_mapping(mapping: dict[str, dict]) -> ColumnMapping | UnsupportedValue:
    """
    Parses a single column mapping entry from the ADF type translator into a ``ColumnMapping``.

    Args:
        mapping: Single column mapping dictionary containing ``source`` and ``sink`` keys.

    Returns:
        Parsed ``ColumnMapping`` or ``UnsupportedValue`` when required fields are missing.
    """
    source = get_value_or_unsupported(mapping, "source", "column mapping")
    if isinstance(source, UnsupportedValue):
        return source

    sink = get_value_or_unsupported(mapping, "sink", "column mapping")
    if isinstance(sink, UnsupportedValue):
        return sink

    sink_column_name = get_value_or_unsupported(sink, "name", "sink dataset")
    if isinstance(sink_column_name, UnsupportedValue):
        return sink_column_name

    sink_column_type = get_value_or_unsupported(sink, "type", "sink dataset")
    if isinstance(sink_column_type, UnsupportedValue):
        return sink_column_type

    source_name = (mapping.get("source") or {}).get("name")
    return ColumnMapping(
        source_column_name=source_name or f"_c{source.get('ordinal', 1) - 1}",
        sink_column_name=sink_column_name,
        sink_column_type=sink_column_type,
    )

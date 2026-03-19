"""This module defines a translator for translating Lookup activities.

Translators in this module normalize ADF Lookup activity payloads into internal
representations.  A Lookup activity reads data from a dataset (file or database)
and returns the result as a task value so that downstream tasks can reference it.
Each translator must validate required fields, parse the source dataset and its
properties, and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.models.ir.pipeline import LookupActivity
from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import (
    get_data_source_definition,
    get_data_source_properties,
    get_value_or_unsupported,
    merge_unsupported_values,
)


def translate_lookup_activity(activity: dict, base_kwargs: dict) -> LookupActivity | UnsupportedValue:
    """
    Translates an ADF Lookup activity into a ``LookupActivity`` object.

    Lookup activities are translated into notebook tasks that read data via Spark
    (either a native file source or a database using JDBC), collect the rows, and
    publish the result as a Databricks task value.

    This method returns an ``UnsupportedValue`` if the activity cannot be translated
    due to missing or invalid dataset definitions or unsupported dataset types.

    Args:
        activity: Lookup activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``LookupActivity`` representation of the lookup task.
    """
    source_dataset = get_data_source_definition(get_value_or_unsupported(activity, "input_dataset_definitions"))
    source_properties = get_data_source_properties(get_value_or_unsupported(activity, "source"))
    first_row_only = activity.get("first_row_only", True)
    source_query = _parse_source_query(activity.get("source") or {})

    if isinstance(source_dataset, Dataset) and isinstance(source_properties, dict):
        return LookupActivity(
            **base_kwargs,
            source_dataset=source_dataset,
            source_properties=source_properties,
            first_row_only=first_row_only,
            source_query=source_query,
        )

    return merge_unsupported_values([source_dataset, source_properties])


def _parse_source_query(source: dict) -> str | None:
    """
    Extracts the optional SQL query from a Lookup source block.

    ADF Lookup activities support ``sql_reader_query`` (AzureSqlSource) and
    ``query`` (generic) as query properties.

    Args:
        source: Source definition from the Lookup activity.

    Returns:
        SQL query string, or ``None`` when no query is specified.
    """
    return source.get("sql_reader_query") or source.get("query")

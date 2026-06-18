"""This module defines shared utilities for translating data pipelines.

Utilities in this module cover common translation patterns such as mapping
dictionaries with parser specifications, normalizing expressions, and enriching
metadata (e.g. appending system tags).
"""

import re
import warnings
from collections.abc import Callable
from importlib import import_module
from typing import Any

from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.pipeline import Activity, Authentication, DatabricksNotebookActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning

DEFAULT_TIMEOUT_SECONDS = 43200


def camel_to_snake(name: str) -> str:
    """
    Converts a camelCase or PascalCase string to snake_case.

    Used when loading ADF definitions that use camelCase (e.g. REST/portal export)
    so that downstream code, which expects snake_case keys, works unchanged.
    Note: acronyms (e.g. HTTPResponse) may not round-trip cleanly.

    Args:
        name: Identifier in camelCase or PascalCase.

    Returns:
        Same identifier in snake_case.
    """
    substituted = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", substituted).lower()


def recursive_camel_to_snake(obj: Any) -> Any:
    """
    Recursively converts all dict keys in a structure from camelCase to snake_case.
    Leaves list order and non-dict values unchanged. Creates new dicts/lists (no in-place mutation).

    Args:
        obj: Nested structure of dicts, lists, and primitives (e.g. ADF JSON).

    Returns:
        New structure with the same values but dict keys in snake_case.
    """
    if isinstance(obj, dict):
        return {camel_to_snake(k): recursive_camel_to_snake(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [recursive_camel_to_snake(item) for item in obj]
    return obj


def _annotations_to_tags(annotations: list | dict | None) -> dict | None:
    """Convert ADF annotations (a list of strings) to a tags dict."""
    if isinstance(annotations, list):
        return {a: "" for a in annotations if isinstance(a, str)} or None
    return annotations


def normalize_arm_pipeline(pipeline: dict) -> dict:
    """
    Normalizes an ARM/REST-style ADF pipeline into the flat shape expected by
    translate_pipeline: top-level activities, parameters, trigger, tags, and
    per-activity fields (e.g. type_properties merged into the activity root).

    Use for pipeline JSON that has a "properties" wrapper and/or activities
    with "typeProperties" / "type_properties" (e.g. exported from the Azure portal).
    Call recursive_camel_to_snake first if the payload is camelCase.

    Args:
        pipeline: Raw pipeline dict (camelCase or snake_case).

    Returns:
        Pipeline dict with name, activities, parameters, trigger, tags, and
        each activity with type_properties merged into the root.
    """
    if isinstance(pipeline.get("properties"), dict):
        props = pipeline["properties"]
        activities = props.get("activities") or props.get("Activities") or []
        parameters = props.get("parameters") if "parameters" in props else props.get("Parameters")
        pipeline_definition = {
            "name": pipeline.get("name"),
            "activities": list(activities),
            "parameters": parameters,
            "trigger": None,
            "tags": pipeline.get("tags") or _annotations_to_tags(props.get("annotations")) or {},
        }
    else:
        pipeline_definition = dict(pipeline)
        if "trigger" not in pipeline_definition:
            pipeline_definition["trigger"] = None
    activities = pipeline_definition.get("activities") or []
    normalized_activities = []
    for activity in activities:
        if not isinstance(activity, dict):
            normalized_activities.append(activity)
            continue
        activity_definition = dict(activity)
        normalized_activity_definition = _normalize_activity_type_properties(activity_definition)
        normalized_activities.append(normalized_activity_definition)
    pipeline_definition["activities"] = normalized_activities
    return pipeline_definition


def translate(items: dict | None, mapping: dict) -> dict | None:
    """
    Maps dictionary values using a translation specification.

    Args:
        items: Source dictionary.
        mapping: Translation specification; Each key defines a ``key`` to look up and a ``parser`` callable.

    Returns:
        Translated dictionary as a ``dict`` or ``None`` when no input is provided.
    """
    if items is None:
        return None
    output = {}
    for key, value in mapping.items():
        source_key = mapping[key]["key"]
        parser = mapping[key]["parser"]
        value = parser(items.get(source_key))
        if value is not None:
            output[key] = value
    return output


def parse_mapping(mapping: dict[str, Any] | None, parser: Callable[[Any], Any] | None = None) -> dict[str, Any]:
    """
    Parses dictionary values into strings.

    Args:
        mapping: Dictionary of key-value pairs
        parser: Method to apply to each mapping value

    Returns:
        Mapping with parsed values
    """
    if not mapping:
        return {}

    if parser is not None:
        return {key: parser(value) for key, value in mapping.items() if value is not None}

    return {key: value for key, value in mapping.items() if value is not None}


def append_system_tags(tags: dict | None) -> dict:
    """
    Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

    Args:
        tags: Existing job tags.

    Returns:
        dict: Updated tag dictionary.
    """
    if tags is None:
        return {"CREATED_BY_WKMIGRATE": ""}

    tags["CREATED_BY_WKMIGRATE"] = ""
    return tags


_TIMEOUT_PATTERN = re.compile(r"^(?:(\d+)\.)?((\d{1,2}):(\d{2}):(\d{2}))$")


def parse_timeout_string(timeout_string: str, prefix: str = "") -> int:
    """
    Parses a timeout string in the format ``d.hh:mm:ss`` or ``hh:mm:ss`` into seconds.

    Supports ADF timeout formats including:
    - ``"0.12:00:00"`` (12 hours)
    - ``"1.00:00:00"`` (1 day)
    - ``"2.05:30:00"`` (2 days, 5 hours, 30 minutes)
    - ``"00:30:00"`` (30 minutes, no day prefix)

    When the timeout string cannot be parsed or represents a zero/negative duration,
    a ``NotTranslatableWarning`` is emitted and the ADF default of 12 hours
    (``DEFAULT_TIMEOUT_SECONDS``) is returned instead of raising an exception.

    Args:
        timeout_string: Timeout string from the activity policy.
        prefix: Prefix to add to the timeout string to align with the format 'd.hh:mm:ss'.

    Returns:
        Total seconds represented by the timeout, or ``DEFAULT_TIMEOUT_SECONDS``
        when the value cannot be parsed.

    Warns:
        NotTranslatableWarning: If the timeout string is not in a recognised format
            or represents zero/negative duration.
    """
    if prefix:
        timeout_string = f"{prefix}{timeout_string}"

    match = _TIMEOUT_PATTERN.match(timeout_string)
    if not match:
        warnings.warn(
            NotTranslatableWarning(
                "timeout", f"Invalid timeout format: '{timeout_string}'. Expected 'd.hh:mm:ss' or 'hh:mm:ss'."
            )
        )
        return DEFAULT_TIMEOUT_SECONDS

    days = int(match.group(1)) if match.group(1) is not None else 0
    hours = int(match.group(3))
    minutes = int(match.group(4))
    seconds = int(match.group(5))

    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    if total <= 0:
        warnings.warn(NotTranslatableWarning("timeout", f"Timeout must be positive: '{timeout_string}'"))
        return DEFAULT_TIMEOUT_SECONDS
    return total


def parse_authentication(secret_key: str, authentication: dict | None) -> Authentication | UnsupportedValue | None:
    """
    Parses an ADF authentication configuration into an ``Authentication`` object.

    Args:
        secret_key: Secret scope key for the password.
        authentication: Authentication dictionary from the ADF activity, or ``None``.

    Returns:
        Parsed ``Authentication`` or ``None`` when no auth is configured.
    """
    if authentication is None:
        return None
    authentication_type = authentication.get("type")
    if not authentication_type:
        return UnsupportedValue(value=authentication, message="Missing value 'type' for authentication")
    if authentication_type.lower() == "basic":
        username = authentication.get("username", "")
        if not username:
            return UnsupportedValue(value=authentication, message="Missing value 'username' for basic authentication")
        return Authentication(
            auth_type=authentication_type,
            username=username,
            password_secret_key=secret_key,
        )
    return UnsupportedValue(value=authentication, message=f"Unsupported authentication type '{authentication_type}'")


def extract_group(input_string: str, regex: str) -> str | UnsupportedValue:
    """
    Extracts a regex group from an input string.

    Args:
        input_string: Input string to search.
        regex: Regex pattern to match.

    Returns:
        Extracted group as a ``str``.
    """
    match = re.search(pattern=regex, string=input_string)
    if match is None:
        return UnsupportedValue(
            value=input_string, message=f"No match for regex '{regex}' found in input string '{input_string}'"
        )
    return match.group(1)


def get_value_or_unsupported(items: dict, key: str, item_type: str = "dictionary") -> Any | UnsupportedValue:
    """
    Gets a value from a dictionary or returns an ``UnsupportedValue`` object if the key is not found.

    Args:
        items: Dictionary to search.
        key: Key to look up.
        item_type: Item type for error messages (default ``"dictionary"``).

    Returns:
        Value as a ``Any`` or ``UnsupportedValue`` object if the key is not found.
    """
    value = items.get(key)
    if value is None:
        return UnsupportedValue(value=items, message=f"Missing value for key '{key}' in {item_type}")
    return value


def merge_unsupported_values(values: list[Any]) -> UnsupportedValue:
    """
    Merges a list of unsupported values into a single ``UnsupportedValue`` object.

    Args:
        values: List of translated values.

    Returns:
        Single ``UnsupportedValue`` object.
    """
    unsupported = [value for value in values if isinstance(value, UnsupportedValue)]
    if unsupported:
        return UnsupportedValue(value=values, message=";".join([value.message for value in unsupported]))
    raise ValueError("No unsupported values in input list")


def get_data_source_definition(dataset_definitions: list[dict] | UnsupportedValue) -> Dataset | UnsupportedValue:
    """
    Parses the first dataset definition from an activity into a ``Dataset`` object.

    Validates that the definition contains the required ``properties`` and ``type``
    fields before delegating to the dataset translator.

    Args:
        dataset_definitions: Raw dataset definitions list from the ADF activity, or an
            ``UnsupportedValue`` propagated from an earlier validation step.

    Returns:
        Parsed ``Dataset`` or ``UnsupportedValue`` when parsing fails.
    """
    if isinstance(dataset_definitions, UnsupportedValue):
        return dataset_definitions

    if not dataset_definitions:
        return UnsupportedValue(value=dataset_definitions, message="No dataset definition provided")

    dataset = dataset_definitions[0]
    properties = dataset.get("properties")
    if properties is None:
        return UnsupportedValue(value=dataset, message="Missing property 'properties' in dataset definition")

    dataset_type = properties.get("type")
    if dataset_type is None:
        return UnsupportedValue(value=dataset, message="Missing property 'type' in dataset definition")

    if not isinstance(dataset_type, str):
        return UnsupportedValue(
            value=dataset, message=f"Invalid value {dataset_type} for property 'type' in dataset definition"
        )

    dataset_translators = import_module("wkmigrate.translators.dataset_translators")
    return dataset_translators.translate_dataset(dataset)


def get_data_source_properties(data_source_definition: dict | UnsupportedValue) -> dict | UnsupportedValue:
    """
    Parses data-source properties from an ADF activity source or sink block.

    Validates that the definition contains a ``type`` field and delegates to
    ``parse_format_options`` to produce a format-specific options dictionary.

    Args:
        data_source_definition: Source or sink definition from the ADF activity, or an
            ``UnsupportedValue`` propagated from an earlier validation step.

    Returns:
        Data-source properties as a ``dict`` or ``UnsupportedValue`` when parsing fails.
    """
    if isinstance(data_source_definition, UnsupportedValue):
        return data_source_definition

    source_type = data_source_definition.get("type")
    if source_type is None:
        return UnsupportedValue(value=data_source_definition, message="Missing property 'type' in source definition")

    if not isinstance(source_type, str):
        return UnsupportedValue(
            value=data_source_definition,
            message=f"Invalid value {source_type} for property 'type' in source definition",
        )

    dataset_parsers = import_module("wkmigrate.parsers.dataset_parsers")
    return dataset_parsers.parse_format_options(data_source_definition)


def get_placeholder_activity(base_kwargs: dict) -> DatabricksNotebookActivity:
    """
    Creates a placeholder notebook task for unsupported activities.

    Args:
        base_kwargs: Common task metadata.

    Returns:
        Databricks ``NotebookActivity`` object as a placeholder task.
    """
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path="/UNSUPPORTED_ADF_ACTIVITY",
    )


def normalize_translated_result(result: Activity | UnsupportedValue, base_kwargs: dict) -> Activity:
    """
    Normalizes translator results so callers always receive Activities.

    Translators may return an ``UnsupportedValue`` to signal that an activity could not
    be translated. In those cases, this helper converts the unsupported value into a
    placeholder notebook activity so downstream components (such as the workflow
    preparer) continue to operate on ``Activity`` instances only.

    Args:
        result: Activity or UnsupportedValue as an internal representation
        base_kwargs: Activity keyword-arguments

    Returns:
        A placeholder DatabricksNotebookActivity for any UnsupportedValue; Otherwise the input Activity
    """
    if isinstance(result, UnsupportedValue):
        return get_placeholder_activity(base_kwargs)

    return result


def _normalize_activity_type_properties(activity: dict) -> dict:
    type_properties = activity.pop("type_properties", None) or activity.pop("typeProperties", None)
    if isinstance(type_properties, dict):
        for original_key, original_value in type_properties.items():
            converted_key = camel_to_snake(original_key) if isinstance(original_key, str) else original_key
            if converted_key in activity:
                continue
            activity[converted_key] = original_value

    for branch_key in ("if_false_activities", "if_true_activities", "activities"):
        branch = activity.get(branch_key)
        if isinstance(branch, list):
            activity[branch_key] = [
                _normalize_activity_type_properties(dict(nested)) for nested in branch if isinstance(nested, dict)
            ]
    return activity

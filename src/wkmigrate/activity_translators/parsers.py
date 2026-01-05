"""Utility functions for normalizing activity definitions into IR."""

import ast
import re
import warnings
from datetime import datetime, timedelta
from importlib import import_module

from wkmigrate.datasets import dataset_parsers, property_parsers
from wkmigrate.enums.condition_operation_pattern import ConditionOperationPattern
from wkmigrate.models.ir.activities import Activity, ColumnMapping, Dependency
from wkmigrate.models.ir.datasets import Dataset, DatasetProperties
from wkmigrate.not_translatable import NotTranslatableWarning


def parse_dataset(datasets: list[dict]) -> Dataset:
    """
    Parses a dataset definition from a Data Factory pipeline activity into IR.

    Args:
        datasets: Dataset references supplied on the activity.

    Returns:
        Normalized dataset as a ``Dataset`` object.
    """
    dataset = datasets[0]
    properties = dataset.get("properties")
    if properties is None:
        raise ValueError("Dataset properties cannot be None")
    dataset_type = properties.get("type")
    if dataset_type is None:
        raise ValueError("Dataset type cannot be None")
    parser = dataset_parsers.get(dataset_type)
    if parser is None:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")
    return parser(dataset)


def parse_dataset_mapping(mapping: dict) -> list[ColumnMapping]:
    """
    Parses a mapping from one set of data columns to another.

    Args:
        mapping: Data column mapping definition.

    Returns:
        List of column mapping definitions as ``ColumnMapping`` objects.
    """
    return [
        ColumnMapping(
            source_column_name=(mapping.get("source").get("name") or f"_c{mapping.get('source').get('ordinal') - 1}"),
            sink_column_name=mapping.get("sink").get("name"),
            sink_column_type=mapping.get("sink").get("type"),
        )
        for mapping in (mapping.get("mappings") or [])
    ]


def parse_dataset_properties(dataset_definition: dict) -> DatasetProperties:
    """
    Parses dataset properties from an ADF activity definition to a ``DatasetProperties`` object.

    Args:
        dataset_definition: Dataset definition from the activity.

    Returns:
        Dataset properties as a ``DatasetProperties`` object.

    Raises:
        ValueError: If the dataset type is missing or not a string.
        NotTranslatableWarning: If the dataset property type is not supported.
    """
    dataset_type = dataset_definition.get("type")
    if dataset_type is None:
        raise ValueError("Missing dataset type")
    if not isinstance(dataset_type, str):
        raise ValueError("Dataset type must be a string")
    parser = property_parsers.get(dataset_type)
    if parser is None:
        warnings.warn(
            NotTranslatableWarning("dataset_type", f"Unsupported dataset property type: {dataset_type}"), stacklevel=3
        )
        return DatasetProperties(dataset_type=dataset_type, options={})

    return parser(dataset_definition)


def parse_for_each_tasks(tasks: list[dict] | None) -> list[Activity]:
    """
    Parses multiple task definitions within a ForEach task.

    Args:
        tasks: List of nested activity definitions.

    Returns:
        Translated activities as ``Activity`` objects.
    """
    if tasks is None:
        return []
    parsed: list[Activity] = []
    for task in tasks:
        result = _parse_for_each_task(task)
        if result is None:
            continue
        if isinstance(result, tuple):
            parsed.append(result[0])
            parsed.extend(result[1])
            continue
        parsed.append(result)
    return parsed


def parse_for_each_items(items: dict | None) -> str | None:
    """
    Parses a list of items passed to a ForEach task into a serialized list expression.

    Args:
        items: Expression describing ForEach items.

    Returns:
        Serialized list expression understood by Databricks Jobs.
    """
    if items is None:
        return None
    if "value" not in items:
        raise ValueError('For Each task must specify "value" property in "items" list')
    value = items.get("value")
    if value is None:
        return None
    # TODO: Move all dynamic function patterns to a common enum list
    array_pattern = r"@array\('(.+)'\)"
    match = re.match(string=value, pattern=array_pattern)
    if match:
        matched_item = match.group(1)
        return f'["{matched_item}"]'

    create_array_pattern = r"@createArray\((.+)\)"
    match = re.match(string=value, pattern=create_array_pattern)
    if match:
        matched_item = match.group(1)
        list_items = ast.literal_eval(matched_item)
        quoted_items = ",".join([f'"{item}"' for item in list_items])
        return f"[{quoted_items}]"
    return None


def parse_policy(policy: dict | None) -> dict:
    """
    Parses a data factory pipeline activity policy into a dictionary of policy settings.

    Args:
        policy: Activity policy block from the ADF definition.

    Returns:
        Dictionary containing policy settings.

    Raises:
        NotTranslatableWarning: If secure input/output logging is used.
    """
    if policy is None:
        return {}
    cached_policy = policy.get("_wkmigrate_cached_policy")
    if cached_policy is not None:
        return cached_policy
    # Warn about secure input/output logging:
    if "secure_input" in policy:
        warnings.warn(
            NotTranslatableWarning(
                "policy.secure_input",
                "Secure input logging not applicable to Databricks workflows.",
            ),
            stacklevel=2,
        )
    if "secure_output" in policy:
        warnings.warn(
            NotTranslatableWarning(
                "policy.secure_output",
                "Secure output logging not applicable to Databricks workflows.",
            ),
            stacklevel=2,
        )
    # Parse the policy attributes:
    parsed_policy = {}
    # Parse the timeout seconds:
    if "timeout" in policy:
        timeout_value = policy.get("timeout")
        if timeout_value is not None:
            parsed_policy["timeout_seconds"] = _parse_activity_timeout_string(timeout_value)
    # Parse the number of retry attempts:
    if "retry" in policy:
        retry_value = policy.get("retry")
        if retry_value is not None:
            parsed_policy["max_retries"] = int(retry_value)
    # Parse the retry wait time in milliseconds:
    if "retry_interval_in_seconds" in policy:
        parsed_policy["min_retry_interval_millis"] = 1000 * int(policy.get("retry_interval_in_seconds", 0))
    policy["_wkmigrate_cached_policy"] = parsed_policy
    return parsed_policy


def parse_dependencies(dependencies: list[dict] | None) -> list[Dependency] | None:
    """
    Parses a data factory pipeline activity's dependencies.

    Args:
        dependencies: Dependency definitions provided by the activity.

    Returns:
        List of ``Dependency`` objects describing upstream relationships.
    """
    if dependencies is None:
        return None
    # Parse the dependencies from the list:
    parsed_dependencies: list[Dependency] = []
    for dependency in dependencies:
        # Get the dependency condition:
        conditions = dependency.get("dependencyConditions")
        # Validate the dependency conditions:
        if conditions is not None and len(conditions) > 1:
            raise ValueError("Dependencies with multiple conditions are not supported.")
        # Append the dependency:
        parsed_dependencies.append(
            Dependency(
                task_key=dependency.get("activity", None),
                outcome=dependency.get("outcome", None),
            )
        )
    return parsed_dependencies


def parse_notebook_parameters(parameters: dict | None) -> dict | None:
    """
    Parses task parameters in a Databricks notebook activity definition.

    Args:
        parameters: Parameter dictionary from the ADF activity.

    Returns:
        Mapping of parameter names to their default values.

    Raises:
        NotTranslatableWarning: If a parameter cannot be resolved.
    """
    if parameters is None:
        return None
    # Parse the parameters:
    parsed_parameters = {}
    for name, value in parameters.items():
        if not isinstance(value, str):
            warnings.warn(
                NotTranslatableWarning(
                    f"parameters.{name}",
                    f'Could not resolve default value for parameter {name}, setting to ""',
                ),
                stacklevel=2,
            )
            value = ""
        parsed_parameters[name] = value
    return parsed_parameters


def parse_condition_expression(condition: dict) -> dict:
    """
    Parses a condition expression in an If Condition activity definition.

    Args:
        condition: Condition expression dictionary from ADF.

    Returns:
        Dictionary describing the parsed operator and its operands.

    Raises:
        ValueError: If a valid condition expression cannot be parsed.
    """
    # Match a boolean operator:
    condition_value = str(condition.get("value"))
    if not condition_value:
        raise ValueError("Missing condition value")
    for operation in ConditionOperationPattern:
        match = re.match(string=condition_value, pattern=operation.value)
        if match is not None:
            return {
                "op": operation.name,
                "left": match.group(1).replace('"', "").replace("'", ""),
                "right": match.group(2).replace('"', "").replace("'", ""),
            }
    warnings.warn(
        NotTranslatableWarning(
            "condition.value",
            'Condition expression must include "equals", "greaterThan", "greaterThanOrEquals", "lessThan", or "lessThanOrEquals" operation.',
        ),
        stacklevel=3,
    )
    return condition


def _parse_activity_timeout_string(timeout_string: str) -> int:
    """
    Parses a timeout string in the format ``d.hh:mm:ss`` into seconds.

    Args:
        timeout_string: Timeout string from the activity policy.

    Returns:
        Total seconds represented by the timeout.
    """
    if timeout_string[:2] == "0.":
        # Parse the timeout string to HH:MM:SS format:
        timeout_string = timeout_string[2:]
        time_format = "%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
    else:
        # Parse the timeout string to DD.HH:MM:SS format:
        timeout_string = timeout_string.zfill(11)
        time_format = "%d.%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(
            days=date_time.day,
            hours=date_time.hour,
            minutes=date_time.minute,
            seconds=date_time.second,
        )
    return int(time_delta.total_seconds())


def _parse_array_string(array_string: str) -> str:
    """
    Parses an array string into a JSON-safe format.

    Args:
        array_string: Raw array expression emitted by ADF.

    Returns:
        JSON-safe representation of the array.
    """
    double_quote_character = '"'
    single_quote_character = "'"
    test = f"""["{'","'.join([f'{element.replace(single_quote_character, "").replace(double_quote_character, "")}' for element in array_string.split(',')])}"]"""
    return test


def _parse_for_each_task(task: dict | None) -> Activity | tuple[Activity, list[Activity]] | None:
    """
    Parses a single task definition within a ForEach task into an ``Activity`` object and a list of downstream tasks.

    Args:
        task: Nested activity definition from the ADF pipeline.

    Returns:
        Translated activity, optionally paired with additional downstream tasks.
    """
    task_with_filtered_parameters = _filter_parameters(task)
    translator = import_module("wkmigrate.activity_translators.activity_translator")
    return getattr(translator, "translate_activity")(task_with_filtered_parameters)


def _filter_parameters(activity: dict | None) -> dict | None:
    """
    Filters redundant parameters from an activity definition.

    Args:
        activity: Activity definition as a dictionary.

    Returns:
        Filtered activity definition with redundant parameters removed.

    Raises:
        NotTranslatableWarning: If a base parameter is not provided.
    """
    if activity is None:
        return None
    if "base_parameters" not in activity:
        warnings.warn(
            NotTranslatableWarning(
                "for_each.base_parameters",
                "No baseParameters for ForEach inner activity",
            ),
            stacklevel=3,
        )
        return activity
    base_parameters = activity.get("base_parameters")
    if base_parameters is None:
        return activity
    parameters = _filter_parameters(base_parameters)
    if parameters is None:
        return None
    filtered_parameters = {}
    for name, expression in parameters.items():
        if expression is not None and expression.get("value") == "@item()":
            warnings.warn(
                f"Removing redundant parameter {name} with value {expression.get('value')}",
                stacklevel=2,
            )
            continue
        filtered_parameters.update({name: expression})
    activity["base_parameters"] = filtered_parameters
    return activity

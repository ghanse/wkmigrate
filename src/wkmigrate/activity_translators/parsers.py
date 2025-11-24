import ast
import re
import warnings
from datetime import datetime, timedelta
from importlib import import_module
from wkmigrate.datasets import dataset_parsers, property_parsers
from wkmigrate.enums.condition_operation_pattern import ConditionOperationPattern
from wkmigrate.not_translatable import NotTranslatableWarning


def parse_dataset(datasets: list[dict]) -> dict:
    dataset = datasets[0]
    properties = dataset.get("properties")
    if properties is None:
        raise ValueError("Dataset properties cannot be None")
    dataset_type = properties.get("type")
    if dataset_type is None:
        raise ValueError("Dataset type cannot be None")
    return dataset_parsers[dataset_type](dataset)


def parse_dataset_mapping(mapping: dict) -> list[dict]:
    """Parses a mapping from one set of data columns to another.
    :parameter mapping: Data column mapping as a ``dict``
    :return: Parsed data column mapping as a ``list[dict]``
    """
    return [
        {
            "source_column_name": mapping.get("source").get("name") or f"_c{mapping.get('source').get('ordinal') - 1}",
            "sink_column_name": mapping.get("sink").get("name"),
            "sink_column_type": mapping.get("sink").get("type"),
        }
        for mapping in (mapping.get("mappings") or [])
    ]


def parse_dataset_properties(dataset_definition: dict) -> dict:
    """Parses various properties (e.g. query timeout, isolation level) from an input dataset definition.
    :parameter dataset_definition: Dataset definition as a ``dict``
    :return: Parsed dataset properties as a ``dict``
    """
    dataset_type = dataset_definition.get("type")
    if dataset_type is None:
        raise ValueError("Dataset type cannot be None")
    return property_parsers[dataset_type](dataset_definition)


def parse_for_each_tasks(tasks: list[dict] | None) -> list[dict] | None:
    """Parses multiple task definitions within a ForEach task to a common object model.
    :parameter tasks: List of source pipeline ForEach task definitions as ``dict`` objects
    :return: List of common object model ForEach task definitions as ``dict`` objects
    """
    if tasks is None:
        return None
    parsed_tasks = [_parse_for_each_task(task) for task in tasks]
    return [task for task in parsed_tasks if task is not None]


def parse_for_each_items(items: dict | None) -> str | None:
    """Parses a list of items passed to a ForEach task to a common object model.
    :parameter items: Set of values passed to a ForEach activity as a ``str``.
    :return: List of values to pass to the for-each task
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
    """Parses a data factory pipeline activity policy to a common object model.
    :parameter policy: Dictionary definition of the source pipeline activity policy
    :return: Dictionary definition of the policy settings
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


def parse_dependencies(dependencies: list[dict] | None) -> list[dict] | None:
    """Parses a data factory pipeline activity's dependencies to a common object model.
    :parameter dependencies: Dictionary definition of the source pipeline activity's dependencies
    :return: Dictionary definition of the task-level parameter definitions
    """
    if dependencies is None:
        return None
    # Parse the dependencies from the list:
    parsed_dependencies = []
    for dependency in dependencies:
        # Get the dependency condition:
        conditions = dependency.get("dependencyConditions")
        # Validate the dependency conditions:
        if conditions is not None and len(conditions) > 1:
            raise ValueError("Dependencies with multiple conditions are not supported.")
        # Append the dependency:
        parsed_dependencies.append(
            {
                "task_key": dependency.get("activity", None),
                "outcome": dependency.get("outcome", None),
            }
        )
    return parsed_dependencies


def parse_notebook_parameters(parameters: dict | None) -> dict | None:
    """Parses task parameters in a Databricks notebook activity definition from Data Factory's object model to a
    set of key/value pairs in the Databricks SDK object model.
    :parameter parameters: Set of task parameters in Data Factory as a ``dict[str, Any]``
    :return: Set of task parameters as a ``dict[str, str]``
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
    """Parses a condition expression in an If Condition activity definition from Data Factory's object model to the
    Databricks SDK object model.
    :parameter condition: Condition expression in Data Factory as a ``dict[str, Any]``
    :return: Condition expression as a ``dict[str, str]``
    """
    # Validate the condition:
    if "value" not in condition:
        raise ValueError("Condition expression must include a valid conditional value")
    # Match a boolean operator:
    condition_value = condition.get("value")
    if condition_value is None:
        raise ValueError("Condition value cannot be None")
    for op in ConditionOperationPattern:
        match = re.match(string=condition_value, pattern=op.value)
        if match is not None:
            return {
                "op": op.name,
                "left": match.group(1).replace('"', "").replace("'", ""),
                "right": match.group(2).replace('"', "").replace("'", ""),
            }
    raise ValueError(
        'Condition expression must include "equals", "greaterThan", "greaterThanOrEquals", "lessThan", or '
        '"lessThanOrEquals" operation.'
    )


def _parse_activity_timeout_string(timeout_string: str) -> int:
    """Parses a timeout string in the format ``d.hh:mm:ss`` into an integer number of seconds.
    :parameter timeout_string: Timeout string in the format ``d.hh:mm:ss``
    :return: Integer number of seconds
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
    """Parses an array string into a JSON-safe format.
    :parameter array_string: Array values as a ``str``
    :return: Array values as a JSON-safe ``str``
    """
    double_quote_character = '"'
    single_quote_character = "'"
    test = f"""["{'","'.join([f'{element.replace(single_quote_character, "").replace(double_quote_character, "")}' for element in array_string.split(',')])}"]"""
    print(test)
    return test


def _parse_for_each_task(task: dict | None) -> dict | None:
    """Parses a single task definition within a ForEach task to a common object model.
    :parameter task: Source pipeline definition of the ForEach task as a ``dict``
    :return: Common object model for each task definition as a ``dict``
    """
    task_with_filtered_parameters = _filter_parameters(task)
    translator = import_module("wkmigrate.activity_translators.activity_translator")
    return getattr(translator, "translate_activity")(task_with_filtered_parameters)


def _filter_parameters(activity: dict | None) -> dict | None:
    if activity is None:
        return None
    if "base_parameters" not in activity:
        warnings.warn(
            NotTranslatableWarning(
                "for_each.base_parameters",
                "No baseParameters for ForEach inner activity",
            ),
            stacklevel=2,
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
                NotTranslatableWarning(
                    f"for_each.parameters.{name}",
                    f"Removing redundant parameter {name} with value {expression.get('value')}",
                ),
                stacklevel=2,
            )
            continue
        filtered_parameters.update({name: expression})
    activity["base_parameters"] = filtered_parameters
    return activity

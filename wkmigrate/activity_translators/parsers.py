import re
import warnings
from datetime import datetime, timedelta
from importlib import import_module
from typing import Optional
from wkmigrate.enums.condition_operation_pattern import ConditionOperationPattern
from wkmigrate.enums.isolation_level import IsolationLevel


def parse_data_column_mapping(mapping: dict) -> list[dict]:
    """ Parses a mapping from one set of data columns to another.
        :parameter mapping: Data column mapping as a ``dict``
        :return: Parsed data column mapping as a ``list[dict]``
    """
    return [{
        'source_column_name': mapping.get('source').get('name'),
        'sink_column_name': mapping.get('sink').get('name'),
        'sink_column_type': mapping.get('sink').get('type')
    } for mapping in mapping.get('mappings')]


def parse_query_timeout_seconds(properties: Optional[dict]) -> int:
    """ Parses the timeout number of seconds from the dataset properties.
        :parameter properties: Dataset properties as a ``dict``
        :return: Timeout seconds as an ``int``
    """
    if properties is None or 'query_timeout' not in properties:
        return 0
    return _parse_query_timeout_string(properties.get('query_timeout'))


def parse_query_isolation_level(properties: Optional[dict]) -> Optional[str]:
    """ Parses the database transaction isolation level from the dataset properties.
        :parameter properties: Dataset properties as a ``dict``
        :return: Isolation level as an ``str``
    """
    if properties is None or 'isolation_level' not in properties:
        return 'READ_COMMITTED'
    return IsolationLevel[properties.get('isolation_level')]


def parse_for_each_tasks(tasks: Optional[list[dict]]) -> Optional[list[dict]]:
    """ Parses multiple task definitions within a ForEach task to a common object model.
        :parameter tasks: List of source pipeline ForEach task definitions as ``dict`` objects
        :return: List of common object model ForEach task definitions as ``dict`` objects
    """
    return [_parse_for_each_task(task) for task in tasks]


def parse_for_each_items(items: Optional[dict]) -> Optional[str]:
    """ Parses a list of items passed to a ForEach task to a common object model.
        :parameter items: Set of values passed to a ForEach activity as a ``str``.
        :return: List of values to pass to the for-each task
    """
    if 'value' not in items:
        raise ValueError('For Each task must specify "value" property in "items" list')
    value = items.get('value')
    # TODO: Move all dynamic function patterns to a common enum list
    array_pattern = r'@array\((.+)\)'
    if re.match(string=value, pattern=array_pattern):
        inputs = re.match(string=value, pattern=array_pattern).group(1)
        return _parse_array_string(inputs)
    return None


def parse_policy(policy: Optional[dict]) -> dict:
    """ Parses a data factory pipeline activity policy to a common object model.
        :parameter policy: Dictionary definition of the source pipeline activity policy
        :return: Dictionary definition of the policy settings
    """
    if policy is None:
        return {}
    # Warn about secure input/output logging:
    if 'secure_input' in policy:
        warnings.warn('Secure input logging not applicable to Databricks workflows.', stacklevel=2)
    if 'secure_output' in policy:
        warnings.warn('Secure output logging not applicable to Databricks workflows.', stacklevel=2)
    # Parse the policy attributes:
    parsed_policy = {}
    # Parse the timeout seconds:
    if 'timeout' in policy:
        parsed_policy['timeout_seconds'] = _parse_activity_timeout_string(policy.get('timeout'))
    # Parse the number of retry attempts:
    if 'retry' in policy:
        parsed_policy['max_retries'] = int(policy.get('retry'))
    # Parse the retry wait time in milliseconds:
    if 'retry_interval_in_seconds' in policy:
        parsed_policy['min_retry_interval_millis'] = 1000 * int(policy.get('retry_interval_in_seconds', 0))
    return parsed_policy


def parse_dependencies(dependencies: Optional[list[dict]]) -> Optional[list[dict]]:
    """ Parses a data factory pipeline activity's dependencies to a common object model.
        :parameter dependencies: Dictionary definition of the source pipeline activity's dependencies
        :return: Dictionary definition of the task-level parameter definitions
    """
    if dependencies is None:
        return None
    # Parse the dependencies from the list:
    parsed_dependencies = []
    for dependency in dependencies:
        # Get the dependency condition:
        conditions = dependency.get('dependencyConditions')
        # Validate the dependency conditions:
        if conditions is not None and len(conditions) > 1:
            raise ValueError('Dependencies with multiple conditions are not supported.')
        # Append the dependency:
        parsed_dependencies.append({
            'task_key': dependency.get('activity', None),
            'outcome': dependency.get('outcome', None)
        })
    return parsed_dependencies


def parse_notebook_parameters(parameters: Optional[dict]) -> Optional[dict]:
    """ Parses task parameters in a Databricks notebook activity definition from Data Factory's object model to a
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
            warnings.warn(f'Could not resolve default value for parameter {name}, setting to ""', stacklevel=2)
            value = ""
        parsed_parameters[name] = value
    return parsed_parameters


def parse_condition_expression(condition: dict) -> dict:
    """ Parses a condition expression in an If Condition activity definition from Data Factory's object model to the
        Databricks SDK object model.
        :parameter condition: Condition expression in Data Factory as a ``dict[str, Any]``
        :return: Condition expression as a ``dict[str, str]``
    """
    # Validate the condition:
    if 'value' not in condition:
        raise ValueError('Condition expression must include a valid conditional value')
    # Match a boolean operator:
    for op in ConditionOperationPattern:
        match = re.match(string=condition.get('value'), pattern=op.value)
        if match is not None:
            return {
                'op': op.name,
                'left': match.group(1).replace('"', '').replace("'", ""),
                'right': match.group(2).replace('"', '').replace("'", "")
            }
    raise ValueError('Condition expression must include "equals", "greaterThan", "greaterThanOrEquals", "lessThan", or '
                     '"lessThanOrEquals" operation.')


def _parse_query_timeout_string(timeout_string: str) -> int:
    """ Parses a timeout string in the format ``hh:mm:ss`` into an integer number of seconds.
        :parameter timeout_string: Timeout string in the format ``hh:mm:ss``
        :return: Integer number of seconds
    """
    time_format = '%H:%M:%S'
    date_time = datetime.strptime(timeout_string, time_format)
    time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
    return int(time_delta.total_seconds())


def _parse_activity_timeout_string(timeout_string: str) -> int:
    """ Parses a timeout string in the format ``d.hh:mm:ss`` into an integer number of seconds.
        :parameter timeout_string: Timeout string in the format ``d.hh:mm:ss``
        :return: Integer number of seconds
    """
    if timeout_string[:2] == '0.':
        # Parse the timeout string to HH:MM:SS format:
        timeout_string = timeout_string[2:]
        time_format = '%H:%M:%S'
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)
    else:
        # Parse the timeout string to DD.HH:MM:SS format:
        timeout_string = timeout_string.zfill(11)
        time_format = '%d.%H:%M:%S'
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(days=date_time.day, hours=date_time.hour,
                               minutes=date_time.minute, seconds=date_time.second)
    return int(time_delta.total_seconds())


def _parse_array_string(array_string: str) -> str:
    """ Parses an array string into a JSON-safe format.
        :parameter array_string: Array values as a ``str``
        :return: Array values as a JSON-safe ``str``
    """
    return f'[{','.join([f'"{element.replace("'", "").replace('"', '')}"' 
                         for element in array_string.split(',')])}]'


def _parse_for_each_task(task: Optional[dict]) -> Optional[dict]:
    """ Parses a single task definition within a ForEach task to a common object model.
        :parameter task: Source pipeline definition of the ForEach task as a ``dict``
        :return: Common object model for each task definition as a ``dict``
    """
    task_with_filtered_parameters = _filter_parameters(task)
    translator = import_module('wkmigrate.activity_translators.activity_translator')
    return getattr(translator, 'translate_activity')(task_with_filtered_parameters)


def _filter_parameters(activity: Optional[dict]) -> Optional[dict]:
    if 'base_parameters' not in activity:
        warnings.warn('No baseParameters for ForEach inner activity', stacklevel=2)
        return activity
    parameters = _filter_parameters(activity.get('base_parameters'))
    if parameters is None:
        return None
    filtered_parameters = {}
    for name, expression in parameters.items():
        if expression.get('value') == '@item()':
            warnings.warn(f'Removing redundant parameter {name} with value {expression.get('value')}')
            continue
        filtered_parameters.update({name: expression})
    activity['base_parameters'] = filtered_parameters
    return activity

import pytest
from contextlib import nullcontext as does_not_raise
from wkmigrate.activity_translators.activity_translator import (
    translate_activities,
    translate_activity,
    parse_activity_properties,
)


class TestActivityTranslator:
    """Unit tests for the activity translator methods."""

    @pytest.mark.parametrize(
        "activity_definition, expected_result",
        [
            (None, None),
            ([], []),
            (
                [
                    {
                        "type": "DatabricksNotebook",
                        "name": "Activity1",
                        "description": "Test activity",
                        "policy": {
                            "timeout": "7.00:00:00",
                            "retry": 3,
                            "retry_interval_in_seconds": 30,
                        },
                        "notebook_path": "/path/to/notebook",
                        "base_parameters": {"param": "val"},
                    }
                ],
                [
                    {
                        "type": "DatabricksNotebook",
                        "task_key": "Activity1",
                        "description": "Test activity",
                        "timeout_seconds": 604800,
                        "max_retries": 3,
                        "min_retry_interval_millis": 30000,
                        "notebook_task": {
                            "notebook_path": "/path/to/notebook",
                            "base_parameters": {"param": "val"},
                        },
                    }
                ],
            ),
            (
                [
                    {
                        "type": "DatabricksNotebook",
                        "name": "Activity1",
                        "description": "Test activity",
                        "policy": {
                            "timeout": "7.00:00:00",
                            "retry": 3,
                            "retry_interval_in_seconds": 30,
                        },
                        "depends_on": [{"activity": "PreviousActivity"}],
                        "notebook_path": "/path/to/notebook",
                    }
                ],
                [
                    {
                        "type": "DatabricksNotebook",
                        "task_key": "Activity1",
                        "description": "Test activity",
                        "timeout_seconds": 604800,
                        "max_retries": 3,
                        "min_retry_interval_millis": 30000,
                        "depends_on": [{"task_key": "PreviousActivity", "outcome": None}],
                        "notebook_task": {"notebook_path": "/path/to/notebook"},
                    }
                ],
            ),
            (
                [
                    {
                        "type": "DatabricksNotebook",
                        "description": "Test activity",
                        "policy": {
                            "timeout": "7.00:00:00",
                            "retry": 3,
                            "retry_interval_in_seconds": 30,
                        },
                        "depends_on": [
                            {
                                "activity": "PreviousActivity",
                                "dependency_conditions": ["Succeeded"],
                            }
                        ],
                        "notebook_path": "/path/to/notebook",
                    },
                    {
                        "type": "DatabricksNotebook",
                        "description": "Test activity",
                        "policy": {
                            "timeout": "7.00:00:00",
                            "retry": 3,
                            "retry_interval_in_seconds": 30,
                        },
                        "depends_on": [
                            {
                                "activity": "PreviousActivity",
                                "dependency_conditions": ["Succeeded"],
                            }
                        ],
                        "notebook_path": "/path/to/notebook",
                    },
                ],
                [
                    {
                        "type": "DatabricksNotebook",
                        "task_key": "TASK_NAME_NOT_PROVIDED",
                        "description": "Test activity",
                        "timeout_seconds": 604800,
                        "max_retries": 3,
                        "min_retry_interval_millis": 30000,
                        "depends_on": [{"task_key": "PreviousActivity", "outcome": None}],
                        "notebook_task": {"notebook_path": "/path/to/notebook"},
                    },
                    {
                        "type": "DatabricksNotebook",
                        "task_key": "TASK_NAME_NOT_PROVIDED",
                        "description": "Test activity",
                        "timeout_seconds": 604800,
                        "max_retries": 3,
                        "min_retry_interval_millis": 30000,
                        "depends_on": [{"task_key": "PreviousActivity", "outcome": None}],
                        "notebook_task": {"notebook_path": "/path/to/notebook"},
                    },
                ],
            ),
        ],
    )
    def test_translate_activities_parses_results(self, activity_definition, expected_result):
        result = translate_activities(activity_definition)
        assert result == expected_result

    @pytest.mark.parametrize(
        "activity_definition, expected_result, context",
        [
            (
                {
                    "type": "DatabricksNotebook",
                    "name": "Activity1",
                    "description": "Test activity",
                    "policy": {
                        "timeout": "7.00:00:00",
                        "retry": 3,
                        "retry_interval_in_seconds": 30,
                    },
                    "depends_on": [],
                    "notebook_path": "/path/to/notebook",
                },
                {
                    "type": "DatabricksNotebook",
                    "task_key": "Activity1",
                    "description": "Test activity",
                    "timeout_seconds": 604800,
                    "max_retries": 3,
                    "min_retry_interval_millis": 30000,
                    "depends_on": [],
                    "notebook_task": {"notebook_path": "/path/to/notebook"},
                },
                does_not_raise(),
            ),
            (
                {
                    "type": "IfCondition",
                    "name": "IfConditionActivity",
                    "description": "Test if-else condition activity",
                    "expression": {
                        "type": "Expression",
                        "value": '@equals("true", "true")',
                    },
                },
                {
                    "type": "IfCondition",
                    "task_key": "IfConditionActivity",
                    "description": "Test if-else condition activity",
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": '"true"',
                        "right": '"true"',
                    },
                },
                pytest.warns(UserWarning),
            ),
        ],
    )
    def test_translate_activity_parses_result(self, activity_definition, expected_result, context):
        with context:
            result = translate_activity(activity_definition)
            assert result == expected_result

    @pytest.mark.parametrize(
        "activity_properties, expected_result",
        [
            (
                {"type": "DatabricksNotebook", "notebook_path": "/path/to/notebook"},
                {"notebook_task": {"notebook_path": "/path/to/notebook"}},
            ),
            (
                {
                    "type": "DatabricksNotebook",
                    "notebook_path": "/path/to/notebook",
                    "base_parameters": {"param_name": "param_value"},
                },
                {
                    "notebook_task": {
                        "notebook_path": "/path/to/notebook",
                        "base_parameters": {"param_name": "param_value"},
                    }
                },
            ),
        ],
    )
    def test_parse_activity_properties_parses_result(self, activity_properties, expected_result):
        print(activity_properties)
        result = parse_activity_properties(activity_properties)
        assert result == expected_result

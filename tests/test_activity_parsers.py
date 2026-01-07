from contextlib import nullcontext as does_not_raise
import pytest
from wkmigrate.activity_translators.parsers import (
    parse_dataset_mapping,
    parse_for_each_items,
    parse_policy,
    parse_dependencies,
    parse_notebook_parameters,
    parse_condition_expression,
    _parse_activity_timeout_string,
    _parse_array_string,
)
from wkmigrate.models.ir.activities import ColumnMapping, Dependency


class TestActivityParsers:
    """Unit tests for the activity parsing methods."""

    @pytest.mark.parametrize(
        "column_mapping, expected_result, context",
        [
            (None, None, pytest.raises(AttributeError, match="'NoneType' object has no attribute 'get'")),
            ({}, [], does_not_raise()),
            (
                {
                    "mappings": [
                        {"source": {"name": "col1", "type": "string"}, "sink": {"name": "col1", "type": "string"}}
                    ]
                },
                [ColumnMapping(source_column_name="col1", sink_column_name="col1", sink_column_type="string")],
                does_not_raise(),
            ),
        ],
    )
    def test_parse_column_mappings(self, column_mapping, expected_result, context):
        with context:
            assert parse_dataset_mapping(column_mapping) == expected_result

    @pytest.mark.parametrize(
        "for_each_items, expected_result, context",
        [
            ({"value": "@array('1,2,3')"}, '["1","2","3"]', does_not_raise()),
            ({"value": '@array(\'"a","b","c"\')'}, '["a","b","c"]', does_not_raise()),
            ({"value": "not_an_array"}, None, does_not_raise()),
        ],
    )
    def test_parse_for_each_items(self, for_each_items, expected_result, context):
        with context:
            assert parse_for_each_items(for_each_items) == expected_result

    @pytest.mark.parametrize(
        "policy_definition, expected_result, context",
        [
            (None, {}, does_not_raise()),
            (
                {"timeout": "0.01:30:00", "retry": 3, "retry_interval_in_seconds": 60},
                {
                    "timeout_seconds": 5400,
                    "max_retries": 3,
                    "min_retry_interval_millis": 60000,
                },
                does_not_raise(),
            ),
        ],
    )
    def test_parse_policy(self, policy_definition, expected_result, context):
        with context:
            assert parse_policy(policy_definition) == expected_result

    @pytest.mark.parametrize(
        "dependencies, expected_result, context",
        [
            (None, None, does_not_raise()),
            (
                [{"activity": "Task1", "dependencyConditions": ["Succeeded"]}],
                [Dependency(task_key="Task1", outcome=None)],
                does_not_raise(),
            ),
            (
                [
                    {
                        "activity": "Task1",
                        "dependencyConditions": ["Succeeded", "Completed"],
                    }
                ],
                None,
                pytest.raises(ValueError),
            ),
        ],
    )
    def test_parse_dependencies(self, dependencies, expected_result, context):
        with context:
            assert parse_dependencies(dependencies) == expected_result

    @pytest.mark.parametrize(
        "notebook_parameters, expected_result",
        [
            (None, None),
            (
                {"param1": "value1", "param2": "value2"},
                {"param1": "value1", "param2": "value2"},
            ),
            (
                {"param1": "value1", "param2": {"expression": "@this()"}},
                {"param1": "value1", "param2": ""},
            ),
        ],
    )
    def test_parse_notebook_parameters(self, notebook_parameters, expected_result):
        assert parse_notebook_parameters(notebook_parameters) == expected_result

    @pytest.mark.parametrize(
        "condition_expression, expected_result, context",
        [
            ({"expression": "@equals(1, 1)"}, {"expression": "@equals(1, 1)"}, pytest.warns(UserWarning)),
            (
                {"value": "@equals(1, 1)"},
                {"op": "EQUAL_TO", "left": "1", "right": "1"},
                does_not_raise(),
            ),
            (
                {"value": "@greater(2, 1)"},
                {"op": "GREATER_THAN", "left": "2", "right": "1"},
                does_not_raise(),
            ),
        ],
    )
    def test_parse_condition_expression(self, condition_expression, expected_result, context):
        with context:
            assert parse_condition_expression(condition_expression) == expected_result

    @pytest.mark.parametrize(
        "timeout_string, expected_result",
        [
            ("0.00:05:00", 300),
            ("1.02:30:00", 95400),
        ],
    )
    def test_parse_timeout_string(self, timeout_string, expected_result):
        assert _parse_activity_timeout_string(timeout_string) == expected_result

    @pytest.mark.parametrize(
        "array_string, expected_result",
        [
            ("1,2,3", '["1","2","3"]'),
            ("'a','b','c'", '["a","b","c"]'),
        ],
    )
    def test_parse_array_string(self, array_string, expected_result):
        assert _parse_array_string(array_string) == expected_result

    @pytest.mark.parametrize(
        "items",
        [
            {"invalid_key": "@array(1,2,3)"},
        ],
    )
    def test_parse_for_each_items_error(self, items):
        with pytest.raises(ValueError):
            parse_for_each_items(items)

    @pytest.mark.parametrize(
        "condition",
        [
            {"value": "@invalidOperation(1, 1)"},
        ],
    )
    def test_parse_condition_expression_error(self, condition):
        with pytest.warns(UserWarning):
            assert parse_condition_expression(condition) == condition

import pytest
from contextlib import nullcontext as does_not_raise
from wkmigrate.activity_translators.parsers import (
    parse_column_mappings,
    parse_query_timeout_seconds,
    parse_query_isolation_level,
    parse_for_each_items,
    parse_policy,
    parse_dependencies,
    parse_notebook_parameters,
    parse_condition_expression,
    _parse_timeout_string,
    _parse_array_string,
)
from wkmigrate.enums.isolation_level import IsolationLevel


class TestActivityParsers:
    """Unit tests for the activity parsing methods."""

    @pytest.mark.parametrize(
        "column_mapping, expected_result, context",
        [
            (None, None, does_not_raise()),
            ({}, None, does_not_raise()),
            (
                {"mappings": [{"source": "col1", "sink": "col1"}]},
                [{"source": "col1", "sink": "col1"}],
                does_not_raise(),
            ),
        ],
    )
    def test_parse_column_mappings(self, column_mapping, expected_result, context):
        with context:
            assert parse_column_mappings(column_mapping) == expected_result

    @pytest.mark.parametrize(
        "query_timeout_definition, expected_result, context",
        [
            (None, 0, does_not_raise()),
            ({}, 0, does_not_raise()),
            ({"query_timeout": "0.00:05:00"}, 300, does_not_raise()),
            ({"query_timeout": "1.02:30:00"}, 95400, does_not_raise()),
        ],
    )
    def test_parse_query_timeout_seconds(self, query_timeout_definition, expected_result, context):
        with context:
            assert parse_query_timeout_seconds(query_timeout_definition) == expected_result

    @pytest.mark.parametrize(
        "query_isolation_level, expected_result, context",
        [
            (None, "READ_COMMITTED", does_not_raise()),
            ({}, "READ_COMMITTED", does_not_raise()),
            (
                {"isolation_level": "ReadCommitted"},
                IsolationLevel.ReadCommitted.value,
                does_not_raise(),
            ),
            (
                {"isolation_level": "Serializable"},
                IsolationLevel.Serializable.value,
                does_not_raise(),
            ),
        ],
    )
    def test_parse_query_isolation_level(self, query_isolation_level, expected_result, context):
        with context:
            assert parse_query_isolation_level(query_isolation_level) == expected_result

    @pytest.mark.parametrize(
        "for_each_items, expected_result, context",
        [
            ({"value": "@array(1,2,3)"}, '["1","2","3"]', does_not_raise()),
            ({"value": '@array("a","b","c")'}, '["a","b","c"]', does_not_raise()),
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
                [{"task_key": "Task1", "outcome": None}],
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
            ({"expression": "@equals(1, 1)"}, None, pytest.raises(ValueError)),
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
        assert _parse_timeout_string(timeout_string) == expected_result

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
        with pytest.raises(ValueError):
            parse_condition_expression(condition)

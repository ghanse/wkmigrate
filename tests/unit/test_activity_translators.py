"""Comprehensive tests for activity translators using JSON fixtures.

This module tests all activity translators against realistic ADF payloads
loaded from JSON fixture files. Each test case includes input payloads
and expected IR outputs for validation.
"""

from __future__ import annotations

import warnings

import pytest

from tests.conftest import get_base_kwargs, get_fixture
from wkmigrate.models.ir.pipeline import (
    CopyActivity,
    Authentication,
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    WebActivity,
)
from wkmigrate.translators.activity_translators.databricks_job_activity_translator import (
    translate_databricks_job_activity,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.activity_translators.activity_translator import (
    default_context,
    translate_activities,
    translate_activities_with_context,
    translate_activity,
    visit_activity,
)
from wkmigrate.translators.activity_translators.for_each_activity_translator import (
    translate_for_each_activity,
)
from wkmigrate.translators.activity_translators.if_condition_activity_translator import (
    translate_if_condition_activity,
)
from wkmigrate.translators.activity_translators.notebook_activity_translator import (
    translate_notebook_activity,
)
from wkmigrate.translators.activity_translators.spark_jar_activity_translator import (
    translate_spark_jar_activity,
)
from wkmigrate.translators.activity_translators.lookup_activity_translator import (
    translate_lookup_activity,
)
from wkmigrate.translators.activity_translators.web_activity_translator import translate_web_activity
from wkmigrate.translators.activity_translators.set_variable_activity_translator import (
    translate_set_variable_activity,
)
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    translate_spark_python_activity,
)
from wkmigrate.translators.activity_translators.copy_activity_translator import (
    translate_copy_activity,
)
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.parsers.expression_parsers import parse_variable_value
from wkmigrate.utils import get_placeholder_activity

NOTEBOOK_ACTIVITY: dict = {
    "name": "nb_task",
    "type": "DatabricksNotebook",
    "depends_on": [],
    "policy": {"timeout": "0.01:00:00"},
    "notebook_path": "/notebooks/etl",
}

SPARK_JAR_ACTIVITY: dict = {
    "name": "jar_task",
    "type": "DatabricksSparkJar",
    "depends_on": [{"activity": "nb_task", "dependency_conditions": ["Succeeded"]}],
    "policy": {"timeout": "0.02:00:00"},
    "main_class_name": "com.example.Main",
}

SET_VARIABLE_ACTIVITY: dict = {
    "name": "set_my_var",
    "type": "SetVariable",
    "depends_on": [],
    "variable_name": "myVar",
    "value": "static_value",
}


def test_basic_notebook_activity(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic notebook activity."""
    fixture = get_fixture(notebook_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.notebook_path == fixture["expected"]["notebook_path"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]


def test_notebook_with_parameters(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with parameters."""
    fixture = get_fixture(notebook_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters == fixture["expected"]["base_parameters"]


def test_notebook_with_dependency(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with upstream dependency."""
    fixture = get_fixture(notebook_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "upstream_task"


def test_notebook_with_linked_service(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with cluster configuration."""
    fixture = get_fixture(notebook_activity_fixtures, "with_linked_service")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.new_cluster is not None
    assert result.new_cluster.service_name == "databricks-cluster-001"
    assert result.new_cluster.autoscale == {"min_workers": 2, "max_workers": 8}


def test_notebook_secure_io_warns(notebook_activity_fixtures: list[dict]) -> None:
    """Test that secure input/output settings emit warnings."""
    fixture = get_fixture(notebook_activity_fixtures, "secure_io")

    with pytest.warns(UserWarning):
        result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)


def test_notebook_missing_path_returns_unsupported(notebook_activity_fixtures: list[dict]) -> None:
    """Test that missing notebook_path returns UnsupportedValue."""
    fixture = get_fixture(notebook_activity_fixtures, "missing_notebook_path")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_notebook_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_notebook_expression_parameters_warns(notebook_activity_fixtures: list[dict]) -> None:
    """Test that expression parameters emit warnings and are set to empty string."""
    fixture = get_fixture(notebook_activity_fixtures, "expression_parameters")

    with pytest.warns(UserWarning):
        result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters["expression_param"] == ""


def test_basic_spark_jar_activity(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Spark JAR activity."""
    fixture = get_fixture(spark_jar_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.main_class_name == fixture["expected"]["main_class_name"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]


def test_spark_jar_with_parameters(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with parameters."""
    fixture = get_fixture(spark_jar_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.parameters == fixture["expected"]["parameters"]


def test_spark_jar_with_libraries(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with libraries."""
    fixture = get_fixture(spark_jar_activity_fixtures, "with_libraries")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.libraries is not None
    assert len(result.libraries) == 7  # jar, jar, maven, pypi, whl, egg, cran


def test_spark_jar_with_dependency(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with dependency."""
    fixture = get_fixture(spark_jar_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.depends_on is not None
    assert len(result.depends_on) == 1


def test_spark_jar_missing_main_class_returns_unsupported(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test that missing main_class_name returns UnsupportedValue."""
    fixture = get_fixture(spark_jar_activity_fixtures, "missing_main_class")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_spark_jar_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_basic_spark_python_activity(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Spark Python activity."""
    fixture = get_fixture(spark_python_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.python_file == fixture["expected"]["python_file"]


def test_spark_python_with_parameters(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with parameters."""
    fixture = get_fixture(spark_python_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.parameters == fixture["expected"]["parameters"]


def test_spark_python_with_dependency(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with dependency."""
    fixture = get_fixture(spark_python_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.depends_on is not None
    assert result.depends_on[0].task_key == "ingest_data"


def test_spark_python_workspace_path(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with workspace file path."""
    fixture = get_fixture(spark_python_activity_fixtures, "workspace_file_path")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.python_file.startswith("/Workspace")


def test_spark_python_missing_file_returns_unsupported(spark_python_activity_fixtures: list[dict]) -> None:
    """Test that missing python_file returns UnsupportedValue."""
    fixture = get_fixture(spark_python_activity_fixtures, "missing_python_file")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_spark_python_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_single_inner_activity(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with single inner activity creates direct task."""
    fixture = get_fixture(for_each_activity_fixtures, "single_inner_notebook")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]
    assert result.concurrency == fixture["expected"]["concurrency"]
    assert isinstance(result.for_each_task, DatabricksNotebookActivity)


def test_foreach_createarray_expression(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with createArray expression."""
    fixture = get_fixture(for_each_activity_fixtures, "create_array")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]


def test_foreach_multiple_inner_activities_creates_run_job(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with multiple inner activities creates RunJobActivity."""
    fixture = get_fixture(for_each_activity_fixtures, "multiple_inner_activities")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert isinstance(result.for_each_task, RunJobActivity)
    assert result.for_each_task.name == fixture["expected"]["inner_pipeline_name"]


def test_foreach_spark_jar_inner_activity(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with Spark JAR inner activity."""
    fixture = get_fixture(for_each_activity_fixtures, "spark_jar_inner")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert isinstance(result.for_each_task, SparkJarActivity)


def test_foreach_missing_items_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that missing items returns UnsupportedValue."""
    fixture = get_fixture(for_each_activity_fixtures, "missing_items")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_empty_activities_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that empty activities array returns UnsupportedValue."""
    fixture = get_fixture(for_each_activity_fixtures, "empty_activities")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_unsupported_items_expression_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that unsupported items expression returns UnsupportedValue."""
    fixture = get_fixture(for_each_activity_fixtures, "unsupported_items")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_equals_both_branches(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with equals expression and both branches."""
    fixture = get_fixture(if_condition_activity_fixtures, "equals_both_branches")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]
    assert result.left == fixture["expected"]["left"]
    assert result.right == fixture["expected"]["right"]
    assert len(result.child_activities) == fixture["expected"]["child_activities_count"]


def test_if_condition_only_true_branch(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with only if_true branch."""
    fixture = get_fixture(if_condition_activity_fixtures, "only_true_branch")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert len(result.child_activities) == fixture["expected"]["child_activities_count"]


def test_if_condition_greater_than(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with greater than expression."""
    fixture = get_fixture(if_condition_activity_fixtures, "greater_than")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]


def test_if_condition_less_than(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with less than expression."""
    fixture = get_fixture(if_condition_activity_fixtures, "less_than")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]


def test_if_condition_nested_foreach(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with nested ForEach in false branch."""
    fixture = get_fixture(if_condition_activity_fixtures, "nested_foreach")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    has_foreach = any(isinstance(child, ForEachActivity) for child in result.child_activities)
    assert has_foreach


def test_if_condition_missing_expression_returns_unsupported(if_condition_activity_fixtures: list[dict]) -> None:
    """Test that missing expression returns UnsupportedValue."""
    fixture = get_fixture(if_condition_activity_fixtures, "missing_expression")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_if_condition_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_unsupported_expression_returns_unsupported(if_condition_activity_fixtures: list[dict]) -> None:
    """Test that unsupported expression type returns UnsupportedValue."""
    fixture = get_fixture(if_condition_activity_fixtures, "unsupported_expression")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_if_condition_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_no_children(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with no child activities."""
    fixture = get_fixture(if_condition_activity_fixtures, "no_children")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert len(result.child_activities) == 0


def test_unsupported_type_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that unsupported activity types create placeholder notebook."""
    fixture = get_fixture(unsupported_activity_fixtures, "unsupported_type")
    result = translate_activity(fixture["input"])

    assert result.task_key == fixture["expected"]["task_key"]
    assert result.notebook_path == fixture["expected"]["notebook_path"]


def test_execute_pipeline_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that ExecutePipeline activity creates placeholder."""
    fixture = get_fixture(unsupported_activity_fixtures, "execute_pipeline")
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_wait_creates_placeholder_with_dependency(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that Wait activity creates placeholder with dependency preserved."""
    fixture = get_fixture(unsupported_activity_fixtures, "wait_activity")
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"
    assert result.depends_on is not None
    assert result.depends_on[0].task_key == "previous_task"


def test_no_name_gets_default(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that activity without name gets default name."""
    fixture = get_fixture(unsupported_activity_fixtures, "no_name")
    result = translate_activity(fixture["input"])

    assert result.name == "UNNAMED_TASK"
    assert result.task_key == "UNNAMED_TASK"


def test_skipped_single_dependency_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that dependency on Skipped condition creates UnsupportedValue in depends_on."""
    fixture = get_fixture(unsupported_activity_fixtures, "dependency_skipped_single")
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_skipped_dependency_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that dependency on Skipped condition creates UnsupportedValue in depends_on."""
    fixture = get_fixture(unsupported_activity_fixtures, "dependency_skipped")
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_multiple_dependency_conditions_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that multiple dependency conditions creates UnsupportedValue."""
    fixture = get_fixture(unsupported_activity_fixtures, "multiple_conditions")
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_lookup_sql_first_row_only(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with SQL source and first_row_only."""
    fixture = get_fixture(lookup_activity_fixtures, "sql_first_row")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.first_row_only is True
    assert result.source_query == fixture["expected"]["source_query"]
    assert result.source_dataset is not None


def test_lookup_sql_all_rows(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with SQL source returning all rows."""
    fixture = get_fixture(lookup_activity_fixtures, "sql_all_rows")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is False
    assert result.source_query == fixture["expected"]["source_query"]


def test_lookup_csv_file_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with CSV file source."""
    fixture = get_fixture(lookup_activity_fixtures, "csv_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True
    assert result.source_query is None
    assert result.source_dataset is not None


def test_lookup_parquet_file_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with Parquet file source."""
    fixture = get_fixture(lookup_activity_fixtures, "parquet_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is False
    assert result.source_query is None


def test_lookup_json_file_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with JSON file source."""
    fixture = get_fixture(lookup_activity_fixtures, "json_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True


def test_lookup_delta_table_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with Delta table source."""
    fixture = get_fixture(lookup_activity_fixtures, "delta_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True
    assert result.source_dataset is not None


def test_lookup_sql_no_query_uses_table(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with SQL source but no query."""
    fixture = get_fixture(lookup_activity_fixtures, "sql_no_query")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.source_query is None
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "prepare_data"


def test_lookup_default_first_row_only(lookup_activity_fixtures: list[dict]) -> None:
    """Test that first_row_only defaults to True when not specified."""
    fixture = get_fixture(lookup_activity_fixtures, "default_first_row_only")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True


def test_lookup_missing_dataset_returns_placeholder(lookup_activity_fixtures: list[dict]) -> None:
    """Test that missing input dataset creates a placeholder activity."""
    fixture = get_fixture(lookup_activity_fixtures, "missing_dataset")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_lookup_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_lookup_missing_source_returns_placeholder(lookup_activity_fixtures: list[dict]) -> None:
    """Test that missing source creates a placeholder activity."""
    fixture = get_fixture(lookup_activity_fixtures, "missing_source")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_lookup_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_lookup_unsupported_dataset_type_returns_placeholder(lookup_activity_fixtures: list[dict]) -> None:
    """Test that unsupported dataset type creates a placeholder activity."""
    fixture = get_fixture(lookup_activity_fixtures, "unsupported_dataset_type")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_lookup_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_basic_databricks_job_activity(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Databricks Job activity."""
    fixture = get_fixture(databricks_job_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.existing_job_id == fixture["expected"]["existing_job_id"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]


def test_databricks_job_with_parameters(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test translation of a Databricks Job activity with runtime job parameters."""
    fixture = get_fixture(databricks_job_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.existing_job_id == fixture["expected"]["existing_job_id"]
    assert result.job_parameters == fixture["expected"]["job_parameters"]


def test_databricks_job_with_dependency(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test translation of a Databricks Job activity with an upstream dependency."""
    fixture = get_fixture(databricks_job_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.existing_job_id == fixture["expected"]["existing_job_id"]
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "upstream_task"


def test_databricks_job_missing_job_id_returns_unsupported(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test that a missing existing_job_id returns UnsupportedValue."""
    fixture = get_fixture(databricks_job_activity_fixtures, "missing_job_id")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_databricks_job_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_translate_activities_returns_none_for_none() -> None:
    """Test that None input returns None."""
    result = translate_activities(None)
    assert result is None


def test_translate_activities_returns_empty_for_empty() -> None:
    """Test that empty list returns empty list."""
    result = translate_activities([])
    assert result == []


def test_translate_activities_flattens_if_condition() -> None:
    """Test that IfCondition children are flattened."""
    activities = [
        {
            "name": "check_condition",
            "type": "IfCondition",
            "expression": {"type": "Expression", "value": "@equals('a', 'a')"},
            "if_true_activities": [
                {
                    "name": "true_task",
                    "type": "DatabricksNotebook",
                    "depends_on": [],
                    "policy": {"timeout": "0.01:00:00"},
                    "notebook_path": "/test",
                }
            ],
        }
    ]

    result = translate_activities(activities)

    assert result is not None
    assert len(result) == 2
    assert isinstance(result[0], IfConditionActivity)
    assert isinstance(result[1], DatabricksNotebookActivity)


def test_translate_activities_multiple_activities() -> None:
    """Test translation of multiple activities."""
    activities = [
        {
            "name": "task1",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/notebook1",
        },
        {
            "name": "task2",
            "type": "DatabricksSparkJar",
            "depends_on": [{"activity": "task1", "dependency_conditions": ["Succeeded"]}],
            "policy": {"timeout": "0.02:00:00"},
            "main_class_name": "com.example.Main",
        },
    ]

    result = translate_activities(activities)

    assert result is not None
    assert len(result) == 2
    assert isinstance(result[0], DatabricksNotebookActivity)
    assert isinstance(result[1], SparkJarActivity)
    assert result[1].depends_on is not None
    assert result[1].depends_on[0].task_key == "task1"


def test_web_activity_post_with_body_and_headers(web_activity_fixtures: list[dict]) -> None:
    """Test translation of a Web activity with POST method, body, and headers."""
    fixture = get_fixture(web_activity_fixtures, "post_with_body_and_headers")
    result = translate_activity(fixture["input"])

    assert isinstance(result, WebActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.url == fixture["expected"]["url"]
    assert result.method == fixture["expected"]["method"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]
    assert result.min_retry_interval_millis == fixture["expected"]["min_retry_interval_millis"]


def test_web_activity_post_body_and_headers_stored(web_activity_fixtures: list[dict]) -> None:
    """Test that body and headers are stored on the WebActivity IR."""
    fixture = get_fixture(web_activity_fixtures, "post_with_body_and_headers")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.body == {"event": "pipeline_started", "status": "running"}
    assert result.headers == {"Content-Type": "application/json", "X-Api-Key": "secret-key"}


def test_web_activity_get_no_body(web_activity_fixtures: list[dict]) -> None:
    """Test translation of a GET Web activity with no body."""
    fixture = get_fixture(web_activity_fixtures, "get_no_body")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.method == "GET"
    assert result.body is None
    assert result.headers is None


def test_web_activity_method_uppercased(web_activity_fixtures: list[dict]) -> None:
    """Test that the HTTP method is normalised to uppercase."""
    fixture = get_fixture(web_activity_fixtures, "put_with_body")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.method == "PUT"


def test_web_activity_missing_url_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that a missing URL returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "missing_url")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_web_activity_missing_method_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that a missing method returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "missing_method")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_web_activity_with_auth_and_advanced_options(web_activity_fixtures: list[dict]) -> None:
    """Test translation of a Web activity with authentication and advanced options."""
    fixture = get_fixture(web_activity_fixtures, "post_with_auth")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.url == fixture["expected"]["url"]
    assert result.method == fixture["expected"]["method"]
    assert result.disable_cert_validation is True
    assert result.http_request_timeout_seconds == fixture["expected"]["http_request_timeout_seconds"]
    assert result.turn_off_async is True
    assert result.authentication is not None
    assert isinstance(result.authentication, Authentication)
    assert result.authentication.auth_type == fixture["expected"]["authentication_type"]
    assert result.authentication.username == "atest"
    assert result.authentication.password_secret_key == "authenticated_post_auth_password"


def test_web_activity_defaults_for_optional_fields(web_activity_fixtures: list[dict]) -> None:
    """Test that optional fields default correctly when not provided."""
    fixture = get_fixture(web_activity_fixtures, "get_no_body")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.authentication is None
    assert result.disable_cert_validation is False
    assert result.http_request_timeout_seconds is None
    assert result.turn_off_async is False


def test_web_activity_translate_activity_dispatch(web_activity_fixtures: list[dict]) -> None:
    """Test that translate_activity dispatches WebActivity to the correct translator."""
    fixture = get_fixture(web_activity_fixtures, "get_no_body")
    result = translate_activity(fixture["input"])

    assert isinstance(result, WebActivity)
    assert result.url == fixture["expected"]["url"]
    assert result.method == fixture["expected"]["method"]


def test_web_activity_unsupported_auth_type_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that an unsupported authentication type returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "unsupported_auth_type")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_web_activity_missing_auth_type_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that a missing authentication type returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "missing_auth_type")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_set_variable_static_string(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a static string value."""
    fixture = get_fixture(set_variable_activity_fixtures, "static_string_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_activity_output_expression(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with an activity output expression dict."""
    fixture = get_fixture(set_variable_activity_fixtures, "activity_output_expression")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_pipeline_run_id(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with @pipeline().RunId system variable."""
    fixture = get_fixture(set_variable_activity_fixtures, "pipeline_run_id")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_pipeline_name(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with @pipeline().Pipeline system variable."""
    fixture = get_fixture(set_variable_activity_fixtures, "pipeline_name")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_bare_expression_string(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a bare expression string (no wrapper dict)."""
    fixture = get_fixture(set_variable_activity_fixtures, "bare_expression_string")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_unsupported_expression_returns_unsupported(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with unsupported expression produces an UNSUPPORTED_ADF_ACTIVITY placeholder."""
    fixture = get_fixture(set_variable_activity_fixtures, "unsupported_expression_string")
    placeholder = get_placeholder_activity({"name": fixture["input"]["name"], "task_key": fixture["input"]["name"]})
    result = translate_activity(fixture["input"])
    assert result == placeholder


def test_set_variable_missing_variable_name_returns_unsupported(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with missing variable_name returns UnsupportedValue."""
    fixture = get_fixture(set_variable_activity_fixtures, "missing_variable_name")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ = translate_set_variable_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert "variable_name" in result.message


def test_set_variable_resolves_known_variable_reference(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with @variables() resolves to taskValues.get when variable is in context."""
    fixture = get_fixture(set_variable_activity_fixtures, "variables_reference_known")
    ctx = default_context()
    for var_name, task_key in fixture["context_variables"].items():
        ctx = ctx.with_variable(var_name, task_key)
    base_kwargs = get_base_kwargs(fixture["input"])
    result, context = translate_set_variable_activity(fixture["input"], base_kwargs, ctx)

    assert context is not None
    assert context.get_variable_task_key(fixture["expected"]["variable_name"]) == fixture["expected"]["task_key"]
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_unknown_variable_reference_returns_unsupported(
    set_variable_activity_fixtures: list[dict],
) -> None:
    """Test SetVariable with @variables() for unknown variable returns placeholder."""
    fixture = get_fixture(set_variable_activity_fixtures, "variables_reference_unknown")
    placeholder = get_placeholder_activity({"name": fixture["input"]["name"], "task_key": fixture["input"]["name"]})
    result = translate_activity(fixture["input"])
    assert result == placeholder


def test_context_cache_visit_populates_cache() -> None:
    """Visiting a named activity stores it in the returned context."""
    ctx = default_context()
    translated, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert ctx.get_activity("nb_task") is translated
    assert isinstance(translated, DatabricksNotebookActivity)


def test_context_cache_returns_cached_on_second_call() -> None:
    """A second visit for the same name returns the identical cached object."""
    ctx = default_context()
    first, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)
    second, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert first is second


def test_context_cache_does_not_grow_on_duplicate() -> None:
    """Visiting the same activity twice does not add a second cache entry."""
    ctx = default_context()
    _, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)
    cache_size_after_first = len(ctx.activity_cache)
    _, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert len(ctx.activity_cache) == cache_size_after_first


def test_context_cache_populates_all_activities() -> None:
    """All translated activities appear in the final context cache."""
    activities = [NOTEBOOK_ACTIVITY, SPARK_JAR_ACTIVITY]
    result, ctx = translate_activities_with_context(activities)

    assert result is not None
    assert len(result) == 2
    assert "nb_task" in ctx.activity_cache
    assert "jar_task" in ctx.activity_cache
    assert isinstance(ctx.get_activity("nb_task"), DatabricksNotebookActivity)
    assert isinstance(ctx.get_activity("jar_task"), SparkJarActivity)


def test_context_cache_none_input() -> None:
    """None input returns None result and the supplied context unchanged."""
    ctx = default_context()
    result, returned_ctx = translate_activities_with_context(None, ctx)

    assert result is None
    assert returned_ctx is ctx


def test_context_cache_empty_input() -> None:
    """Empty list returns empty result and the supplied context unchanged."""
    ctx = default_context()
    result, returned_ctx = translate_activities_with_context([], ctx)

    assert result == []
    assert len(returned_ctx.activity_cache) == 0


def test_context_cache_returns_pre_populated() -> None:
    """When the context already contains an activity, visit_activity returns it."""
    ctx = default_context()
    first, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    second, ctx2 = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert second is first
    assert ctx2 is ctx


def test_context_cache_threads_through_if_condition() -> None:
    """Child activities from both IfCondition branches appear in the final cache."""
    if_activity = {
        "name": "branching_check",
        "type": "IfCondition",
        "expression": {"type": "Expression", "value": "@equals('x', 'y')"},
        "if_true_activities": [
            {
                "name": "true_child",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/true_path",
            }
        ],
        "if_false_activities": [
            {
                "name": "false_child",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.False",
            }
        ],
    }
    result, ctx = translate_activities_with_context([if_activity])

    assert result is not None
    assert "branching_check" in ctx.activity_cache
    assert "true_child" in ctx.activity_cache
    assert "false_child" in ctx.activity_cache


def test_context_cache_threads_through_dependency_chain() -> None:
    """Upstream activities are cached before their dependents during topological visit."""
    activities = [
        SPARK_JAR_ACTIVITY,
        NOTEBOOK_ACTIVITY,
    ]
    result, ctx = translate_activities_with_context(activities)

    assert result is not None
    nb = ctx.get_activity("nb_task")
    jar = ctx.get_activity("jar_task")
    assert nb is not None
    assert jar is not None
    assert isinstance(nb, DatabricksNotebookActivity)
    assert isinstance(jar, SparkJarActivity)


def test_context_cache_immutability() -> None:
    """The original context is not mutated when a new activity is added."""
    ctx_before = default_context()
    _, ctx_after = visit_activity(NOTEBOOK_ACTIVITY, False, ctx_before)

    assert len(ctx_before.activity_cache) == 0
    assert len(ctx_after.activity_cache) == 1


def test_context_cache_foreach_multi_inner_uses_fresh_cache() -> None:
    """Multi-inner ForEach translates inner activities with a fresh cache.

    A parent-cached SparkJar named ``inner_nb`` must not shadow the inner
    pipeline's Notebook activity of the same name.
    """
    ctx = default_context()
    cached_as_jar = {
        "name": "inner_nb",
        "type": "DatabricksSparkJar",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "main_class_name": "com.example.Cached",
    }
    _, ctx = visit_activity(cached_as_jar, False, ctx)
    assert isinstance(ctx.get_activity("inner_nb"), SparkJarActivity)

    for_each = {
        "name": "loop",
        "type": "ForEach",
        "items": {"value": "@array(['a','b'])"},
        "batch_count": 1,
        "activities": [
            {
                "name": "inner_nb",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/inner/path",
            },
            {
                "name": "inner_jar",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.Inner",
            },
        ],
    }
    result, _ = translate_activities_with_context([for_each], ctx)

    assert result is not None
    for_each_result = result[0]
    assert isinstance(for_each_result, ForEachActivity)
    assert isinstance(for_each_result.for_each_task, RunJobActivity)
    inner_tasks = for_each_result.for_each_task.pipeline.tasks
    inner_nb_task = next(t for t in inner_tasks if t.name == "inner_nb")
    assert isinstance(inner_nb_task, DatabricksNotebookActivity)


def test_context_cache_foreach_multi_inner_does_not_modify_parent() -> None:
    """Multi-inner ForEach does not leak inner activities into the parent cache."""
    ctx = default_context()
    outer = {
        "name": "outer_task",
        "type": "DatabricksNotebook",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "notebook_path": "/outer/path",
    }
    _, ctx = visit_activity(outer, False, ctx)

    for_each = {
        "name": "loop",
        "type": "ForEach",
        "items": {"value": "@array(['a','b'])"},
        "batch_count": 1,
        "activities": [
            {
                "name": "inner_nb",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/inner/path",
            },
            {
                "name": "inner_jar",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.Inner",
            },
        ],
    }
    result, final_ctx = translate_activities_with_context([for_each], ctx)

    assert result is not None
    assert "outer_task" in final_ctx.activity_cache
    assert "loop" in final_ctx.activity_cache
    assert "inner_nb" not in final_ctx.activity_cache
    assert "inner_jar" not in final_ctx.activity_cache


def test_variable_cache_with_variable_returns_new_context() -> None:
    """with_variable returns a new context containing the variable mapping."""
    ctx = TranslationContext()
    updated = ctx.with_variable("myVar", "set_my_var")

    assert updated.get_variable_task_key("myVar") == "set_my_var"
    assert ctx.get_variable_task_key("myVar") is None


def test_variable_cache_get_missing_returns_none() -> None:
    """get_variable_task_key returns None for variables not in the cache."""
    ctx = TranslationContext()

    assert ctx.get_variable_task_key("nonexistent") is None


def test_variable_cache_immutability() -> None:
    """The original context is not mutated when a variable is added."""
    ctx_before = TranslationContext()
    ctx_after = ctx_before.with_variable("x", "task_x")

    assert len(ctx_before.variable_cache) == 0
    assert len(ctx_after.variable_cache) == 1


def test_variable_cache_overwrite() -> None:
    """A later with_variable call for the same name overwrites the previous mapping."""
    ctx = TranslationContext()
    ctx = ctx.with_variable("myVar", "first_task")
    ctx = ctx.with_variable("myVar", "second_task")

    assert ctx.get_variable_task_key("myVar") == "second_task"


def test_variable_cache_preserves_activity_cache() -> None:
    """with_variable preserves the existing activity cache."""
    ctx = default_context()
    _, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)
    ctx = ctx.with_variable("myVar", "set_my_var")

    assert ctx.get_activity("nb_task") is not None
    assert ctx.get_variable_task_key("myVar") == "set_my_var"


def test_variable_cache_populated_by_set_variable_visit() -> None:
    """Visiting a SetVariable activity populates the variable cache."""
    ctx = default_context()
    _, ctx = visit_activity(SET_VARIABLE_ACTIVITY, False, ctx)

    assert ctx.get_variable_task_key("myVar") == "set_my_var"


def test_variable_cache_populated_by_translate_activities_with_context() -> None:
    """translate_activities_with_context populates the variable cache for SetVariable."""
    activities = [SET_VARIABLE_ACTIVITY]
    _, ctx = translate_activities_with_context(activities)

    assert ctx.get_variable_task_key("myVar") == "set_my_var"


def test_variable_cache_available_to_downstream_set_variable() -> None:
    """A downstream SetVariable can reference a variable set by an upstream SetVariable."""
    upstream = {
        "name": "set_source",
        "type": "SetVariable",
        "depends_on": [],
        "variable_name": "sourceVar",
        "value": "hello",
    }
    downstream = {
        "name": "copy_var",
        "type": "SetVariable",
        "depends_on": [{"activity": "set_source", "dependency_conditions": ["Succeeded"]}],
        "variable_name": "copiedVar",
        "value": {
            "value": "@variables('sourceVar')",
            "type": "Expression",
        },
    }
    result, ctx = translate_activities_with_context([upstream, downstream])

    assert result is not None
    assert len(result) == 2
    assert ctx.get_variable_task_key("sourceVar") == "set_source"
    assert ctx.get_variable_task_key("copiedVar") == "copy_var"
    downstream_activity = ctx.get_activity("copy_var")
    assert isinstance(downstream_activity, SetVariableActivity)
    assert downstream_activity.variable_value == "dbutils.jobs.taskValues.get(taskKey='set_source', key='sourceVar')"


def test_set_variable_activity_output_double_quotes(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with double-quoted activity output expression."""
    fixture = get_fixture(set_variable_activity_fixtures, "activity_output_double_quotes")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_resolves_double_quoted_variable_reference(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with double-quoted @variables() resolves when variable is in context."""
    fixture = get_fixture(set_variable_activity_fixtures, "variables_reference_double_quotes")
    ctx = default_context()
    for var_name, task_key in fixture["context_variables"].items():
        ctx = ctx.with_variable(var_name, task_key)
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ = translate_set_variable_activity(fixture["input"], base_kwargs, ctx)

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_parse_variable_value_activity_output_double_quotes() -> None:
    """parse_variable_value resolves double-quoted @activity() output references."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": '@activity("LookupTask").output.firstRow', "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='LookupTask', key='result')"


def test_parse_variable_value_variables_reference_double_quotes() -> None:
    """parse_variable_value resolves double-quoted @variables() when variable is in context."""
    ctx = TranslationContext().with_variable("myVar", "set_my_var")
    result = parse_variable_value({"value": '@variables("myVar")', "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='set_my_var', key='myVar')"


def test_parse_variable_value_variables_reference_found() -> None:
    """parse_variable_value resolves @variables() when the variable is in the context."""
    ctx = TranslationContext().with_variable("myVar", "set_my_var")
    result = parse_variable_value({"value": "@variables('myVar')", "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='set_my_var', key='myVar')"


def test_parse_variable_value_variables_reference_not_found() -> None:
    """parse_variable_value returns UnsupportedValue when the variable is not in context."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@variables('unknown')", "type": "Expression"}, ctx)

    assert isinstance(result, UnsupportedValue)
    assert "unknown" in result.message


def test_set_variable_integer_value(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with an integer value produces a Python int literal."""
    fixture = get_fixture(set_variable_activity_fixtures, "integer_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_boolean_value(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a boolean value produces a Python bool literal."""
    fixture = get_fixture(set_variable_activity_fixtures, "boolean_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_float_value(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a float value produces a Python float literal."""
    fixture = get_fixture(set_variable_activity_fixtures, "float_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_nested_activity_output(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with nested activity output path like firstRow.columnName."""
    fixture = get_fixture(set_variable_activity_fixtures, "nested_activity_output")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_parse_variable_value_integer() -> None:
    """parse_variable_value handles integer values directly."""
    ctx = TranslationContext()
    assert parse_variable_value(42, ctx) == "42"


def test_parse_variable_value_boolean() -> None:
    """parse_variable_value handles boolean values directly."""
    ctx = TranslationContext()
    assert parse_variable_value(True, ctx) == "True"
    assert parse_variable_value(False, ctx) == "False"


def test_parse_variable_value_nested_output_property() -> None:
    """parse_variable_value resolves nested activity output like firstRow.myColumn."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@activity('Lookup').output.firstRow.col1", "type": "Expression"}, ctx)
    assert result == "json.loads(dbutils.jobs.taskValues.get(taskKey='Lookup', key='result'))['col1']"


def test_parse_variable_value_static_string() -> None:
    """parse_variable_value wraps static strings as Python literals."""
    ctx = TranslationContext()
    result = parse_variable_value("hello", ctx)

    assert result == "'hello'"


def test_parse_variable_value_activity_output() -> None:
    """parse_variable_value resolves @activity() output references."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@activity('LookupTask').output.firstRow", "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='LookupTask', key='result')"


def test_parse_variable_value_pipeline_system_var() -> None:
    """parse_variable_value resolves @pipeline() system variables."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@pipeline().RunId", "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.getContext().tags().get('runId', '')"


def test_copy_postgresql_to_delta(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a PostgreSQL source dataset."""
    fixture = next(f for f in copy_activity_fixtures if "PostgreSQL to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_mysql_to_delta(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a MySQL source dataset."""
    fixture = next(f for f in copy_activity_fixtures if "MySQL to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_oracle_to_delta(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with an Oracle source dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Oracle to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_delta_to_postgresql(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a PostgreSQL sink dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Delta to PostgreSQL" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_delta_to_mysql(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a MySQL sink dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Delta to MySQL" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_delta_to_oracle(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with an Oracle sink dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Delta to Oracle" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_csv_to_delta_with_translator(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with translator column mappings (CSV to Delta)."""
    fixture = next(f for f in copy_activity_fixtures if "CSV to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.source_dataset is not None
    assert result.sink_dataset is not None
    assert len(result.column_mapping) == fixture["expected"]["column_mapping_count"]


def test_copy_invalid_translator_returns_unsupported(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with malformed translator returns UnsupportedValue."""
    fixture = next(f for f in copy_activity_fixtures if "CSV to Delta" in f["description"])
    input_ = fixture["input"].copy()
    input_["translator"] = {"mappings": [{"source": {"name": "id"}, "sink": {}}]}  # missing sink name/type

    result = translate_copy_activity(input_, get_base_kwargs(input_))

    assert isinstance(result, UnsupportedValue)
    assert "translator" in result.message.lower()

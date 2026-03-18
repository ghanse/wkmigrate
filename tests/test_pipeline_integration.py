"""Integration tests for pipeline translation using JSON fixtures.

This module tests end-to-end pipeline translation from ADF definitions
to Databricks workflow IR. Tests cover complex scenarios including
sequential tasks, parallel branches, conditional logic, and batch processing.
"""

from __future__ import annotations

from tests.conftest import get_fixture
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    RunJobActivity,
    SparkJarActivity,
)
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline


def test_etl_pipeline_sequential_tasks(complex_pipeline_fixtures: list[dict]) -> None:
    """Test ETL pipeline with sequential notebook activities."""
    fixture = get_fixture(complex_pipeline_fixtures, "etl_sequential")
    result = translate_pipeline(fixture["input"])

    assert isinstance(result, Pipeline)
    assert result.name == fixture["expected"]["name"]
    assert len(result.tasks) == fixture["expected"]["task_count"]
    assert result.parameters is not None
    assert len(result.parameters) == fixture["expected"]["parameter_count"]

    # Verify task order and dependencies
    task_names = [task.name for task in result.tasks]
    expected_chain = fixture["expected"]["dependency_chain"]
    for i, expected_name in enumerate(expected_chain):
        assert task_names[i] == expected_name

    # Verify dependencies are set correctly
    transform_task = result.tasks[1]
    assert transform_task.depends_on is not None
    assert transform_task.depends_on[0].task_key == "extract_data"

    load_task = result.tasks[2]
    assert load_task.depends_on is not None
    assert load_task.depends_on[0].task_key == "transform_data"


def test_parallel_pipeline_branches_and_join(complex_pipeline_fixtures: list[dict]) -> None:
    """Test pipeline with parallel branches and join."""
    fixture = get_fixture(complex_pipeline_fixtures, "parallel_branches")
    result = translate_pipeline(fixture["input"])

    assert isinstance(result, Pipeline)
    assert result.name == fixture["expected"]["name"]
    assert len(result.tasks) == fixture["expected"]["task_count"]

    # Find the merge task
    merge_task = next(t for t in result.tasks if t.name == "merge_results")

    # Verify merge task depends on all 3 branches
    assert merge_task.depends_on is not None
    assert len(merge_task.depends_on) == 3

    dependency_keys = {dep.task_key for dep in merge_task.depends_on}
    assert dependency_keys == {"process_branch_a", "process_branch_b", "process_branch_c"}


def test_conditional_pipeline_if_condition(complex_pipeline_fixtures: list[dict]) -> None:
    """Test pipeline with IfCondition branching."""
    fixture = get_fixture(complex_pipeline_fixtures, "if_condition_branching")

    result = translate_pipeline(fixture["input"])

    assert isinstance(result, Pipeline)
    assert result.name == fixture["expected"]["name"]

    # Find the IfCondition task
    if_condition = next(t for t in result.tasks if isinstance(t, IfConditionActivity))

    assert if_condition is not None
    assert if_condition.op == "GREATER_THAN"

    # Count total tasks including flattened children
    assert len(result.tasks) == fixture["expected"]["task_count"]


def test_batch_processing_pipeline_foreach(complex_pipeline_fixtures: list[dict]) -> None:
    """Test pipeline with ForEach for batch processing."""
    fixture = get_fixture(complex_pipeline_fixtures, "foreach_batch")
    result = translate_pipeline(fixture["input"])

    assert isinstance(result, Pipeline)
    assert result.name == fixture["expected"]["name"]
    assert len(result.tasks) == fixture["expected"]["task_count"]

    # Find the ForEach task
    foreach_task = next(t for t in result.tasks if isinstance(t, ForEachActivity))

    assert foreach_task is not None
    assert foreach_task.concurrency == fixture["expected"]["for_each_concurrency"]
    assert isinstance(foreach_task.for_each_task, DatabricksNotebookActivity)


def test_mixed_activity_pipeline(complex_pipeline_fixtures: list[dict]) -> None:
    """Test pipeline with mixed activity types."""
    fixture = get_fixture(complex_pipeline_fixtures, "mixed_activity_types")
    result = translate_pipeline(fixture["input"])

    assert isinstance(result, Pipeline)
    assert result.name == fixture["expected"]["name"]
    assert len(result.tasks) == fixture["expected"]["task_count"]
    assert result.parameters is not None
    assert len(result.parameters) == fixture["expected"]["parameter_count"]

    # Verify activity types
    task_types = {type(t).__name__ for t in result.tasks}
    expected_types = set(fixture["expected"]["activity_types"])
    assert expected_types.issubset(task_types)

    # Verify JAR task has libraries
    jar_task = next(t for t in result.tasks if isinstance(t, SparkJarActivity))
    assert jar_task.libraries is not None
    assert len(jar_task.libraries) == 2  # jar + maven


def test_nested_foreach_creates_run_job(complex_pipeline_fixtures: list[dict]) -> None:
    """Test pipeline with nested ForEach containing multiple activities."""
    fixture = get_fixture(complex_pipeline_fixtures, "nested_foreach_run_job")
    result = translate_pipeline(fixture["input"])

    assert isinstance(result, Pipeline)
    assert result.name == fixture["expected"]["name"]

    # Find the ForEach task
    foreach_task = next(t for t in result.tasks if isinstance(t, ForEachActivity))

    assert foreach_task is not None
    # Multiple inner activities should create RunJobActivity
    assert isinstance(foreach_task.for_each_task, RunJobActivity)

    # Verify the inner pipeline has the expected number of tasks
    inner_pipeline = foreach_task.for_each_task.pipeline
    assert inner_pipeline is not None
    assert len(inner_pipeline.tasks) == fixture["expected"]["inner_job_task_count"]


def test_string_parameter() -> None:
    """Test translation of string parameter."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
        "parameters": {"input_path": {"type": "string", "default_value": "/data/input"}},
    }
    result = translate_pipeline(pipeline)

    assert result.parameters is not None
    assert len(result.parameters) == 1


def test_array_parameter() -> None:
    """Test translation of array parameter."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
        "parameters": {"regions": {"type": "array", "default_value": "['US', 'EU', 'APAC']"}},
    }
    result = translate_pipeline(pipeline)

    assert result.parameters is not None
    assert len(result.parameters) == 1


def test_int_parameter() -> None:
    """Test translation of int parameter."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
        "parameters": {"batch_size": {"type": "int", "default_value": 1000}},
    }
    result = translate_pipeline(pipeline)

    assert result.parameters is not None
    assert len(result.parameters) == 1


def test_no_parameters() -> None:
    """Test pipeline with no parameters."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
    }
    result = translate_pipeline(pipeline)

    assert result.parameters is None


def test_tags_include_system_tags() -> None:
    """Test that system tags are added to pipeline tags."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
        "tags": {"environment": "production", "team": "data-engineering"},
    }
    result = translate_pipeline(pipeline)

    assert result.tags is not None
    assert "CREATED_BY_WKMIGRATE" in result.tags
    assert result.tags["environment"] == "production"
    assert result.tags["team"] == "data-engineering"


def test_no_tags_still_gets_system_tags() -> None:
    """Test that pipeline with no tags still gets system tags."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
    }
    result = translate_pipeline(pipeline)

    assert result.tags is not None
    assert "CREATED_BY_WKMIGRATE" in result.tags


def test_schedule_from_trigger() -> None:
    """Test that schedule is extracted from trigger."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
        "trigger": {
            "properties": {
                "type": "ScheduleTrigger",
                "recurrence": {
                    "frequency": "Day",
                    "interval": 1,
                    "start_time": "2024-01-01T00:00:00Z",
                    "time_zone": "UTC",
                },
            }
        },
    }
    result = translate_pipeline(pipeline)

    assert result.schedule is not None


def test_no_trigger_no_schedule() -> None:
    """Test that no trigger results in no schedule."""
    pipeline = {
        "name": "test_pipeline",
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ],
    }
    result = translate_pipeline(pipeline)

    assert result.schedule is None


def test_empty_activities() -> None:
    """Test pipeline with no activities."""
    pipeline = {"name": "empty_pipeline", "activities": []}
    result = translate_pipeline(pipeline)

    assert isinstance(result, Pipeline)
    assert result.name == "empty_pipeline"
    assert len(result.tasks) == 0


def test_no_name_gets_default() -> None:
    """Test pipeline with no name gets default name."""
    pipeline = {
        "activities": [
            {
                "name": "task1",
                "type": "DatabricksNotebook",
                "notebook_path": "/test",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
            }
        ]
    }
    result = translate_pipeline(pipeline)

    assert result.name == "UNNAMED_WORKFLOW"


def test_circular_dependencies_handled() -> None:
    """Test that circular dependencies don't cause infinite loops."""
    pipeline = {
        "name": "circular_pipeline",
        "activities": [
            {
                "name": "task_a",
                "type": "DatabricksNotebook",
                "notebook_path": "/test_a",
                "depends_on": [{"activity": "task_b", "dependency_conditions": ["Succeeded"]}],
                "policy": {"timeout": "0.01:00:00"},
            },
            {
                "name": "task_b",
                "type": "DatabricksNotebook",
                "notebook_path": "/test_b",
                "depends_on": [{"activity": "task_a", "dependency_conditions": ["Succeeded"]}],
                "policy": {"timeout": "0.01:00:00"},
            },
        ],
    }
    # Should not hang or raise an error
    result = translate_pipeline(pipeline)

    assert isinstance(result, Pipeline)
    assert len(result.tasks) == 2


def test_deeply_nested_if_conditions() -> None:
    """Test deeply nested IfCondition activities."""
    pipeline = {
        "name": "nested_conditions",
        "activities": [
            {
                "name": "outer_condition",
                "type": "IfCondition",
                "expression": {"type": "Expression", "value": "@equals('a', 'a')"},
                "depends_on": [],
                "if_true_activities": [
                    {
                        "name": "inner_condition",
                        "type": "IfCondition",
                        "expression": {"type": "Expression", "value": "@equals('b', 'b')"},
                        "depends_on": [],
                        "if_true_activities": [
                            {
                                "name": "innermost_task",
                                "type": "DatabricksNotebook",
                                "notebook_path": "/innermost",
                                "depends_on": [],
                                "policy": {"timeout": "0.01:00:00"},
                            }
                        ],
                    }
                ],
            }
        ],
    }

    result = translate_pipeline(pipeline)

    assert isinstance(result, Pipeline)
    # Should have outer condition, inner condition (flattened), and innermost task (flattened)
    assert len(result.tasks) >= 3


def test_foreach_with_unsupported_inner_activity() -> None:
    """Test ForEach with unsupported inner activity creates placeholder."""
    pipeline = {
        "name": "foreach_unsupported",
        "activities": [
            {
                "name": "foreach_task",
                "type": "ForEach",
                "depends_on": [],
                "batch_count": 2,
                "items": {"type": "Expression", "value": "@array(['a', 'b'])"},
                "activities": [
                    {
                        "name": "unsupported_inner",
                        "type": "SomeUnsupportedType",
                        "depends_on": [],
                    }
                ],
            }
        ],
    }
    result = translate_pipeline(pipeline)

    assert isinstance(result, Pipeline)
    foreach_task = result.tasks[0]
    assert isinstance(foreach_task, ForEachActivity)
    # Inner task should be a placeholder notebook
    assert foreach_task.for_each_task.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"

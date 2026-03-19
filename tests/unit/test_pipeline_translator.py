"""Tests for the pipeline translation methods."""

from contextlib import nullcontext as does_not_raise

import pytest

from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline


@pytest.mark.parametrize(
    "pipeline_definition, expected_result, context",
    [
        (
            {
                "name": "TestPipeline",
                "parameters": {"param1": {"type": "string"}},
                "trigger": {
                    "type": "ScheduleTrigger",
                    "properties": {"recurrence": {"frequency": "Day", "interval": 1}},
                },
                "tags": {"env": "test"},
            },
            Pipeline(
                name="TestPipeline",
                parameters=[{"name": "param1", "default": "None"}],
                schedule={"quartz_cron_expression": "0 0 0 */1 * ?", "timezone_id": "UTC"},
                tags={"env": "test", "CREATED_BY_WKMIGRATE": ""},
                tasks=[],
                not_translatable=[],
                warnings=[],
            ),
            does_not_raise(),
        ),
        (
            {
                "parameters": {"param1": {"type": "string"}},
                "trigger": {
                    "type": "ScheduleTrigger",
                    "properties": {"recurrence": {"frequency": "Day", "interval": 1}},
                },
                "tags": {"env": "test"},
            },
            Pipeline(
                name="UNNAMED_WORKFLOW",
                parameters=[{"name": "param1", "default": "None"}],
                schedule={"quartz_cron_expression": "0 0 0 */1 * ?", "timezone_id": "UTC"},
                tags={"env": "test", "CREATED_BY_WKMIGRATE": ""},
                tasks=[],
                not_translatable=[],
                warnings=[
                    {
                        "property": "pipeline.name",
                        "message": "No pipeline name in source definition, setting to UNNAMED_WORKFLOW",
                    }
                ],
            ),
            does_not_raise(),
        ),
    ],
)
def test_translate_pipeline(pipeline_definition, expected_result, context):
    with context:
        result = translate_pipeline(pipeline_definition)
        assert result == expected_result


def test_unsupported_activity_populates_not_translatable() -> None:
    """Unsupported activity types produce entries in Pipeline.not_translatable."""
    pipeline = {
        "name": "unsupported_test",
        "activities": [
            {
                "name": "custom_task",
                "type": "CustomUnsupportedType",
                "depends_on": [],
            }
        ],
    }
    result = translate_pipeline(pipeline)

    assert len(result.not_translatable) == 1
    entry = result.not_translatable[0]
    assert entry["property"] == "custom_task"
    assert "activity_name" in entry
    assert entry["activity_name"] == "custom_task"
    assert entry["activity_type"] == "CustomUnsupportedType"


def test_unsupported_activity_does_not_appear_in_warnings() -> None:
    """Unsupported activities go to not_translatable, not warnings."""
    pipeline = {
        "name": "separation_test",
        "activities": [
            {
                "name": "bad_task",
                "type": "SetVariable",
                "depends_on": [],
            }
        ],
    }
    result = translate_pipeline(pipeline)

    assert len(result.not_translatable) == 1
    assert len(result.warnings) == 0


def test_mixed_warnings_and_unsupported() -> None:
    """A pipeline with both unsupported activities and translation warnings separates them correctly."""
    pipeline = {
        "name": "mixed_test",
        "activities": [
            {
                "name": "good_notebook",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00", "secure_input": True},
                "notebook_path": "/notebooks/etl",
            },
            {
                "name": "unsupported_task",
                "type": "ExecutePipeline",
                "depends_on": [],
            },
        ],
    }
    result = translate_pipeline(pipeline)

    assert len(result.not_translatable) == 1
    assert result.not_translatable[0]["property"] == "unsupported_task"

    assert len(result.warnings) >= 1
    warning_properties = [w["property"] for w in result.warnings]
    assert "secure_input" in warning_properties

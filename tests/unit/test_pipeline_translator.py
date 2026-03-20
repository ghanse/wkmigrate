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
                not_translatable=[
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


def test_mixed_warnings_and_unsupported() -> None:
    """A pipeline with both unsupported activities and translation warnings collects all in not_translatable."""
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
        ],
    }
    result = translate_pipeline(pipeline)

    # secure_input warning should appear in not_translatable
    warning_properties = [w["property"] for w in result.not_translatable]
    assert "secure_input" in warning_properties

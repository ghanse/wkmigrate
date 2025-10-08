import pytest
from contextlib import nullcontext as does_not_raise
from wkmigrate.pipeline_translators.pipeline_translator import translate_pipeline


class TestPipelineTranslator:
    """Unit tests for the pipeline translation methods."""

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
                {
                    "name": "TestPipeline",
                    "parameters": [{"name": "param1", "default": "None"}],
                    "schedule": {
                        "quartz_cron_expression": "0 0 0 */1 * ?",
                        "timezone_id": "UTC",
                    },
                    "tags": {"env": "test", "CREATED_BY_WKMIGRATE": ""},
                    "tasks": None,
                },
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
                {
                    "name": "UNNAMED_WORKFLOW",
                    "parameters": [{"name": "param1", "default": "None"}],
                    "schedule": {
                        "quartz_cron_expression": "0 0 0 */1 * ?",
                        "timezone_id": "UTC",
                    },
                    "tags": {"env": "test", "CREATED_BY_WKMIGRATE": ""},
                    "tasks": None,
                },
                pytest.warns(UserWarning),
            ),
        ],
    )
    def test_translate_pipeline(self, pipeline_definition, expected_result, context):
        with context:
            result = translate_pipeline(pipeline_definition)
            assert result == expected_result

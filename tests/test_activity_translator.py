from contextlib import nullcontext as does_not_raise
import pytest
from wkmigrate.activity_translators.activity_translator import translate_activities, translate_activity
from wkmigrate.models.ir.activities import DatabricksNotebookActivity, Dependency, IfConditionActivity


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
                            "timeout": "0.01:00:00",
                            "retry": 3,
                            "retry_interval_in_seconds": 30,
                        },
                        "notebook_path": "/path/to/notebook",
                        "base_parameters": {"param": "val"},
                    }
                ],
                [
                    DatabricksNotebookActivity(
                        name="Activity1",
                        task_key="Activity1",
                        activity_type="DatabricksNotebook",
                        description="Test activity",
                        timeout_seconds=3600,
                        max_retries=3,
                        min_retry_interval_millis=30000,
                        notebook_path="/path/to/notebook",
                        base_parameters={"param": "val"},
                    )
                ],
            ),
            (
                [
                    {
                        "type": "DatabricksNotebook",
                        "name": "Activity1",
                        "description": "Test activity",
                        "policy": {
                            "timeout": "0.00:30:00",
                            "retry": 3,
                            "retry_interval_in_seconds": 30,
                        },
                        "depends_on": [{"activity": "PreviousActivity"}],
                        "notebook_path": "/path/to/notebook",
                    }
                ],
                [
                    DatabricksNotebookActivity(
                        name="Activity1",
                        task_key="Activity1",
                        activity_type="DatabricksNotebook",
                        description="Test activity",
                        timeout_seconds=1800,
                        max_retries=3,
                        min_retry_interval_millis=30000,
                        notebook_path="/path/to/notebook",
                        base_parameters=None,
                        depends_on=[Dependency(task_key="PreviousActivity", outcome=None)],
                    )
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
                    DatabricksNotebookActivity(
                        name="UNNAMED_TASK",
                        task_key="UNNAMED_TASK",
                        activity_type="DatabricksNotebook",
                        description="Test activity",
                        timeout_seconds=604800,
                        max_retries=3,
                        min_retry_interval_millis=30000,
                        depends_on=[Dependency(task_key="PreviousActivity", outcome=["Succeeded"])],
                        notebook_path="/path/to/notebook",
                    ),
                    DatabricksNotebookActivity(
                        name="UNNAMED_TASK",
                        task_key="UNNAMED_TASK",
                        activity_type="DatabricksNotebook",
                        description="Test activity",
                        timeout_seconds=604800,
                        max_retries=3,
                        min_retry_interval_millis=30000,
                        depends_on=[Dependency(task_key="PreviousActivity", outcome=["Succeeded"])],
                        notebook_path="/path/to/notebook",
                    ),
                ],
            ),
        ],
    )
    def test_translate_activities_parses_results(self, activity_definition, expected_result):
        activities = translate_activities(activity_definition)
        assert activities == expected_result

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
                DatabricksNotebookActivity(
                    name="Activity1",
                    task_key="Activity1",
                    activity_type="DatabricksNotebook",
                    description="Test activity",
                    timeout_seconds=604800,
                    max_retries=3,
                    min_retry_interval_millis=30000,
                    depends_on=[],
                    notebook_path="/path/to/notebook",
                ),
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
                IfConditionActivity(
                    name="IfConditionActivity",
                    task_key="IfConditionActivity",
                    activity_type="IfCondition",
                    description="Test if-else condition activity",
                    op="EQUAL_TO",
                    left="true",
                    right="true",
                    child_activities=[],
                ),
                pytest.warns(UserWarning),
            ),
        ],
    )
    def test_translate_activity_parses_result(self, activity_definition, expected_result, context):
        with context:
            result_ir = translate_activity(activity_definition)
            # ``translate_activity`` may return a single Activity or (Activity, [Activity])
            if isinstance(result_ir, tuple):
                activity = result_ir[0]
            else:
                activity = result_ir
            assert activity == expected_result

    def test_translate_unsupported_activity_creates_placeholder(self):
        """Unknown activity types should be translated into a placeholder notebook activity."""
        unsupported_definition = {
            "type": "CustomUnsupportedType",
            "name": "UnsupportedActivity",
            "description": "Should fall back to placeholder",
            "policy": {"timeout": "0.00:10:00"},
        }
        result_ir = translate_activity(unsupported_definition)
        activity = result_ir if not isinstance(result_ir, tuple) else result_ir[0]
        assert activity.activity_type == "CustomUnsupportedType"
        assert activity.task_key == "UnsupportedActivity"
        assert activity.timeout_seconds == 600
        assert activity.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"

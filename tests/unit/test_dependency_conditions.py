"""Tests for ghanse/wkmigrate#44: Support Completed and Failed dependency conditions.

ADF supports four dependency conditions: Succeeded, Completed, Failed, Skipped.
Completed and Failed are now handled — they map to run_if=ALL_DONE and
run_if=AT_LEAST_ONE_FAILED respectively.

Fixture JSONs are under tests/resources/json/camel/dependency_conditions/.
"""

from __future__ import annotations

import os


from wkmigrate.definition_stores.json_definition_store import (
    JsonDefinitionStore,
)
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.preparers.preparer import prepare_workflow
from wkmigrate.translators.activity_translators.activity_translator import (
    _parse_dependency,
)

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "resources", "json", "camel", "dependency_conditions")


def _load(pipeline_name: str) -> Pipeline:
    store = JsonDefinitionStore(
        source_directory=_FIXTURE_DIR,
        source_property_case="camel",
    )
    return store.load(pipeline_name)


def test_completed_pipeline_translates() -> None:
    """A pipeline with a Completed dependency should translate."""
    pipeline = _load("completed_condition")
    assert len(pipeline.tasks) == 2


def test_completed_dependency_is_not_unsupported() -> None:
    """Completed condition should produce a valid Dependency, not UnsupportedValue."""
    pipeline = _load("completed_condition")
    cleanup = next(t for t in pipeline.tasks if t.name == "cleanup")
    assert cleanup.depends_on is not None
    assert len(cleanup.depends_on) == 1
    dep = cleanup.depends_on[0]
    assert not isinstance(dep, UnsupportedValue)
    assert dep.task_key == "run_process"


def test_completed_run_if_is_all_done() -> None:
    """Completed condition should map to run_if=ALL_DONE."""
    pipeline = _load("completed_condition")
    prepared = prepare_workflow(pipeline)
    cleanup = next(t for t in prepared.tasks if t["task_key"] == "cleanup")
    assert cleanup.get("run_if") == "ALL_DONE"


def test_completed_preparer_does_not_crash() -> None:
    """Completed dependency should not crash the preparer."""
    pipeline = _load("completed_condition")
    prepared = prepare_workflow(pipeline)
    assert len(prepared.tasks) == 2
    task_keys = {t["task_key"] for t in prepared.tasks}
    assert "cleanup" in task_keys


def test_failed_pipeline_translates() -> None:
    """A pipeline with a Failed dependency should translate."""
    pipeline = _load("failed_condition")
    assert len(pipeline.tasks) == 2


def test_failed_dependency_is_not_unsupported() -> None:
    """Failed condition should produce a valid Dependency, not UnsupportedValue."""
    pipeline = _load("failed_condition")
    handler = next(t for t in pipeline.tasks if t.name == "error_handler")
    assert handler.depends_on is not None
    assert len(handler.depends_on) == 1
    dep = handler.depends_on[0]
    assert not isinstance(dep, UnsupportedValue)
    assert dep.task_key == "run_process"


def test_failed_run_if_is_at_least_one_failed() -> None:
    """Failed condition should map to run_if=AT_LEAST_ONE_FAILED."""
    pipeline = _load("failed_condition")
    prepared = prepare_workflow(pipeline)
    handler = next(t for t in prepared.tasks if t["task_key"] == "error_handler")
    assert handler.get("run_if") == "AT_LEAST_ONE_FAILED"


def test_failed_preparer_does_not_crash() -> None:
    """Failed dependency should not crash the preparer."""
    pipeline = _load("failed_condition")
    prepared = prepare_workflow(pipeline)
    assert len(prepared.tasks) == 2
    task_keys = {t["task_key"] for t in prepared.tasks}
    assert "error_handler" in task_keys


def test_mixed_pipeline_translates() -> None:
    """A pipeline using Succeeded, Completed, and Failed should translate."""
    pipeline = _load("mixed_conditions")
    assert len(pipeline.tasks) == 4


def test_mixed_no_unsupported_dependencies() -> None:
    """All dependency conditions in a mixed pipeline should produce valid Dependencies."""
    pipeline = _load("mixed_conditions")
    for task in pipeline.tasks:
        if task.depends_on:
            for dep in task.depends_on:
                assert not isinstance(dep, UnsupportedValue), f"Task '{task.name}' has UnsupportedValue dependency."


def test_mixed_run_if_values_per_condition() -> None:
    """Each condition type should produce the correct run_if value."""
    pipeline = _load("mixed_conditions")
    prepared = prepare_workflow(pipeline)
    tasks = {t["task_key"]: t for t in prepared.tasks}

    assert tasks["run_process"].get("run_if") is None
    assert tasks["log_result"].get("run_if") == "ALL_DONE"
    assert tasks["error_handler"].get("run_if") == "AT_LEAST_ONE_FAILED"
    assert tasks["finalize"].get("run_if") is None


def test_mixed_preparer_does_not_crash() -> None:
    """Mixed-condition pipeline should not crash the preparer."""
    pipeline = _load("mixed_conditions")
    prepared = prepare_workflow(pipeline)
    assert len(prepared.tasks) == 4
    task_keys = {t["task_key"] for t in prepared.tasks}
    assert {"run_process", "log_result", "error_handler", "finalize"} == task_keys


def test_completed_inside_if_condition_translates() -> None:
    """Completed condition inside an IfCondition child should translate."""
    pipeline = _load("completed_inside_if_condition")
    assert len(pipeline.tasks) >= 1


def test_completed_inside_if_condition_no_unsupported() -> None:
    """Completed/Failed conditions inside IfCondition children should be accepted."""
    pipeline = _load("completed_inside_if_condition")
    for task in pipeline.tasks:
        if task.depends_on:
            for dep in task.depends_on:
                assert not isinstance(
                    dep, UnsupportedValue
                ), f"Task '{task.name}' has UnsupportedValue dependency ('{dep.message}')."


def test_completed_inside_if_condition_preparer_does_not_crash() -> None:
    """Completed inside IfCondition should not crash the preparer."""
    pipeline = _load("completed_inside_if_condition")
    prepared = prepare_workflow(pipeline)
    assert len(prepared.tasks) >= 1


def test_completed_inside_if_condition_run_if_is_all_done() -> None:
    """Completed sibling dep inside IfCondition should still produce run_if=ALL_DONE."""
    pipeline = _load("completed_inside_if_condition")
    prepared = prepare_workflow(pipeline)
    log_fallback = next(t for t in prepared.tasks if t["task_key"] == "log_fallback")
    assert log_fallback.get("run_if") == "ALL_DONE"


def test_failed_inside_if_condition_run_if_is_at_least_one_failed() -> None:
    """Failed sibling dep inside IfCondition should produce run_if=AT_LEAST_ONE_FAILED."""
    pipeline = _load("failed_inside_if_condition")
    prepared = prepare_workflow(pipeline)
    handler = next(t for t in prepared.tasks if t["task_key"] == "error_handler_fallback")
    assert handler.get("run_if") == "AT_LEAST_ONE_FAILED"


def test_succeeded_inside_if_condition_run_if_is_none() -> None:
    """Succeeded sibling dep inside IfCondition should leave run_if=None."""
    pipeline = _load("succeeded_inside_if_condition")
    prepared = prepare_workflow(pipeline)
    log_fallback = next(t for t in prepared.tasks if t["task_key"] == "log_fallback")
    assert log_fallback.get("run_if") is None


def test_multi_condition_pipeline_translates() -> None:
    """['Succeeded', 'Failed'] dependency should translate (equivalent to Completed)."""
    pipeline = _load("multi_condition_dependency")
    assert len(pipeline.tasks) == 2


def test_multi_condition_dependency_is_not_unsupported() -> None:
    """['Succeeded', 'Failed'] is equivalent to Completed and should be accepted."""
    pipeline = _load("multi_condition_dependency")
    log_task = next(t for t in pipeline.tasks if t.name == "log_outcome")
    assert log_task.depends_on is not None
    assert len(log_task.depends_on) == 1
    dep = log_task.depends_on[0]
    assert not isinstance(dep, UnsupportedValue)
    assert dep.task_key == "call_api"


def test_multi_condition_run_if_is_all_done() -> None:
    """['Succeeded', 'Failed'] is equivalent to Completed → ALL_DONE."""
    pipeline = _load("multi_condition_dependency")
    prepared = prepare_workflow(pipeline)
    log_task = next(t for t in prepared.tasks if t["task_key"] == "log_outcome")
    assert log_task.get("run_if") == "ALL_DONE"


def test_multi_condition_preparer_does_not_crash() -> None:
    """['Succeeded', 'Failed'] dependency should not crash the preparer."""
    pipeline = _load("multi_condition_dependency")
    prepared = prepare_workflow(pipeline)
    assert len(prepared.tasks) == 2


def test_skipped_returns_unsupported() -> None:
    """Skipped condition is not mapped and should return UnsupportedValue."""
    dep = {"activity": "upstream", "dependency_conditions": ["Skipped"]}
    result = _parse_dependency(dep)
    assert isinstance(result, UnsupportedValue)


def test_skipped_mixed_with_succeeded_returns_unsupported() -> None:
    """['Succeeded', 'Skipped'] should return UnsupportedValue."""
    dep = {"activity": "upstream", "dependency_conditions": ["Succeeded", "Skipped"]}
    result = _parse_dependency(dep)
    assert isinstance(result, UnsupportedValue)


def test_sibling_depends_on_if_condition_translates() -> None:
    """Pipeline where a sibling depends on an IfCondition with Completed should translate."""
    pipeline = _load("sibling_depends_on_if_condition")
    assert isinstance(pipeline, Pipeline)


def test_sibling_depends_on_if_condition_has_dual_outcome() -> None:
    """Sibling dependency on IfCondition should expand to outcome='true' + outcome='false'."""
    pipeline = _load("sibling_depends_on_if_condition")
    cleanup = next(t for t in pipeline.tasks if t.task_key == "cleanup")
    outcomes = [dep.outcome for dep in cleanup.depends_on]
    assert "true" in outcomes
    assert "false" in outcomes
    assert len(cleanup.depends_on) == 2


def test_sibling_depends_on_if_condition_run_if_all_done() -> None:
    """Sibling with Completed condition should have run_if=ALL_DONE."""
    pipeline = _load("sibling_depends_on_if_condition")
    cleanup = next(t for t in pipeline.tasks if t.task_key == "cleanup")
    assert cleanup.run_if == "ALL_DONE"


def test_sibling_depends_on_if_condition_preparer_does_not_crash() -> None:
    """Preparer should handle dual-outcome dependencies without error."""
    pipeline = _load("sibling_depends_on_if_condition")
    prepared = prepare_workflow(pipeline)
    task_keys = [t.task["task_key"] for t in prepared.activities]
    assert "cleanup" in task_keys


def test_sibling_depends_on_if_condition_child_outcome_unchanged() -> None:
    """IfCondition children should still have single outcome (not expanded)."""
    pipeline = _load("sibling_depends_on_if_condition")
    run_main = next(t for t in pipeline.tasks if t.task_key == "run_main")
    outcomes = [dep.outcome for dep in run_main.depends_on]
    assert outcomes == ["true"]

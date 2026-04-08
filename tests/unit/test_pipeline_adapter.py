"""Unit tests for the PipelineAdapter recursion guard."""

from __future__ import annotations

import warnings

from wkmigrate.definition_stores.pipeline_adapter import PipelineAdapter
from wkmigrate.enums.source_property_case import SourcePropertyCase


def _make_pipeline(name: str, child_ref: str | None = None) -> dict:
    """Build a minimal pipeline dict, optionally containing an ExecutePipeline activity."""
    activities: list[dict] = []
    if child_ref is not None:
        activities.append(
            {
                "name": f"call_{child_ref}",
                "type": "ExecutePipeline",
                "pipeline": {"reference_name": child_ref, "type": "PipelineReference"},
            }
        )
    return {"name": name, "activities": activities}


def test_circular_pipeline_reference_emits_warning_and_does_not_recurse() -> None:
    """A direct A->B->A cycle must not cause infinite recursion."""
    pipelines = {
        "pipeline_a": _make_pipeline("pipeline_a", child_ref="pipeline_b"),
        "pipeline_b": _make_pipeline("pipeline_b", child_ref="pipeline_a"),
    }

    adapter = PipelineAdapter(
        get_dataset=lambda name: {},
        get_linked_service=lambda name: {},
        get_pipeline=lambda name: pipelines[name],
        source_property_case=SourcePropertyCase.SNAKE,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["pipeline_a"])

    # pipeline_a should have enriched pipeline_b, but pipeline_b's reference
    # back to pipeline_a should have been skipped with a warning.
    warning_messages = [str(w.message) for w in caught]
    assert any("Circular pipeline reference detected" in m for m in warning_messages)
    assert any("pipeline_a" in m for m in warning_messages)

    # The top-level pipeline_a activity should still have a pipeline_definition
    top_activity = result["activities"][0]
    assert "pipeline_definition" in top_activity

    # The child pipeline_b's activity referencing pipeline_a should NOT have pipeline_definition
    child_pipeline = top_activity["pipeline_definition"]
    child_activity = child_pipeline["activities"][0]
    assert "pipeline_definition" not in child_activity


def test_self_referencing_pipeline_emits_warning() -> None:
    """A pipeline that references itself must not recurse."""
    pipelines = {
        "self_ref": _make_pipeline("self_ref", child_ref="self_ref"),
    }

    adapter = PipelineAdapter(
        get_dataset=lambda name: {},
        get_linked_service=lambda name: {},
        get_pipeline=lambda name: pipelines[name],
        source_property_case=SourcePropertyCase.SNAKE,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["self_ref"])

    warning_messages = [str(w.message) for w in caught]
    assert any("Circular pipeline reference detected" in m for m in warning_messages)
    assert any("self_ref" in m for m in warning_messages)

    # The activity should not have pipeline_definition since it was skipped
    activity = result["activities"][0]
    assert "pipeline_definition" not in activity


def test_transitive_cycle_detected() -> None:
    """A->B->C->A transitive cycle must be caught."""
    pipelines = {
        "a": _make_pipeline("a", child_ref="b"),
        "b": _make_pipeline("b", child_ref="c"),
        "c": _make_pipeline("c", child_ref="a"),
    }

    adapter = PipelineAdapter(
        get_dataset=lambda name: {},
        get_linked_service=lambda name: {},
        get_pipeline=lambda name: pipelines[name],
        source_property_case=SourcePropertyCase.SNAKE,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["a"])

    warning_messages = [str(w.message) for w in caught]
    assert any("Circular pipeline reference detected" in m for m in warning_messages)

    # a -> b enriched, b -> c enriched, c -> a skipped
    a_activity = result["activities"][0]
    assert "pipeline_definition" in a_activity
    b_pipeline = a_activity["pipeline_definition"]
    b_activity = b_pipeline["activities"][0]
    assert "pipeline_definition" in b_activity
    c_pipeline = b_activity["pipeline_definition"]
    c_activity = c_pipeline["activities"][0]
    assert "pipeline_definition" not in c_activity


def test_no_cycle_no_warning() -> None:
    """A linear chain A->B->C should not trigger any circular reference warning."""
    pipelines = {
        "a": _make_pipeline("a", child_ref="b"),
        "b": _make_pipeline("b", child_ref="c"),
        "c": _make_pipeline("c"),
    }

    adapter = PipelineAdapter(
        get_dataset=lambda name: {},
        get_linked_service=lambda name: {},
        get_pipeline=lambda name: pipelines[name],
        source_property_case=SourcePropertyCase.SNAKE,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["a"])

    circular_warnings = [w for w in caught if "Circular pipeline reference detected" in str(w.message)]
    assert len(circular_warnings) == 0

    # All pipeline_definitions should be present
    a_activity = result["activities"][0]
    assert "pipeline_definition" in a_activity
    b_activity = a_activity["pipeline_definition"]["activities"][0]
    assert "pipeline_definition" in b_activity

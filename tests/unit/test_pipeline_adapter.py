"""Unit tests for PipelineAdapter Execute Pipeline enrichment and recursion guard."""

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


def _make_adapter(pipelines: dict[str, dict]) -> PipelineAdapter:
    """Build a PipelineAdapter wired to the given pipeline lookup."""
    return PipelineAdapter(
        get_dataset=lambda name: {},
        get_linked_service=lambda name: {},
        get_pipeline=lambda name: pipelines[name],
        source_property_case=SourcePropertyCase.SNAKE,
    )


def _circular_warning_messages(caught: list) -> list[str]:
    """Extract circular-reference warning message strings."""
    return [str(w.message) for w in caught if "Circular pipeline reference detected" in str(w.message)]


def test_direct_cycle_emits_warning_and_stops_recursion() -> None:
    """A direct A->B->A cycle must not cause infinite recursion."""
    pipelines = {
        "pipeline_a": _make_pipeline("pipeline_a", child_ref="pipeline_b"),
        "pipeline_b": _make_pipeline("pipeline_b", child_ref="pipeline_a"),
    }
    adapter = _make_adapter(pipelines)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["pipeline_a"])

    circular = _circular_warning_messages(caught)
    assert len(circular) == 1
    assert "pipeline_a" in circular[0]

    top_activity = result["activities"][0]
    assert "pipeline_definition" in top_activity

    child_activity = top_activity["pipeline_definition"]["activities"][0]
    assert "pipeline_definition" not in child_activity


def test_self_referencing_pipeline_emits_warning() -> None:
    """A pipeline that references itself must not recurse."""
    pipelines = {"self_ref": _make_pipeline("self_ref", child_ref="self_ref")}
    adapter = _make_adapter(pipelines)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["self_ref"])

    circular = _circular_warning_messages(caught)
    assert len(circular) == 1
    assert "self_ref" in circular[0]

    assert "pipeline_definition" not in result["activities"][0]


def test_transitive_cycle_detected() -> None:
    """A->B->C->A transitive cycle must be caught."""
    pipelines = {
        "a": _make_pipeline("a", child_ref="b"),
        "b": _make_pipeline("b", child_ref="c"),
        "c": _make_pipeline("c", child_ref="a"),
    }
    adapter = _make_adapter(pipelines)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["a"])

    assert len(_circular_warning_messages(caught)) == 1

    a_activity = result["activities"][0]
    assert "pipeline_definition" in a_activity
    b_activity = a_activity["pipeline_definition"]["activities"][0]
    assert "pipeline_definition" in b_activity
    c_activity = b_activity["pipeline_definition"]["activities"][0]
    assert "pipeline_definition" not in c_activity


def test_linear_chain_produces_no_circular_warning() -> None:
    """A linear chain A->B->C should not trigger any circular reference warning."""
    pipelines = {
        "a": _make_pipeline("a", child_ref="b"),
        "b": _make_pipeline("b", child_ref="c"),
        "c": _make_pipeline("c"),
    }
    adapter = _make_adapter(pipelines)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = adapter.adapt(pipelines["a"])

    assert len(_circular_warning_messages(caught)) == 0

    a_activity = result["activities"][0]
    assert "pipeline_definition" in a_activity
    b_activity = a_activity["pipeline_definition"]["activities"][0]
    assert "pipeline_definition" in b_activity

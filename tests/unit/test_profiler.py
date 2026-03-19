"""Unit tests for the profiler module."""

from __future__ import annotations

from dataclasses import dataclass

from wkmigrate.models.ir.profile import (
    DatasetDetail,
    FactoryProfile,
    IntegrationRuntimeDetail,
    ObjectCount,
)
from wkmigrate.profiler import _collect_activities, format_profile, profile_factory


# ---------------------------------------------------------------------------
# Lightweight mock client for profiler tests
# ---------------------------------------------------------------------------

@dataclass
class _StubFactoryClient:
    """Minimal stand-in for FactoryClient used by profile_factory."""

    factory_name: str = "test-factory"
    _pipelines: list[dict] | None = None
    _datasets: list[dict] | None = None
    _linked_services: list[dict] | None = None
    _triggers: list[dict] | None = None
    _integration_runtimes: list[dict] | None = None

    def list_pipelines_full(self) -> list[dict]:
        return self._pipelines or []

    def list_datasets(self) -> list[dict]:
        return self._datasets or []

    def list_linked_services(self) -> list[dict]:
        return self._linked_services or []

    def list_triggers(self) -> list[dict]:
        return self._triggers or []

    def list_integration_runtimes(self) -> list[dict]:
        return self._integration_runtimes or []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pipeline(activities: list[dict]) -> dict:
    return {"name": "pipeline1", "activities": activities}


def _make_activity(name: str, activity_type: str, **extra: object) -> dict:
    result: dict = {"name": name, "type": activity_type}
    result.update(extra)
    return result


def _make_dataset(name: str, ds_type: str, ls_name: str | None = None) -> dict:
    props: dict = {"type": ds_type}
    if ls_name is not None:
        props["linked_service_name"] = {"reference_name": ls_name}
    return {"name": name, "properties": props}


def _make_linked_service(name: str, ls_type: str) -> dict:
    return {"name": name, "properties": {"type": ls_type}}


def _make_integration_runtime(name: str, rt_type: str, node_count: int | None = None) -> dict:
    props: dict = {"type": rt_type}
    if node_count is not None:
        props["type_properties"] = {"compute_properties": {"number_of_nodes": node_count}}
    return {"name": name, "properties": props}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProfileCountsPipelines:
    def test_counts_pipelines(self) -> None:
        client = _StubFactoryClient(
            _pipelines=[_make_pipeline([]), _make_pipeline([])],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.pipelines.total == 2
        assert result.pipelines.supported == 2
        assert result.pipelines.unsupported == 0


class TestProfileCountsActivities:
    def test_counts_flat_activities(self) -> None:
        client = _StubFactoryClient(
            _pipelines=[
                _make_pipeline([
                    _make_activity("a1", "Copy"),
                    _make_activity("a2", "Lookup"),
                ]),
            ],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.activities.total == 2
        assert result.activities.supported == 2

    def test_counts_activities_across_pipelines(self) -> None:
        client = _StubFactoryClient(
            _pipelines=[
                _make_pipeline([_make_activity("a1", "Copy")]),
                _make_pipeline([_make_activity("a2", "Lookup")]),
            ],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.activities.total == 2


class TestProfileSupportedVsUnsupported:
    def test_supported_vs_unsupported_activities(self) -> None:
        client = _StubFactoryClient(
            _pipelines=[
                _make_pipeline([
                    _make_activity("a1", "Copy"),
                    _make_activity("a2", "ExecutePipeline"),
                    _make_activity("a3", "AzureFunctionActivity"),
                ]),
            ],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.activities.supported == 1
        assert result.activities.unsupported == 2
        assert "ExecutePipeline" in result.unsupported_activity_types
        assert "AzureFunctionActivity" in result.unsupported_activity_types

    def test_supported_vs_unsupported_datasets(self) -> None:
        client = _StubFactoryClient(
            _datasets=[
                _make_dataset("ds1", "Parquet"),
                _make_dataset("ds2", "CosmosDbSqlApiCollection"),
            ],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.datasets.supported == 1
        assert result.datasets.unsupported == 1
        assert "CosmosDbSqlApiCollection" in result.unsupported_dataset_types

    def test_supported_vs_unsupported_linked_services(self) -> None:
        client = _StubFactoryClient(
            _linked_services=[
                _make_linked_service("ls1", "AzureBlobFS"),
                _make_linked_service("ls2", "Salesforce"),
            ],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.linked_services.supported == 1
        assert result.linked_services.unsupported == 1


class TestProfileDatasetDetails:
    def test_dataset_details_populated(self) -> None:
        client = _StubFactoryClient(
            _datasets=[_make_dataset("ds1", "Parquet", ls_name="ls1")],
            _linked_services=[_make_linked_service("ls1", "AzureBlobFS")],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        assert len(result.dataset_details) == 1
        detail = result.dataset_details[0]
        assert detail.dataset_name == "ds1"
        assert detail.dataset_type == "Parquet"
        assert detail.linked_service_name == "ls1"
        assert detail.linked_service_type == "AzureBlobFS"

    def test_dataset_detail_missing_linked_service(self) -> None:
        client = _StubFactoryClient(
            _datasets=[_make_dataset("ds1", "Parquet")],
        )
        result = profile_factory(client)  # type: ignore[arg-type]
        detail = result.dataset_details[0]
        assert detail.linked_service_name is None
        assert detail.linked_service_type is None


class TestProfileEmptyFactory:
    def test_empty_factory(self) -> None:
        client = _StubFactoryClient()
        result = profile_factory(client)  # type: ignore[arg-type]
        assert result.pipelines.total == 0
        assert result.activities.total == 0
        assert result.datasets.total == 0
        assert result.linked_services.total == 0
        assert result.triggers.total == 0
        assert result.integration_runtimes.total == 0
        assert result.dataset_details == []
        assert result.integration_runtime_details == []
        assert result.unsupported_activity_types == []
        assert result.unsupported_dataset_types == []


class TestFormatProfileOutput:
    def test_format_contains_factory_name(self) -> None:
        profile = FactoryProfile(
            factory_name="my-factory",
            pipelines=ObjectCount(3, 3, 0),
            activities=ObjectCount(10, 8, 2),
            linked_services=ObjectCount(5, 4, 1),
            datasets=ObjectCount(7, 6, 1),
            triggers=ObjectCount(2, 2, 0),
            integration_runtimes=ObjectCount(1, 1, 0),
            unsupported_activity_types=["ExecutePipeline"],
            unsupported_dataset_types=["CosmosDb"],
        )
        output = format_profile(profile)
        assert "my-factory" in output
        assert "Pipelines:            3" in output
        assert "8 supported, 2 unsupported" in output
        assert "ExecutePipeline" in output
        assert "CosmosDb" in output

    def test_format_empty_profile(self) -> None:
        profile = FactoryProfile(
            factory_name="empty",
            pipelines=ObjectCount(0, 0, 0),
            activities=ObjectCount(0, 0, 0),
            linked_services=ObjectCount(0, 0, 0),
            datasets=ObjectCount(0, 0, 0),
            triggers=ObjectCount(0, 0, 0),
            integration_runtimes=ObjectCount(0, 0, 0),
        )
        output = format_profile(profile)
        assert "empty" in output
        assert "Unsupported" not in output

    def test_format_dataset_details(self) -> None:
        profile = FactoryProfile(
            factory_name="f",
            pipelines=ObjectCount(0, 0, 0),
            activities=ObjectCount(0, 0, 0),
            linked_services=ObjectCount(0, 0, 0),
            datasets=ObjectCount(1, 1, 0),
            triggers=ObjectCount(0, 0, 0),
            integration_runtimes=ObjectCount(0, 0, 0),
            dataset_details=[DatasetDetail("ds1", "Parquet", "ls1", "AzureBlobFS")],
        )
        output = format_profile(profile)
        assert "ds1 (Parquet) via ls1 [AzureBlobFS]" in output

    def test_format_integration_runtime_details(self) -> None:
        profile = FactoryProfile(
            factory_name="f",
            pipelines=ObjectCount(0, 0, 0),
            activities=ObjectCount(0, 0, 0),
            linked_services=ObjectCount(0, 0, 0),
            datasets=ObjectCount(0, 0, 0),
            triggers=ObjectCount(0, 0, 0),
            integration_runtimes=ObjectCount(1, 1, 0),
            integration_runtime_details=[IntegrationRuntimeDetail("ir1", "SelfHosted", 3)],
        )
        output = format_profile(profile)
        assert "ir1 (SelfHosted, 3 nodes)" in output


class TestCollectActivitiesRecursive:
    def test_foreach_nesting(self) -> None:
        inner = _make_activity("inner", "Copy")
        foreach = _make_activity("foreach", "ForEach", activities=[inner])
        result: list[dict] = []
        _collect_activities([foreach], result)
        assert len(result) == 2
        assert result[0]["name"] == "foreach"
        assert result[1]["name"] == "inner"

    def test_if_condition_branches(self) -> None:
        true_act = _make_activity("true_act", "Copy")
        false_act = _make_activity("false_act", "Lookup")
        if_cond = _make_activity(
            "if_cond",
            "IfCondition",
            if_true_activities=[true_act],
            if_false_activities=[false_act],
        )
        result: list[dict] = []
        _collect_activities([if_cond], result)
        assert len(result) == 3
        names = [a["name"] for a in result]
        assert "if_cond" in names
        assert "true_act" in names
        assert "false_act" in names

    def test_deeply_nested(self) -> None:
        inner = _make_activity("inner", "Copy")
        foreach = _make_activity("foreach", "ForEach", activities=[inner])
        if_cond = _make_activity("if_cond", "IfCondition", if_true_activities=[foreach])
        result: list[dict] = []
        _collect_activities([if_cond], result)
        assert len(result) == 3

    def test_empty_activities(self) -> None:
        result: list[dict] = []
        _collect_activities([], result)
        assert result == []

"""Unit tests for the profiler module."""

from __future__ import annotations

from tests.conftest import (
    StubFactoryClient,
    make_activity,
    make_dataset,
    make_linked_service,
    make_pipeline,
)

from wkmigrate.profiler import (
    DatasetDetail,
    FactoryProfile,
    IntegrationRuntimeDetail,
    ObjectCount,
    format_profile,
    profile_factory,
)


def test_counts_pipelines() -> None:
    client = StubFactoryClient(
        _pipelines=[make_pipeline([]), make_pipeline([])],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.pipelines.total == 2
    assert result.pipelines.supported == 2
    assert result.pipelines.unsupported == 0


def test_counts_flat_activities() -> None:
    client = StubFactoryClient(
        _pipelines=[
            make_pipeline(
                [
                    make_activity("a1", "Copy"),
                    make_activity("a2", "Lookup"),
                ]
            ),
        ],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.total == 2
    assert result.activities.supported == 2


def test_counts_activities_across_pipelines() -> None:
    client = StubFactoryClient(
        _pipelines=[
            make_pipeline([make_activity("a1", "Copy")]),
            make_pipeline([make_activity("a2", "Lookup")]),
        ],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.total == 2


def test_supported_vs_unsupported_activities() -> None:
    client = StubFactoryClient(
        _pipelines=[
            make_pipeline(
                [
                    make_activity("a1", "Copy"),
                    make_activity("a2", "ExecutePipeline"),
                    make_activity("a3", "AzureFunctionActivity"),
                ]
            ),
        ],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.supported == 1
    assert result.activities.unsupported == 2
    assert "ExecutePipeline" in result.unsupported_activity_types
    assert "AzureFunctionActivity" in result.unsupported_activity_types


def test_supported_vs_unsupported_datasets() -> None:
    client = StubFactoryClient(
        _datasets=[
            make_dataset("ds1", "Parquet"),
            make_dataset("ds2", "CosmosDbSqlApiCollection"),
        ],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.datasets.supported == 1
    assert result.datasets.unsupported == 1
    assert "CosmosDbSqlApiCollection" in result.unsupported_dataset_types


def test_supported_vs_unsupported_linked_services() -> None:
    client = StubFactoryClient(
        _linked_services=[
            make_linked_service("ls1", "AzureBlobFS"),
            make_linked_service("ls2", "Salesforce"),
        ],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.linked_services.supported == 1
    assert result.linked_services.unsupported == 1


def test_dataset_details_populated() -> None:
    client = StubFactoryClient(
        _datasets=[make_dataset("ds1", "Parquet", ls_name="ls1")],
        _linked_services=[make_linked_service("ls1", "AzureBlobFS")],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    assert len(result.dataset_details) == 1
    detail = result.dataset_details[0]
    assert detail.dataset_name == "ds1"
    assert detail.dataset_type == "Parquet"
    assert detail.linked_service_name == "ls1"
    assert detail.linked_service_type == "AzureBlobFS"


def test_dataset_detail_missing_linked_service() -> None:
    client = StubFactoryClient(
        _datasets=[make_dataset("ds1", "Parquet")],
    )
    result = profile_factory(client)  # type: ignore[arg-type]
    detail = result.dataset_details[0]
    assert detail.linked_service_name is None
    assert detail.linked_service_type is None


def test_empty_factory() -> None:
    client = StubFactoryClient()
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


def test_format_contains_factory_name() -> None:
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


def test_format_empty_profile() -> None:
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


def test_format_dataset_details() -> None:
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


def test_format_integration_runtime_details() -> None:
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


def test_foreach_nesting() -> None:
    inner = make_activity("inner", "Copy")
    foreach = make_activity("foreach", "ForEach", activities=[inner])
    client = StubFactoryClient(_pipelines=[make_pipeline([foreach])])
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.total == 2
    assert result.activities.supported == 2


def test_if_condition_branches() -> None:
    true_act = make_activity("true_act", "Copy")
    false_act = make_activity("false_act", "Lookup")
    if_cond = make_activity(
        "if_cond",
        "IfCondition",
        if_true_activities=[true_act],
        if_false_activities=[false_act],
    )
    client = StubFactoryClient(_pipelines=[make_pipeline([if_cond])])
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.total == 3
    assert result.activities.supported == 3


def test_deeply_nested() -> None:
    inner = make_activity("inner", "Copy")
    foreach = make_activity("foreach", "ForEach", activities=[inner])
    if_cond = make_activity("if_cond", "IfCondition", if_true_activities=[foreach])
    client = StubFactoryClient(_pipelines=[make_pipeline([if_cond])])
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.total == 3


def test_empty_activities() -> None:
    client = StubFactoryClient(_pipelines=[make_pipeline([])])
    result = profile_factory(client)  # type: ignore[arg-type]
    assert result.activities.total == 0

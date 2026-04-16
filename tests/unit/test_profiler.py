"""Unit tests for the profiler package."""

from wkmigrate.profiler.profile import (
    DatasetDetail,
    FactoryProfile,
    IntegrationRuntimeDetail,
    ObjectCount,
)
from wkmigrate.profiler.profiler import (
    _collect_activities,
    _count_activities,
    _count_datasets,
    _count_linked_services,
    _build_integration_runtime_details,
    format_profile,
)


# -- _collect_activities -------------------------------------------------------


def test_collect_flat_activities():
    activities = [{"type": "Copy", "name": "A"}, {"type": "Lookup", "name": "B"}]
    result: list[dict] = []
    _collect_activities(activities, result)
    assert len(result) == 2


def test_collect_nested_for_each():
    activities = [
        {
            "type": "ForEach",
            "name": "Loop",
            "activities": [
                {"type": "Copy", "name": "Inner1"},
                {"type": "Lookup", "name": "Inner2"},
            ],
        }
    ]
    result: list[dict] = []
    _collect_activities(activities, result)
    assert len(result) == 3


def test_collect_nested_if_condition_branches():
    activities = [
        {
            "type": "IfCondition",
            "name": "Branch",
            "if_true_activities": [{"type": "Copy", "name": "TruePath"}],
            "if_false_activities": [
                {"type": "WebActivity", "name": "FalsePath"},
                {"type": "ExecutePipeline", "name": "AlsoFalse"},
            ],
        }
    ]
    result: list[dict] = []
    _collect_activities(activities, result)
    # IfCondition itself + 1 true + 2 false = 4
    assert len(result) == 4


def test_collect_empty_and_none():
    result: list[dict] = []
    _collect_activities([], result)
    assert result == []

    _collect_activities(None, result)
    assert result == []


# -- _count_activities ---------------------------------------------------------


def test_count_all_supported():
    pipelines = [{"activities": [{"type": "Copy"}, {"type": "Lookup"}]}]
    counts, unsupported = _count_activities(pipelines)
    assert counts == ObjectCount(total=2, supported=2, unsupported=0)
    assert unsupported == []


def test_count_all_unsupported():
    pipelines = [{"activities": [{"type": "ExecutePipeline"}, {"type": "AzureFunctionActivity"}]}]
    counts, unsupported = _count_activities(pipelines)
    assert counts == ObjectCount(total=2, supported=0, unsupported=2)
    assert unsupported == ["AzureFunctionActivity", "ExecutePipeline"]


def test_count_mixed():
    pipelines = [
        {
            "activities": [
                {"type": "Copy"},
                {"type": "ExecutePipeline"},
                {"type": "ForEach", "activities": [{"type": "DatabricksNotebook"}]},
            ]
        }
    ]
    counts, unsupported = _count_activities(pipelines)
    # Copy + ExecutePipeline + ForEach + DatabricksNotebook = 4 total
    assert counts.total == 4
    assert counts.supported == 3  # Copy, ForEach, DatabricksNotebook
    assert counts.unsupported == 1
    assert unsupported == ["ExecutePipeline"]


def test_count_activities_empty_pipeline():
    pipelines = [{"activities": []}]
    counts, unsupported = _count_activities(pipelines)
    assert counts == ObjectCount(total=0, supported=0, unsupported=0)
    assert unsupported == []


def test_count_activities_skips_non_dict_pipelines():
    pipelines = ["not_a_dict", {"activities": [{"type": "Copy"}]}]
    counts, _ = _count_activities(pipelines)
    assert counts.total == 1


# -- _count_datasets -----------------------------------------------------------


def test_count_datasets_supported():
    datasets = [
        {"name": "ds1", "properties": {"type": "Parquet", "linked_service_name": {"reference_name": "ls1"}}},
        {"name": "ds2", "properties": {"type": "AzureSqlTable", "linked_service_name": {"reference_name": "ls2"}}},
    ]
    linked_services = [
        {"name": "ls1", "properties": {"type": "AzureBlobFS"}},
        {"name": "ls2", "properties": {"type": "AzureSqlDatabase"}},
    ]
    counts, details, unsupported = _count_datasets(datasets, linked_services)
    assert counts == ObjectCount(total=2, supported=2, unsupported=0)
    assert unsupported == []
    assert details[0] == DatasetDetail("ds1", "Parquet", "ls1", "AzureBlobFS")
    assert details[1] == DatasetDetail("ds2", "AzureSqlTable", "ls2", "AzureSqlDatabase")


def test_count_datasets_unsupported():
    datasets = [
        {"name": "ds1", "properties": {"type": "SalesforceObject"}},
    ]
    counts, details, unsupported = _count_datasets(datasets, [])
    assert counts == ObjectCount(total=1, supported=0, unsupported=1)
    assert unsupported == ["SalesforceObject"]
    assert details[0].linked_service_name is None


def test_count_datasets_missing_type():
    datasets = [{"name": "ds1", "properties": {}}]
    counts, details, unsupported = _count_datasets(datasets, [])
    assert counts.unsupported == 1
    assert details[0].dataset_type == "Unknown"


# -- _count_linked_services ----------------------------------------------------


def test_count_linked_services_mixed():
    linked_services = [
        {"name": "ls1", "properties": {"type": "AzureBlobFS"}},
        {"name": "ls2", "properties": {"type": "CosmosDb"}},
        {"name": "ls3", "properties": {"type": "AzureDatabricks"}},
    ]
    counts = _count_linked_services(linked_services)
    assert counts == ObjectCount(total=3, supported=2, unsupported=1)


# -- _build_integration_runtime_details ----------------------------------------


def test_integration_runtime_managed():
    runtimes = [{"name": "ir-managed", "properties": {"type": "Managed"}}]
    counts, details = _build_integration_runtime_details(runtimes)
    assert counts == ObjectCount(total=1, supported=1, unsupported=0)
    assert details == [IntegrationRuntimeDetail("ir-managed", "Managed", node_count=None)]


def test_integration_runtime_self_hosted_with_nodes():
    runtimes = [
        {
            "name": "ir-self",
            "properties": {
                "type": "SelfHosted",
                "type_properties": {"compute_properties": {"number_of_nodes": 4}},
            },
        }
    ]
    _, details = _build_integration_runtime_details(runtimes)
    assert details[0].node_count == 4


# -- format_profile ------------------------------------------------------------


def test_format_profile_basic():
    profile = FactoryProfile(
        factory_name="test-factory",
        pipelines=ObjectCount(2, 2, 0),
        activities=ObjectCount(5, 3, 2),
        linked_services=ObjectCount(3, 2, 1),
        datasets=ObjectCount(4, 3, 1),
        triggers=ObjectCount(1, 1, 0),
        integration_runtimes=ObjectCount(1, 1, 0),
        unsupported_activity_types=["ExecutePipeline", "Wait"],
        unsupported_dataset_types=["SalesforceObject"],
    )
    text = format_profile(profile)
    assert "test-factory" in text
    assert "3 supported, 2 unsupported" in text
    assert "ExecutePipeline" in text
    assert "Wait" in text
    assert "SalesforceObject" in text


def test_format_profile_no_unsupported():
    profile = FactoryProfile(
        factory_name="clean-factory",
        pipelines=ObjectCount(1, 1, 0),
        activities=ObjectCount(2, 2, 0),
        linked_services=ObjectCount(1, 1, 0),
        datasets=ObjectCount(1, 1, 0),
        triggers=ObjectCount(0, 0, 0),
        integration_runtimes=ObjectCount(0, 0, 0),
    )
    text = format_profile(profile)
    assert "Unsupported" not in text


def test_format_profile_dataset_details():
    profile = FactoryProfile(
        factory_name="f",
        pipelines=ObjectCount(0, 0, 0),
        activities=ObjectCount(0, 0, 0),
        linked_services=ObjectCount(0, 0, 0),
        datasets=ObjectCount(1, 1, 0),
        triggers=ObjectCount(0, 0, 0),
        integration_runtimes=ObjectCount(0, 0, 0),
        dataset_details=[DatasetDetail("my_ds", "Parquet", "my_ls", "AzureBlobFS")],
    )
    text = format_profile(profile)
    assert "my_ds (Parquet) via my_ls [AzureBlobFS]" in text


def test_format_profile_integration_runtime_details():
    profile = FactoryProfile(
        factory_name="f",
        pipelines=ObjectCount(0, 0, 0),
        activities=ObjectCount(0, 0, 0),
        linked_services=ObjectCount(0, 0, 0),
        datasets=ObjectCount(0, 0, 0),
        triggers=ObjectCount(0, 0, 0),
        integration_runtimes=ObjectCount(1, 1, 0),
        integration_runtime_details=[IntegrationRuntimeDetail("ir-self", "SelfHosted", node_count=4)],
    )
    text = format_profile(profile)
    assert "ir-self (SelfHosted, 4 nodes)" in text

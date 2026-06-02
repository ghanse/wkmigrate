"""Unit tests for the profiler package."""

import pytest

from wkmigrate.profiler.profile import (
    DatasetDetail,
    FactoryProfile,
    IntegrationRuntimeDetail,
    ObjectCount,
    PipelineDetail,
)
from wkmigrate.profiler.profiler import (
    _build_integration_runtime_details,
    _build_pipeline_details,
    _camel_to_snake,
    _collect_activities,
    _count_activities,
    _count_datasets,
    _count_linked_services,
    _leaf_resource_name,
    _load_from_arm,
    format_profile,
    profile_factory,
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


# -- profile_factory argument validation ---------------------------------------


def test_profile_factory_requires_client_or_arm_template():
    with pytest.raises(ValueError, match="must be provided"):
        profile_factory()


def test_profile_factory_rejects_both_client_and_arm_template():
    # Minimal sentinel; we won't reach the client path.
    class _Sentinel:
        factory_name = "f"

    with pytest.raises(ValueError, match="not both"):
        profile_factory(client=_Sentinel(), arm_template={"resources": []})  # type: ignore[arg-type]


# -- _camel_to_snake / _leaf_resource_name -------------------------------------


def test_camel_to_snake_basic():
    assert _camel_to_snake("camelCase") == "camel_case"
    assert _camel_to_snake("linkedServiceName") == "linked_service_name"
    assert _camel_to_snake("snake_case") == "snake_case"
    # ARM JSON field names are camelCase by convention (no all-caps acronyms),
    # so the simple ``insert _ before every uppercase`` rule is sufficient.
    # We don't test pathological cases like "URL" because they don't occur.


def test_leaf_resource_name_strips_factory_prefix():
    assert _leaf_resource_name("my-factory/my-pipeline") == "my-pipeline"
    assert _leaf_resource_name("standalone-name") == "standalone-name"
    assert _leaf_resource_name("") == ""


# -- _load_from_arm ------------------------------------------------------------


def _arm_template_sample() -> dict:
    """Returns a small but representative ARM template for tests."""
    return {
        "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
        "resources": [
            {
                "type": "Microsoft.DataFactory/factories",
                "name": "my-factory",
                "properties": {},
            },
            {
                "type": "Microsoft.DataFactory/factories/pipelines",
                "name": "my-factory/orders-etl",
                "properties": {
                    "activities": [
                        {
                            "type": "Copy",
                            "name": "copyOrders",
                            "inputs": [
                                {"referenceName": "ds_blob_orders", "type": "DatasetReference"}
                            ],
                            "outputs": [
                                {"referenceName": "ds_sql_orders", "type": "DatasetReference"}
                            ],
                        },
                        {
                            "type": "DatabricksNotebook",
                            "name": "transformOrders",
                            "linkedServiceName": {
                                "referenceName": "AzureDatabricks_LS",
                                "type": "LinkedServiceReference",
                            },
                        },
                    ],
                    "parameters": {"region": {"type": "String"}},
                },
            },
            {
                "type": "Microsoft.DataFactory/factories/pipelines",
                "name": "my-factory/idle-pipeline",
                "properties": {"activities": []},
            },
            {
                "type": "Microsoft.DataFactory/factories/datasets",
                "name": "my-factory/ds_blob_orders",
                "properties": {
                    "type": "Parquet",
                    "linkedServiceName": {
                        "referenceName": "AzureBlobFS_LS",
                        "type": "LinkedServiceReference",
                    },
                },
            },
            {
                "type": "Microsoft.DataFactory/factories/datasets",
                "name": "my-factory/ds_sql_orders",
                "properties": {
                    "type": "AzureSqlTable",
                    "linkedServiceName": {
                        "referenceName": "AzureSql_LS",
                        "type": "LinkedServiceReference",
                    },
                },
            },
            {
                "type": "Microsoft.DataFactory/factories/linkedservices",
                "name": "my-factory/AzureBlobFS_LS",
                "properties": {"type": "AzureBlobFS"},
            },
            {
                "type": "Microsoft.DataFactory/factories/linkedservices",
                "name": "my-factory/AzureSql_LS",
                "properties": {
                    "type": "AzureSqlDatabase",
                    "connectVia": {
                        "referenceName": "SelfHostedIR_OnPrem",
                        "type": "IntegrationRuntimeReference",
                    },
                },
            },
            {
                "type": "Microsoft.DataFactory/factories/linkedservices",
                "name": "my-factory/AzureDatabricks_LS",
                "properties": {"type": "AzureDatabricks"},
            },
            {
                "type": "Microsoft.DataFactory/factories/triggers",
                "name": "my-factory/daily-trigger",
                "properties": {
                    "type": "ScheduleTrigger",
                    "pipelines": [
                        {
                            "pipelineReference": {
                                "referenceName": "orders-etl",
                                "type": "PipelineReference",
                            }
                        }
                    ],
                },
            },
            {
                "type": "Microsoft.DataFactory/factories/integrationRuntimes",
                "name": "my-factory/SelfHostedIR_OnPrem",
                "properties": {
                    "type": "SelfHosted",
                    "typeProperties": {"computeProperties": {"numberOfNodes": 2}},
                },
            },
        ],
    }


def test_load_from_arm_factory_name_inferred_from_factory_resource():
    loaded = _load_from_arm(_arm_template_sample())
    assert loaded["factory_name"] == "my-factory"
    assert {p["name"] for p in loaded["pipelines"]} == {"orders-etl", "idle-pipeline"}
    assert {ds["name"] for ds in loaded["datasets"]} == {"ds_blob_orders", "ds_sql_orders"}


def test_load_from_arm_normalises_camel_to_snake():
    loaded = _load_from_arm(_arm_template_sample())
    orders = next(p for p in loaded["pipelines"] if p["name"] == "orders-etl")
    # Activities lifted to top-level and keys snake_cased
    copy_act = orders["activities"][0]
    assert copy_act["inputs"][0]["reference_name"] == "ds_blob_orders"
    assert copy_act["outputs"][0]["reference_name"] == "ds_sql_orders"
    nb = orders["activities"][1]
    assert nb["linked_service_name"]["reference_name"] == "AzureDatabricks_LS"


def test_load_from_arm_falls_back_to_override_when_no_factory_resource():
    # An ARM template without a top-level factories resource should still load.
    template = {
        "resources": [
            {
                "type": "Microsoft.DataFactory/factories/pipelines",
                "name": "ignored/orphan",
                "properties": {"activities": []},
            }
        ]
    }
    loaded = _load_from_arm(template, factory_name_override="my-override")
    assert loaded["factory_name"] == "my-override"


def test_load_from_arm_factory_name_unknown_when_neither_source_present():
    loaded = _load_from_arm({"resources": []})
    assert loaded["factory_name"] == "Unknown"


def test_profile_factory_via_arm_template_end_to_end():
    """Full end-to-end profiling against a parsed ARM template."""
    profile = profile_factory(arm_template=_arm_template_sample())
    assert profile.factory_name == "my-factory"
    assert profile.pipelines.total == 2
    # 3 activities total: Copy + DatabricksNotebook + (none in idle pipeline)
    assert profile.activities.total == 2
    assert profile.activities.supported == 2
    # 2 datasets (both supported types)
    assert profile.datasets.total == 2
    assert profile.datasets.supported == 2
    # 3 linked services (all supported)
    assert profile.linked_services.total == 3
    assert profile.triggers.total == 1
    assert profile.integration_runtimes.total == 1


# -- _build_pipeline_details ---------------------------------------------------


def test_pipeline_details_via_arm_template():
    profile = profile_factory(arm_template=_arm_template_sample())
    details_by_name = {pd.pipeline_name: pd for pd in profile.pipeline_details}
    assert set(details_by_name) == {"orders-etl", "idle-pipeline"}

    orders = details_by_name["orders-etl"]
    # orders-etl has 2 activities (Copy + DatabricksNotebook), both supported
    assert orders.activities == ObjectCount(total=2, supported=2, unsupported=0)
    # References 2 datasets, both supported types
    assert orders.datasets.total == 2
    assert orders.datasets.supported == 2
    # Linked services reached: AzureBlobFS_LS + AzureSql_LS (via datasets) +
    # AzureDatabricks_LS (directly referenced by the notebook activity) = 3
    assert orders.linked_services.total == 3
    assert orders.linked_services.supported == 3
    # 1 trigger binds this pipeline; 1 distinct IR is reached via AzureSql_LS.connect_via
    assert orders.total_triggers == 1
    assert orders.total_integration_runtimes == 1
    # The mirror totals match the ObjectCount totals
    assert orders.total_activities == orders.activities.total
    assert orders.total_datasets == orders.datasets.total
    assert orders.total_linked_services == orders.linked_services.total

    idle = details_by_name["idle-pipeline"]
    # Empty pipeline → zeros everywhere
    assert idle.activities == ObjectCount(0, 0, 0)
    assert idle.datasets == ObjectCount(0, 0, 0)
    assert idle.linked_services == ObjectCount(0, 0, 0)
    assert idle.total_triggers == 0
    assert idle.total_integration_runtimes == 0


def test_pipeline_details_supported_vs_unsupported_split():
    """A pipeline mixing supported + unsupported activities and datasets surfaces both."""
    pipelines = [
        {
            "name": "mixed",
            "activities": [
                {"type": "Copy", "name": "ok"},
                {"type": "ExecutePipeline", "name": "bad", "type_properties": {}},
                {
                    "type": "Lookup",
                    "name": "look",
                    "type_properties": {
                        "dataset": {"reference_name": "unsupported_ds", "type": "DatasetReference"}
                    },
                },
            ],
        }
    ]
    datasets = [
        {
            "name": "unsupported_ds",
            "properties": {
                "type": "SalesforceObject",
                "linked_service_name": {"reference_name": "Salesforce_LS", "type": "LinkedServiceReference"},
            },
        }
    ]
    linked_services = [{"name": "Salesforce_LS", "properties": {"type": "Salesforce"}}]
    details = _build_pipeline_details(
        pipelines=pipelines,
        datasets=datasets,
        linked_services=linked_services,
        triggers=[],
        integration_runtimes=[],
    )
    assert len(details) == 1
    pd = details[0]
    assert pd.pipeline_name == "mixed"
    # 3 activities: Copy (supported), ExecutePipeline (unsupported), Lookup (supported)
    assert pd.activities == ObjectCount(total=3, supported=2, unsupported=1)
    # 1 dataset referenced, type is unsupported
    assert pd.datasets == ObjectCount(total=1, supported=0, unsupported=1)
    # The dataset's LS is unsupported
    assert pd.linked_services == ObjectCount(total=1, supported=0, unsupported=1)


def test_pipeline_details_triggers_only_counted_when_bound_to_pipeline():
    pipelines = [
        {"name": "p1", "activities": []},
        {"name": "p2", "activities": []},
    ]
    triggers = [
        {
            "name": "trig-p1",
            "properties": {
                "type": "ScheduleTrigger",
                "pipelines": [{"pipeline_reference": {"reference_name": "p1", "type": "PipelineReference"}}],
            },
        }
    ]
    details = _build_pipeline_details(
        pipelines=pipelines, datasets=[], linked_services=[], triggers=triggers, integration_runtimes=[]
    )
    by_name = {pd.pipeline_name: pd for pd in details}
    assert by_name["p1"].total_triggers == 1
    assert by_name["p2"].total_triggers == 0


def test_pipeline_details_integration_runtime_via_linked_service_connect_via():
    """IR count is derived from connect_via on the linked services a pipeline uses."""
    pipelines = [
        {
            "name": "p",
            "activities": [
                {
                    "type": "Copy",
                    "name": "cp",
                    "inputs": [{"reference_name": "ds", "type": "DatasetReference"}],
                }
            ],
        }
    ]
    datasets = [
        {
            "name": "ds",
            "properties": {
                "type": "Parquet",
                "linked_service_name": {"reference_name": "LS", "type": "LinkedServiceReference"},
            },
        }
    ]
    linked_services = [
        {
            "name": "LS",
            "properties": {
                "type": "AzureSqlDatabase",
                "connect_via": {"reference_name": "OnPremIR", "type": "IntegrationRuntimeReference"},
            },
        }
    ]
    [pd] = _build_pipeline_details(
        pipelines=pipelines,
        datasets=datasets,
        linked_services=linked_services,
        triggers=[],
        integration_runtimes=[{"name": "OnPremIR", "properties": {"type": "SelfHosted"}}],
    )
    assert pd.total_integration_runtimes == 1


def test_format_profile_pipeline_details_section_rendered():
    profile = FactoryProfile(
        factory_name="f",
        pipelines=ObjectCount(1, 1, 0),
        activities=ObjectCount(0, 0, 0),
        linked_services=ObjectCount(0, 0, 0),
        datasets=ObjectCount(0, 0, 0),
        triggers=ObjectCount(0, 0, 0),
        integration_runtimes=ObjectCount(0, 0, 0),
        pipeline_details=[
            PipelineDetail(
                pipeline_name="orders-etl",
                activities=ObjectCount(3, 2, 1),
                datasets=ObjectCount(2, 2, 0),
                linked_services=ObjectCount(2, 1, 1),
                total_activities=3,
                total_datasets=2,
                total_linked_services=2,
                total_triggers=1,
                total_integration_runtimes=1,
            )
        ],
    )
    text = format_profile(profile)
    assert "Pipeline Details:" in text
    assert "- orders-etl" in text
    assert "Activities:      3 (2 supported, 1 unsupported)" in text
    assert "Datasets:        2 (2 supported, 0 unsupported)" in text
    assert "Linked Services: 2 (1 supported, 1 unsupported)" in text
    assert "Triggers: 1, Integration Runtimes: 1" in text

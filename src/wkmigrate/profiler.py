"""Profile an Azure Data Factory resource to assess migration readiness."""

from __future__ import annotations

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.models.ir.profile import (
    DatasetDetail,
    FactoryProfile,
    IntegrationRuntimeDetail,
    ObjectCount,
)

SUPPORTED_ACTIVITY_TYPES = {
    "Copy",
    "DatabricksJob",
    "DatabricksNotebook",
    "DatabricksSparkJar",
    "DatabricksSparkPython",
    "ForEach",
    "IfCondition",
    "Lookup",
    "SetVariable",
    "WebActivity",
}

SUPPORTED_DATASET_TYPES = {
    "Avro",
    "DelimitedText",
    "Json",
    "Orc",
    "Parquet",
    "AzureSqlTable",
    "AzurePostgreSqlTable",
    "AzureMySqlTable",
    "OracleTable",
    "AzureDatabricksDeltaLakeDataset",
}

SUPPORTED_LINKED_SERVICE_TYPES = {
    "AzureBlobFS",
    "AzureBlobStorage",
    "AzureSqlDatabase",
    "AzureDatabricks",
    "AmazonS3",
    "GoogleCloudStorage",
    "AzurePostgreSql",
    "AzureMySql",
    "Oracle",
}


def profile_factory(client: FactoryClient) -> FactoryProfile:
    """Profile an Azure Data Factory resource.

    Args:
        client: Authenticated ``FactoryClient`` for the target factory.

    Returns:
        A ``FactoryProfile`` summarising the factory contents.
    """
    pipelines = client.list_pipelines_full()
    datasets = client.list_datasets()
    linked_services = client.list_linked_services()
    triggers = client.list_triggers()
    integration_runtimes = client.list_integration_runtimes()

    # Count activities (walk nested structures)
    all_activities: list[dict] = []
    for pipeline in pipelines:
        _collect_activities(pipeline.get("activities", []), all_activities)

    supported_activities = [a for a in all_activities if a.get("type") in SUPPORTED_ACTIVITY_TYPES]
    unsupported_activity_types = sorted(
        {a.get("type", "Unknown") for a in all_activities if a.get("type") not in SUPPORTED_ACTIVITY_TYPES}
    )

    # Count datasets
    supported_datasets: list[dict] = []
    unsupported_datasets: list[dict] = []
    dataset_details: list[DatasetDetail] = []
    for dset in datasets:
        props = dset.get("properties", {})
        ds_type = props.get("type", "Unknown")
        ls_ref = props.get("linked_service_name", {})
        ls_name = ls_ref.get("reference_name") if isinstance(ls_ref, dict) else None
        ls_type = _resolve_linked_service_type(ls_name, linked_services) if ls_name else None

        detail = DatasetDetail(
            dataset_name=dset.get("name", "Unknown"),
            dataset_type=ds_type,
            linked_service_name=ls_name,
            linked_service_type=ls_type,
        )
        dataset_details.append(detail)

        if ds_type in SUPPORTED_DATASET_TYPES:
            supported_datasets.append(dset)
        else:
            unsupported_datasets.append(dset)

    unsupported_dataset_types = sorted(
        {ds.get("properties", {}).get("type", "Unknown") for ds in unsupported_datasets}
    )

    # Count linked services
    supported_ls = [
        ls for ls in linked_services if ls.get("properties", {}).get("type") in SUPPORTED_LINKED_SERVICE_TYPES
    ]

    # Count integration runtimes
    ir_details: list[IntegrationRuntimeDetail] = []
    for irt in integration_runtimes:
        props = irt.get("properties", {})
        node_count = None
        if props.get("type") == "SelfHosted":
            node_count = (
                props.get("type_properties", {}).get("compute_properties", {}).get("number_of_nodes")
            )
        ir_details.append(
            IntegrationRuntimeDetail(
                name=irt.get("name", "Unknown"),
                runtime_type=props.get("type", "Unknown"),
                node_count=node_count,
            )
        )

    return FactoryProfile(
        factory_name=client.factory_name,
        pipelines=ObjectCount(len(pipelines), len(pipelines), 0),
        activities=ObjectCount(
            len(all_activities), len(supported_activities), len(all_activities) - len(supported_activities)
        ),
        linked_services=ObjectCount(len(linked_services), len(supported_ls), len(linked_services) - len(supported_ls)),
        datasets=ObjectCount(len(datasets), len(supported_datasets), len(unsupported_datasets)),
        triggers=ObjectCount(len(triggers), len(triggers), 0),
        integration_runtimes=ObjectCount(len(integration_runtimes), len(integration_runtimes), 0),
        dataset_details=dataset_details,
        integration_runtime_details=ir_details,
        unsupported_activity_types=unsupported_activity_types,
        unsupported_dataset_types=unsupported_dataset_types,
    )


def _collect_activities(activities: list[dict], result: list[dict]) -> None:
    """Recursively collect all activities including nested ones.

    Args:
        activities: List of activity dicts to process.
        result: Accumulator list where discovered activities are appended.
    """
    for activity in activities:
        result.append(activity)
        # ForEach inner activities
        inner = activity.get("activities", [])
        if inner:
            _collect_activities(inner, result)
        # IfCondition branches
        for branch_key in ("if_true_activities", "if_false_activities"):
            branch = activity.get(branch_key, [])
            if branch:
                _collect_activities(branch, result)


def _resolve_linked_service_type(name: str, linked_services: list[dict]) -> str | None:
    """Find the type of a linked service by name.

    Args:
        name: Reference name of the linked service.
        linked_services: Full list of linked service dicts.

    Returns:
        The linked service type string, or ``None`` if not found.
    """
    for ls_item in linked_services:
        if ls_item.get("name") == name:
            return ls_item.get("properties", {}).get("type")
    return None


def format_profile(profile: FactoryProfile) -> str:
    """Format a FactoryProfile as human-readable text.

    Args:
        profile: The factory profile to format.

    Returns:
        Formatted multi-line string.
    """
    lines = [
        f"Azure Data Factory Profile: {profile.factory_name}",
        "=" * 60,
        "",
        "Object Counts:",
        f"  Pipelines:            {profile.pipelines.total}",
        f"  Activities:           {profile.activities.total}"
        f" ({profile.activities.supported} supported, {profile.activities.unsupported} unsupported)",
        f"  Linked Services:      {profile.linked_services.total}"
        f" ({profile.linked_services.supported} supported, {profile.linked_services.unsupported} unsupported)",
        f"  Datasets:             {profile.datasets.total}"
        f" ({profile.datasets.supported} supported, {profile.datasets.unsupported} unsupported)",
        f"  Triggers:             {profile.triggers.total}",
        f"  Integration Runtimes: {profile.integration_runtimes.total}",
    ]

    if profile.unsupported_activity_types:
        lines += ["", "Unsupported Activity Types:"]
        for activity_type in profile.unsupported_activity_types:
            lines.append(f"  - {activity_type}")

    if profile.unsupported_dataset_types:
        lines += ["", "Unsupported Dataset Types:"]
        for dataset_type in profile.unsupported_dataset_types:
            lines.append(f"  - {dataset_type}")

    if profile.dataset_details:
        lines += ["", "Dataset Details:"]
        for detail in profile.dataset_details:
            ls_name = detail.linked_service_name or "N/A"
            ls_type = detail.linked_service_type or "N/A"
            lines.append(f"  - {detail.dataset_name} ({detail.dataset_type}) via {ls_name} [{ls_type}]")

    if profile.integration_runtime_details:
        lines += ["", "Integration Runtimes:"]
        for irt in profile.integration_runtime_details:
            node_info = f", {irt.node_count} nodes" if irt.node_count is not None else ""
            lines.append(f"  - {irt.name} ({irt.runtime_type}{node_info})")

    return "\n".join(lines)

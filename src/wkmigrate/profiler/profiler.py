"""Profile an Azure Data Factory resource to assess migration readiness."""

from __future__ import annotations

import logging

# Import translator dispatchers first so every @translates_activity /
# @translates_dataset decorator fires before we read the sets.
import wkmigrate.translators.activity_translators.activity_translator as _activity_reg  # noqa: F401
import wkmigrate.translators.dataset_translators.dataset_translator as _dataset_reg  # noqa: F401
from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.profiler.profile import (
    DatasetDetail,
    FactoryProfile,
    IntegrationRuntimeDetail,
    ObjectCount,
)
from wkmigrate.supported_types import (
    SUPPORTED_ACTIVITY_TYPES,
    SUPPORTED_DATASET_TYPES,
    SUPPORTED_LINKED_SERVICE_TYPES,
)

logger = logging.getLogger(__name__)


def profile_factory(client: FactoryClient) -> FactoryProfile:
    """Profile an Azure Data Factory resource.

    Args:
        client: Authenticated ``FactoryClient`` for the target factory.

    Returns:
        A ``FactoryProfile`` summarising the factory contents.
    """
    pipelines = client.list_pipeline_definitions()
    datasets = client.list_datasets()
    linked_services = client.list_linked_services()
    triggers = client.list_triggers()
    integration_runtimes = client.list_integration_runtimes()

    activity_counts, unsupported_activity_types = _count_activities(pipelines)
    dataset_counts, dataset_details, unsupported_dataset_types = _count_datasets(datasets, linked_services)
    ls_counts = _count_linked_services(linked_services)
    ir_counts, ir_details = _build_integration_runtime_details(integration_runtimes)

    return FactoryProfile(
        factory_name=client.factory_name,
        pipelines=ObjectCount(len(pipelines), len(pipelines), 0),
        activities=activity_counts,
        linked_services=ls_counts,
        datasets=dataset_counts,
        triggers=ObjectCount(len(triggers), len(triggers), 0),
        integration_runtimes=ir_counts,
        dataset_details=dataset_details,
        integration_runtime_details=ir_details,
        unsupported_activity_types=unsupported_activity_types,
        unsupported_dataset_types=unsupported_dataset_types,
    )


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


def _count_activities(pipelines: list) -> tuple[ObjectCount, list[str]]:
    """Walk nested activities, classify supported/unsupported.

    Returns:
        A tuple of ``(ObjectCount, unsupported_type_names)``.
    """
    all_activities: list[dict] = []
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            logger.warning("Skipping non-dictionary pipeline")
            continue
        _collect_activities(pipeline.get("activities") or [], all_activities)

    supported = 0
    unsupported_types: set[str] = set()
    for activity in all_activities:
        activity_type = activity.get("type") or "Unknown"
        if activity_type in SUPPORTED_ACTIVITY_TYPES:
            supported += 1
        else:
            unsupported_types.add(activity_type)

    total = len(all_activities)
    return ObjectCount(total, supported, total - supported), sorted(unsupported_types)


def _count_datasets(
    datasets: list[dict],
    linked_services: list[dict],
) -> tuple[ObjectCount, list[DatasetDetail], list[str]]:
    """Classify datasets, build details.

    Returns:
        A tuple of ``(ObjectCount, dataset_details, unsupported_type_names)``.
    """
    ls_type_map = _build_linked_service_type_map(linked_services)

    supported = 0
    unsupported = 0
    unsupported_types: set[str] = set()
    details: list[DatasetDetail] = []

    for dset in datasets:
        props = dset.get("properties", {})
        ds_type = props.get("type") or "Unknown"
        ls_ref = props.get("linked_service_name", {})
        ls_name = ls_ref.get("reference_name") if isinstance(ls_ref, dict) else None
        ls_type = ls_type_map.get(ls_name) if ls_name else None

        details.append(
            DatasetDetail(
                dataset_name=dset.get("name", "Unknown"),
                dataset_type=ds_type,
                linked_service_name=ls_name,
                linked_service_type=ls_type,
            )
        )

        if ds_type in SUPPORTED_DATASET_TYPES:
            supported += 1
        else:
            unsupported += 1
            unsupported_types.add(ds_type)

    total = supported + unsupported
    return ObjectCount(total, supported, unsupported), details, sorted(unsupported_types)


def _count_linked_services(linked_services: list[dict]) -> ObjectCount:
    """Count supported vs unsupported linked services."""
    supported = sum(
        1 for ls in linked_services if ls.get("properties", {}).get("type") in SUPPORTED_LINKED_SERVICE_TYPES
    )
    total = len(linked_services)
    return ObjectCount(total, supported, total - supported)


def _build_integration_runtime_details(
    integration_runtimes: list[dict],
) -> tuple[ObjectCount, list[IntegrationRuntimeDetail]]:
    """Build integration runtime details and counts.

    Returns:
        A tuple of ``(ObjectCount, ir_details)``.
    """
    details: list[IntegrationRuntimeDetail] = []
    for irt in integration_runtimes:
        props = irt.get("properties", {})
        node_count = None
        if props.get("type") == "SelfHosted":
            node_count = props.get("type_properties", {}).get("compute_properties", {}).get("number_of_nodes")
        details.append(
            IntegrationRuntimeDetail(
                name=irt.get("name", "Unknown"),
                runtime_type=props.get("type", "Unknown"),
                node_count=node_count,
            )
        )
    total = len(integration_runtimes)
    return ObjectCount(total, total, 0), details


def _collect_activities(activities: list[dict] | None, result: list[dict]) -> None:
    """Recursively collect all activities including nested ones.

    Args:
        activities: List of activity dicts to process, or ``None``.
        result: Accumulator list where discovered activities are appended.
    """
    for activity in activities or []:
        result.append(activity)
        # ForEach inner activities
        inner = activity.get("activities") or []
        if inner:
            _collect_activities(inner, result)
        # IfCondition branches
        for branch_key in ("if_true_activities", "if_false_activities"):
            branch = activity.get(branch_key) or []
            if branch:
                _collect_activities(branch, result)


def _build_linked_service_type_map(linked_services: list[dict]) -> dict[str, str | None]:
    """Build a name -> type lookup for linked services.

    Args:
        linked_services: Full list of linked service dicts.

    Returns:
        Mapping from linked service name to its type string.
    """
    return {ls.get("name", ""): ls.get("properties", {}).get("type") for ls in linked_services}

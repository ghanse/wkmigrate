"""Profile an Azure Data Factory resource to assess migration readiness."""

from __future__ import annotations

import logging
import re
from typing import Any

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
    PipelineDetail,
)
from wkmigrate.supported_types import (
    SUPPORTED_ACTIVITY_TYPES,
    SUPPORTED_DATASET_TYPES,
    SUPPORTED_LINKED_SERVICE_TYPES,
)

logger = logging.getLogger(__name__)

# ARM resource types used in factory export JSON.  Matching is case-insensitive
# because Azure portal exports the segment names with mixed casing
# (``linkedServices`` vs ``linkedservices``, ``integrationRuntimes`` vs
# ``integrationruntimes``).
_ARM_PIPELINE_TYPE = "microsoft.datafactory/factories/pipelines"
_ARM_DATASET_TYPE = "microsoft.datafactory/factories/datasets"
_ARM_LINKED_SERVICE_TYPE = "microsoft.datafactory/factories/linkedservices"
_ARM_TRIGGER_TYPE = "microsoft.datafactory/factories/triggers"
_ARM_INTEGRATION_RUNTIME_TYPE = "microsoft.datafactory/factories/integrationruntimes"
_ARM_FACTORY_TYPE = "microsoft.datafactory/factories"


def profile_factory(
    client: FactoryClient | None = None,
    *,
    arm_template: dict[str, Any] | None = None,
    factory_name: str | None = None,
) -> FactoryProfile:
    """Profile an Azure Data Factory resource.

    Pass either an authenticated ``FactoryClient`` for live profiling against a
    deployed Data Factory, or an ``arm_template`` dict for offline profiling
    against an exported ARM JSON template (e.g. ``ARMTemplateForFactory.json``).
    Exactly one of the two must be provided.

    Args:
        client: Authenticated ``FactoryClient`` for the target factory.  When
            omitted, ``arm_template`` must be supplied instead.
        arm_template: Parsed Azure Resource Manager template for an ADF
            factory.  The ``resources`` array is filtered by the standard
            ``Microsoft.DataFactory/factories/*`` types and field names are
            normalised from ARM camelCase to the SDK's snake_case shape so the
            downstream helpers work without branching on input source.
        factory_name: Override for the factory display name.  Required when
            profiling an ARM template that does not embed a
            ``Microsoft.DataFactory/factories`` resource.  Ignored when
            ``client`` is supplied.

    Returns:
        A ``FactoryProfile`` summarising the factory contents, including a
        per-pipeline breakdown in ``pipeline_details``.

    Raises:
        ValueError: If neither ``client`` nor ``arm_template`` was supplied,
            or if both were supplied.
    """
    if client is not None and arm_template is not None:
        raise ValueError("Pass either client= or arm_template=, not both.")
    if client is None and arm_template is None:
        raise ValueError("One of client= or arm_template= must be provided.")

    if client is not None:
        resolved_factory_name = client.factory_name
        pipelines = client.list_pipeline_definitions()
        datasets = client.list_datasets()
        linked_services = client.list_linked_services()
        triggers = client.list_triggers()
        integration_runtimes = client.list_integration_runtimes()
    else:
        # arm_template is not None per the guard above; mypy/typecheckers
        # benefit from the local rebind.
        loaded = _load_from_arm(arm_template, factory_name_override=factory_name)
        resolved_factory_name = loaded["factory_name"]
        pipelines = loaded["pipelines"]
        datasets = loaded["datasets"]
        linked_services = loaded["linked_services"]
        triggers = loaded["triggers"]
        integration_runtimes = loaded["integration_runtimes"]

    activity_counts, unsupported_activity_types = _count_activities(pipelines)
    dataset_counts, dataset_details, unsupported_dataset_types = _count_datasets(datasets, linked_services)
    ls_counts = _count_linked_services(linked_services)
    ir_counts, ir_details = _build_integration_runtime_details(integration_runtimes)
    pipeline_details = _build_pipeline_details(
        pipelines=pipelines,
        datasets=datasets,
        linked_services=linked_services,
        triggers=triggers,
        integration_runtimes=integration_runtimes,
    )

    return FactoryProfile(
        factory_name=resolved_factory_name,
        pipelines=ObjectCount(len(pipelines), len(pipelines), 0),
        activities=activity_counts,
        linked_services=ls_counts,
        datasets=dataset_counts,
        triggers=ObjectCount(len(triggers), len(triggers), 0),
        integration_runtimes=ir_counts,
        dataset_details=dataset_details,
        integration_runtime_details=ir_details,
        pipeline_details=pipeline_details,
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

    if profile.pipeline_details:
        lines += ["", "Pipeline Details:"]
        for pd in profile.pipeline_details:
            lines.append(f"  - {pd.pipeline_name}")
            lines.append(
                f"      Activities:      {pd.activities.total}"
                f" ({pd.activities.supported} supported, {pd.activities.unsupported} unsupported)"
            )
            lines.append(
                f"      Datasets:        {pd.datasets.total}"
                f" ({pd.datasets.supported} supported, {pd.datasets.unsupported} unsupported)"
            )
            lines.append(
                f"      Linked Services: {pd.linked_services.total}"
                f" ({pd.linked_services.supported} supported, {pd.linked_services.unsupported} unsupported)"
            )
            lines.append(
                f"      Triggers: {pd.total_triggers}, Integration Runtimes: {pd.total_integration_runtimes}"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ARM template loader
# ---------------------------------------------------------------------------


def _load_from_arm(
    arm_template: dict[str, Any], *, factory_name_override: str | None = None
) -> dict[str, Any]:
    """Extracts ADF resources from an ARM template and normalises their shape.

    The Azure Portal export uses ARM camelCase field names and nests pipeline
    activities under ``properties.activities``.  The SDK ``as_dict()`` output
    (and therefore the existing counter helpers) expects snake_case fields and
    pipeline activities lifted to the top level.  This loader translates ARM
    JSON into that internal shape so the rest of the profiler stays oblivious
    to the input source.

    Args:
        arm_template: Parsed ARM template dict.  The ``resources`` array is
            scanned; everything outside the Data Factory resource types is
            ignored.
        factory_name_override: Optional factory display name.  Used when the
            ARM template does not include a top-level
            ``Microsoft.DataFactory/factories`` resource.

    Returns:
        Dict with keys ``factory_name``, ``pipelines``, ``datasets``,
        ``linked_services``, ``triggers``, and ``integration_runtimes``.
    """
    resources = arm_template.get("resources") or []
    pipelines: list[dict] = []
    datasets: list[dict] = []
    linked_services: list[dict] = []
    triggers: list[dict] = []
    integration_runtimes: list[dict] = []
    factory_name: str | None = factory_name_override

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        resource_type = (resource.get("type") or "").lower()
        leaf_name = _leaf_resource_name(resource.get("name") or "")
        properties = _arm_to_snake(resource.get("properties") or {})

        if resource_type == _ARM_FACTORY_TYPE:
            if factory_name is None:
                factory_name = leaf_name
        elif resource_type == _ARM_PIPELINE_TYPE:
            # Lift activities to top-level to match SDK shape; preserve
            # the snake-cased ``properties`` for any downstream readers.
            pipelines.append(
                {
                    "name": leaf_name,
                    "activities": properties.get("activities") or [],
                    "parameters": properties.get("parameters") or {},
                    "properties": properties,
                }
            )
        elif resource_type == _ARM_DATASET_TYPE:
            datasets.append({"name": leaf_name, "properties": properties})
        elif resource_type == _ARM_LINKED_SERVICE_TYPE:
            linked_services.append({"name": leaf_name, "properties": properties})
        elif resource_type == _ARM_TRIGGER_TYPE:
            triggers.append({"name": leaf_name, "properties": properties})
        elif resource_type == _ARM_INTEGRATION_RUNTIME_TYPE:
            integration_runtimes.append({"name": leaf_name, "properties": properties})

    if factory_name is None:
        factory_name = "Unknown"

    return {
        "factory_name": factory_name,
        "pipelines": pipelines,
        "datasets": datasets,
        "linked_services": linked_services,
        "triggers": triggers,
        "integration_runtimes": integration_runtimes,
    }


def _leaf_resource_name(arm_name: str) -> str:
    """Strips the ``factory-name/`` prefix from an ARM resource name.

    ARM names for nested resources look like ``factory-name/pipeline-name``.
    This helper returns just the leaf segment so the resource name matches
    what the SDK returns.

    Args:
        arm_name: The raw ARM resource ``name`` field.

    Returns:
        The final ``/``-separated segment, or the input unchanged when there
        is no slash.
    """
    if "/" not in arm_name:
        return arm_name
    return arm_name.split("/")[-1]


_CAMEL_TO_SNAKE_RE = re.compile(r"(?<!^)(?=[A-Z])")


def _arm_to_snake(value: Any) -> Any:
    """Recursively converts camelCase dict keys to snake_case.

    Walks dicts and lists; leaves leaf values (strings, numbers, bools,
    ``None``) untouched.

    Args:
        value: Any JSON-decoded value from an ARM template.

    Returns:
        A new structure with the same shape and snake_cased keys.
    """
    if isinstance(value, dict):
        return {_camel_to_snake(k): _arm_to_snake(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_arm_to_snake(item) for item in value]
    return value


def _camel_to_snake(name: str) -> str:
    """Converts a single camelCase identifier to snake_case.

    Args:
        name: Identifier to convert.

    Returns:
        snake_cased identifier; pass-through for already-snake names.
    """
    return _CAMEL_TO_SNAKE_RE.sub("_", name).lower()


# ---------------------------------------------------------------------------
# Per-pipeline detail computation
# ---------------------------------------------------------------------------


def _build_pipeline_details(
    *,
    pipelines: list[dict],
    datasets: list[dict],
    linked_services: list[dict],
    triggers: list[dict],
    integration_runtimes: list[dict],  # noqa: ARG001 -- accepted for caller symmetry
) -> list[PipelineDetail]:
    """Computes the per-pipeline breakdown used by ``FactoryProfile``.

    For each pipeline, walks its activities (including nested ones inside
    ``ForEach`` and ``IfCondition``) to collect activity types and any
    ``reference_name`` strings that resolve to a known dataset or linked
    service.  Linked-service references are augmented with the linked services
    transitively reached via the pipeline's datasets so a dataset that points
    at ``AzureSqlDatabase_LS`` counts toward the pipeline's linked-service
    total even when the activity itself only names the dataset.

    The integration-runtime count is derived from the ``connect_via``
    reference on each linked service the pipeline reaches.

    Args:
        pipelines: Pipeline definitions in SDK (snake_case) shape.
        datasets: Dataset definitions in SDK shape.
        linked_services: Linked-service definitions in SDK shape.
        triggers: Trigger definitions in SDK shape.
        integration_runtimes: Integration-runtime definitions in SDK shape.
            Accepted for API symmetry; the per-pipeline IR count is derived
            from linked-service ``connect_via`` references.

    Returns:
        One ``PipelineDetail`` per pipeline, in input order.
    """
    dataset_by_name: dict[str, dict] = {ds.get("name", ""): ds for ds in datasets if isinstance(ds, dict)}
    ls_by_name: dict[str, dict] = {ls.get("name", ""): ls for ls in linked_services if isinstance(ls, dict)}

    details: list[PipelineDetail] = []
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            logger.warning("Skipping non-dictionary pipeline in pipeline_details computation")
            continue
        pipeline_name = pipeline.get("name", "Unknown")

        # Walk every nested activity once
        activities: list[dict] = []
        _collect_activities(pipeline.get("activities") or [], activities)

        # Activity supported/unsupported counts within this pipeline
        activity_supported = sum(1 for a in activities if (a.get("type") or "Unknown") in SUPPORTED_ACTIVITY_TYPES)
        activity_total = len(activities)
        activity_counts = ObjectCount(
            total=activity_total,
            supported=activity_supported,
            unsupported=activity_total - activity_supported,
        )

        # Datasets referenced by THIS pipeline's activities
        referenced_dataset_names = _collect_referenced_dataset_names(activities, dataset_by_name)
        ds_supported = 0
        ds_unsupported = 0
        for ds_name in referenced_dataset_names:
            ds = dataset_by_name.get(ds_name)
            if ds is None:
                ds_unsupported += 1
                continue
            ds_type = (ds.get("properties") or {}).get("type") or "Unknown"
            if ds_type in SUPPORTED_DATASET_TYPES:
                ds_supported += 1
            else:
                ds_unsupported += 1
        dataset_counts = ObjectCount(
            total=len(referenced_dataset_names),
            supported=ds_supported,
            unsupported=ds_unsupported,
        )

        # Linked services: those referenced directly by activities + those
        # reached transitively via referenced datasets
        ls_names: set[str] = set()
        ls_names.update(_collect_referenced_linked_service_names(activities, ls_by_name))
        for ds_name in referenced_dataset_names:
            ds = dataset_by_name.get(ds_name)
            if ds is None:
                continue
            ls_ref = (ds.get("properties") or {}).get("linked_service_name") or {}
            ls_ref_name = ls_ref.get("reference_name") if isinstance(ls_ref, dict) else None
            if ls_ref_name:
                ls_names.add(ls_ref_name)

        ls_supported = 0
        ls_unsupported = 0
        for ls_name in sorted(ls_names):
            ls = ls_by_name.get(ls_name)
            if ls is None:
                ls_unsupported += 1
                continue
            ls_type = (ls.get("properties") or {}).get("type") or "Unknown"
            if ls_type in SUPPORTED_LINKED_SERVICE_TYPES:
                ls_supported += 1
            else:
                ls_unsupported += 1
        ls_counts = ObjectCount(
            total=len(ls_names),
            supported=ls_supported,
            unsupported=ls_unsupported,
        )

        # Triggers binding this pipeline
        trigger_count = _count_triggers_for_pipeline(pipeline_name, triggers)

        # Distinct integration runtimes reached via this pipeline's linked services
        ir_names: set[str] = set()
        for ls_name in ls_names:
            ls = ls_by_name.get(ls_name)
            if ls is None:
                continue
            connect_via = (ls.get("properties") or {}).get("connect_via") or {}
            ir_ref_name = connect_via.get("reference_name") if isinstance(connect_via, dict) else None
            if ir_ref_name:
                ir_names.add(ir_ref_name)

        details.append(
            PipelineDetail(
                pipeline_name=pipeline_name,
                activities=activity_counts,
                datasets=dataset_counts,
                linked_services=ls_counts,
                total_activities=activity_counts.total,
                total_datasets=dataset_counts.total,
                total_linked_services=ls_counts.total,
                total_triggers=trigger_count,
                total_integration_runtimes=len(ir_names),
            )
        )

    return details


def _collect_referenced_dataset_names(
    activities: list[dict], dataset_by_name: dict[str, dict]
) -> set[str]:
    """Walks the activity tree collecting every dataset reference.

    Dataset references appear as ``{"reference_name": ..., "type":
    "DatasetReference"}`` blocks across ``inputs``, ``outputs``,
    ``type_properties.dataset``, ``type_properties.source.dataset``,
    ``type_properties.sink.dataset``, and a handful of other locations.
    Instead of enumerating every spot, walk every nested dict in each activity
    and pick out ``reference_name`` strings whose name matches a known
    dataset.

    Args:
        activities: Flat list of activity dicts (control-flow already expanded).
        dataset_by_name: Index used to filter ``reference_name`` candidates so
            we don't conflate linked-service or trigger references.

    Returns:
        Unique dataset names referenced anywhere in the activity tree.
    """
    found: set[str] = set()
    for activity in activities:
        for ref_name, ref_type in _iter_reference_pairs(activity):
            if ref_type == "DatasetReference" and ref_name in dataset_by_name:
                found.add(ref_name)
            elif ref_type is None and ref_name in dataset_by_name:
                # Some activity shapes drop the ``type`` field on references.
                # Fall back to a name-match.
                found.add(ref_name)
    return found


def _collect_referenced_linked_service_names(
    activities: list[dict], ls_by_name: dict[str, dict]
) -> set[str]:
    """Walks the activity tree collecting every linked-service reference.

    Some activities (e.g. ``SqlServerStoredProcedure``, ``WebActivity`` auth)
    reference linked services directly without going through a dataset.

    Args:
        activities: Flat list of activity dicts.
        ls_by_name: Index used to filter ``reference_name`` candidates.

    Returns:
        Unique linked-service names referenced directly by activities.
    """
    found: set[str] = set()
    for activity in activities:
        for ref_name, ref_type in _iter_reference_pairs(activity):
            if ref_type == "LinkedServiceReference" and ref_name in ls_by_name:
                found.add(ref_name)
    return found


def _iter_reference_pairs(value: Any):
    """Yields every ``(reference_name, type)`` pair found recursively in *value*.

    ADF nests references as ``{"reference_name": "...", "type": "..."}`` (SDK
    shape).  This helper walks dicts and lists yielding those pairs so the
    caller can filter by reference type without enumerating every nesting site.

    Args:
        value: Any dict / list / scalar.

    Yields:
        Pairs of ``(reference_name, type_or_None)`` discovered in *value*.
    """
    if isinstance(value, dict):
        ref_name = value.get("reference_name")
        ref_type = value.get("type")
        if isinstance(ref_name, str):
            yield ref_name, ref_type if isinstance(ref_type, str) else None
        for nested in value.values():
            yield from _iter_reference_pairs(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_reference_pairs(item)


def _count_triggers_for_pipeline(pipeline_name: str, triggers: list[dict]) -> int:
    """Counts triggers that bind to a given pipeline.

    A trigger's ``properties.pipelines`` array carries ``pipeline_reference``
    blocks; we match against the ``reference_name`` field.

    Args:
        pipeline_name: Logical pipeline name to match.
        triggers: All triggers in the factory.

    Returns:
        Number of triggers that include *pipeline_name* in their pipeline
        reference list.
    """
    count = 0
    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue
        bound_pipelines = (trigger.get("properties") or {}).get("pipelines") or []
        for bound in bound_pipelines:
            if not isinstance(bound, dict):
                continue
            ref = bound.get("pipeline_reference") or {}
            if isinstance(ref, dict) and ref.get("reference_name") == pipeline_name:
                count += 1
                break
    return count


# ---------------------------------------------------------------------------
# Factory-wide counters (unchanged behaviour)
# ---------------------------------------------------------------------------


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

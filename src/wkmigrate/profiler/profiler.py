"""Profile an Azure Data Factory resource to assess migration readiness."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import wkmigrate.translators.activity_translators.activity_translator as _activity_translator_registry  # noqa: F401
import wkmigrate.translators.dataset_translators.dataset_translator as _dataset_translator_registry  # noqa: F401
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
    git_export_root: str | Path | None = None,
    factory_name: str | None = None,
) -> FactoryProfile:
    """Profiles an Azure Data Factory resource.

    Accepts one of three input sources (mutually exclusive — exactly one must be
    provided):

    * ``client``: live profiling against a deployed Data Factory via the
      authenticated ``FactoryClient``.
    * ``arm_template``: offline profiling against a parsed ARM template dict
      (the shape ``ARMTemplateForFactory.json`` deserializes to, or the output
      of an in-memory composition such as ``to_arm_template()``).
    * ``git_export_root``: offline profiling against an ADF Git-mode export on
      disk — the directory layout written by ADF Studio's Git integration,
      with one JSON file per resource under per-kind subdirectories
      (``pipeline/``, ``dataset/``, ``linkedService/``, ``trigger/``,
      ``integrationRuntime/`` and optionally ``factory/``).

    Args:
        client: Authenticated FactoryClient for the target factory.
        arm_template: Parsed ARM JSON template.
        git_export_root: Filesystem path to the root of an ADF Git-mode export.
        factory_name: Override for the factory display name. Used when
            profiling an ARM template that does not embed a
            ``Microsoft.DataFactory/factories`` resource, or a Git-mode export
            that does not include a ``factory/`` subdirectory. Ignored when a
            client is supplied.

    Returns:
        A ``FactoryProfile`` summarising the factory contents.

    Raises:
        ValueError: If zero or more than one of ``client``, ``arm_template``,
            or ``git_export_root`` was supplied.
    """
    provided = [name for name, value in (
        ("client", client),
        ("arm_template", arm_template),
        ("git_export_root", git_export_root),
    ) if value is not None]
    if len(provided) > 1:
        raise ValueError(f"Only one of 'client', 'arm_template', or 'git_export_root' may be provided; got {provided}")
    if not provided:
        raise ValueError("One of 'client', 'arm_template', or 'git_export_root' must be provided")

    if client is not None:
        resolved_factory_name = client.factory_name
        pipelines = client.list_pipeline_definitions()
        datasets = client.list_datasets()
        linked_services = client.list_linked_services()
        triggers = client.list_triggers()
        integration_runtimes = client.list_integration_runtimes()
    else:
        if arm_template is not None:
            loaded = _load_from_arm(arm_template, factory_name_override=factory_name)
        else:
            # git_export_root is not None per the guards above
            loaded = _load_from_git_export(Path(git_export_root), factory_name_override=factory_name)
        resolved_factory_name = loaded["factory_name"]
        pipelines = loaded["pipelines"]
        datasets = loaded["datasets"]
        linked_services = loaded["linked_services"]
        triggers = loaded["triggers"]
        integration_runtimes = loaded["integration_runtimes"]

    activity_counts, unsupported_activity_types = _count_activities(pipelines)
    dataset_counts, dataset_details, unsupported_dataset_types = _count_datasets(datasets, linked_services)
    linked_service_counts = _count_linked_services(linked_services)
    integration_runtime_counts, integration_runtime_details = _build_integration_runtime_details(integration_runtimes)
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
        linked_services=linked_service_counts,
        datasets=dataset_counts,
        triggers=ObjectCount(len(triggers), len(triggers), 0),
        integration_runtimes=integration_runtime_counts,
        dataset_details=dataset_details,
        integration_runtime_details=integration_runtime_details,
        pipeline_details=pipeline_details,
        unsupported_activity_types=unsupported_activity_types,
        unsupported_dataset_types=unsupported_dataset_types,
    )


def format_profile(profile: FactoryProfile) -> str:
    """Formats a FactoryProfile as human-readable text.

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
            linked_service_name = detail.linked_service_name or "N/A"
            linked_service_type = detail.linked_service_type or "N/A"
            lines.append(
                f"  - {detail.dataset_name} ({detail.dataset_type}) "
                f"via {linked_service_name} [{linked_service_type}]"
            )

    if profile.integration_runtime_details:
        lines += ["", "Integration Runtimes:"]
        for integration_runtime in profile.integration_runtime_details:
            node_info = (
                f", {integration_runtime.node_count} nodes" if integration_runtime.node_count is not None else ""
            )
            lines.append(f"  - {integration_runtime.name} ({integration_runtime.runtime_type}{node_info})")

    if profile.pipeline_details:
        lines += ["", "Pipeline Details:"]
        for pipeline_detail in profile.pipeline_details:
            lines.append(f"  - {pipeline_detail.pipeline_name}")
            lines.append(
                f"      Activities:      {pipeline_detail.activities.total}"
                f" ({pipeline_detail.activities.supported} supported, {pipeline_detail.activities.unsupported} unsupported)"
            )
            lines.append(
                f"      Datasets:        {pipeline_detail.datasets.total}"
                f" ({pipeline_detail.datasets.supported} supported, {pipeline_detail.datasets.unsupported} unsupported)"
            )
            lines.append(
                f"      Linked Services: {pipeline_detail.linked_services.total}"
                f" ({pipeline_detail.linked_services.supported} supported, {pipeline_detail.linked_services.unsupported} unsupported)"
            )
            lines.append(
                f"      Triggers: {pipeline_detail.total_triggers}, Integration Runtimes: {pipeline_detail.total_integration_runtimes}"
            )

    return "\n".join(lines)


def _load_from_arm(arm_template: dict[str, Any], *, factory_name_override: str | None = None) -> dict[str, Any]:
    """Extracts and normalizes ADF resources from an ARM template.

    Args:
        arm_template: Parsed ARM template dict.
        factory_name_override: Optional factory display name. Used when the
            ARM template does not include a top-level 'Microsoft.DataFactory/factories' resource.

    Returns:
        Dict with keys 'factory_name', 'pipelines', 'datasets', 'linked_services', 'triggers',
        and 'integration_runtimes'.
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
        if resource_type == _ARM_PIPELINE_TYPE:
            pipelines.append(
                {
                    "name": leaf_name,
                    "activities": properties.get("activities") or [],
                    "parameters": properties.get("parameters") or {},
                    "properties": properties,
                }
            )
        if resource_type == _ARM_DATASET_TYPE:
            datasets.append({"name": leaf_name, "properties": properties})
        if resource_type == _ARM_LINKED_SERVICE_TYPE:
            linked_services.append({"name": leaf_name, "properties": properties})
        if resource_type == _ARM_TRIGGER_TYPE:
            triggers.append({"name": leaf_name, "properties": properties})
        if resource_type == _ARM_INTEGRATION_RUNTIME_TYPE:
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


_GIT_EXPORT_KIND_TO_ARM_TYPE: dict[str, str] = {
    "pipeline": _ARM_PIPELINE_TYPE,
    "dataset": _ARM_DATASET_TYPE,
    "linkedService": _ARM_LINKED_SERVICE_TYPE,
    "trigger": _ARM_TRIGGER_TYPE,
    "integrationRuntime": _ARM_INTEGRATION_RUNTIME_TYPE,
}


def _load_from_git_export(root: Path, *, factory_name_override: str | None = None) -> dict[str, Any]:
    """Loads an ADF Git-mode export from disk and normalises it into ARM shape.

    ADF Studio's Git integration writes each resource as its own JSON file
    under a per-kind subdirectory of the configured root folder
    (``pipeline/<name>.json``, ``dataset/<name>.json``,
    ``linkedService/<name>.json``, ``trigger/<name>.json``,
    ``integrationRuntime/<name>.json``, with optional ``factory/<name>.json``
    metadata).  This loader walks those subdirectories, reads each file, wraps
    the contents in the ARM-resource envelope, and then delegates to
    :func:`_load_from_arm` so the snake_case key conversion and pipeline-shape
    normalisation stay in a single place.

    Args:
        root: Filesystem path to the root of the Git-mode export.
        factory_name_override: Optional factory display name.  Used when the
            export does not include a ``factory/`` subdirectory.

    Returns:
        Dict with keys ``factory_name``, ``pipelines``, ``datasets``,
        ``linked_services``, ``triggers``, and ``integration_runtimes`` -- the
        same shape :func:`_load_from_arm` returns.

    Raises:
        ValueError: If ``root`` is not an existing directory.

    Notes:
        - Subdirectories that don't exist are skipped silently — a factory
          with no triggers simply yields an empty ``triggers`` list.
        - Files that fail to parse as JSON emit a warning and are skipped so
          one bad file doesn't abort the whole profile.
        - Files without a top-level ``name`` field fall back to the file stem.
    """
    if not root.is_dir():
        raise ValueError(f"git_export_root {root!r} is not a directory")

    factory_name: str | None = factory_name_override
    factory_dir = root / "factory"
    if factory_name is None and factory_dir.is_dir():
        for factory_file in sorted(factory_dir.glob("*.json")):
            try:
                body = json.loads(factory_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Skipping malformed Git-mode factory file %s: %s", factory_file, exc)
                continue
            factory_name = body.get("name") or factory_file.stem
            break

    resources: list[dict] = []
    for kind, arm_type in _GIT_EXPORT_KIND_TO_ARM_TYPE.items():
        kind_dir = root / kind
        if not kind_dir.is_dir():
            continue
        for resource_file in sorted(kind_dir.glob("*.json")):
            try:
                body = json.loads(resource_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Skipping malformed Git-mode file %s: %s", resource_file, exc)
                continue
            resources.append(
                {
                    "type": arm_type,
                    "name": body.get("name") or resource_file.stem,
                    "properties": body.get("properties", {}),
                }
            )

    return _load_from_arm({"resources": resources}, factory_name_override=factory_name)


def _leaf_resource_name(arm_name: str) -> str:
    """Strips the 'factory_name/' prefix from an ARM resource name.

    Args:
        arm_name: ARM resource 'name' field.

    Returns:
        Reformatted leaf resource name.
    """
    if "/" not in arm_name:
        return arm_name
    return arm_name.split("/")[-1]


_CAMEL_TO_SNAKE_RE = re.compile(r"(?<!^)(?=[A-Z])")


def _arm_to_snake(value: Any) -> Any:
    """Converts camelCase property keys to snake_case.

    Args:
        value: Any JSON-decoded value from an ARM template.

    Returns:
        A new structure with the same shape and snake_cased keys.
    """
    if isinstance(value, dict):
        return {_camel_to_snake(key): _arm_to_snake(inner_value) for key, inner_value in value.items()}
    if isinstance(value, list):
        return [_arm_to_snake(item) for item in value]
    return value


def _camel_to_snake(name: str) -> str:
    """Converts a single camelCase identifier to snake_case.

    Args:
        name: Identifier to convert.

    Returns:
        snake_cased identifier.
    """
    return _CAMEL_TO_SNAKE_RE.sub("_", name).lower()


def _build_pipeline_details(
    *,
    pipelines: list[dict],
    datasets: list[dict],
    linked_services: list[dict],
    triggers: list[dict],
    integration_runtimes: list[dict],  # noqa: ARG001 -- accepted for caller symmetry
) -> list[PipelineDetail]:
    """Computes a per-pipeline breakdown of activities, datasets, linked services,
    triggers, and integration runtimes.

    All input objects should be normalized to snake case.

    Args:
        pipelines: Pipeline definitions.
        datasets: Dataset definitions.
        linked_services: Linked-service definitions.
        triggers: Trigger definitions.
        integration_runtimes: Integration-runtime definitions.  Accepted for
            caller symmetry; the per-pipeline IR count is derived from each
            linked service's ``connect_via`` reference rather than this list.

    Returns:
        A list of ``PipelineDetail`` objects for each pipeline.
    """
    del integration_runtimes  # see docstring — derived from linked-service connect_via
    dataset_by_name = {dataset.get("name", ""): dataset for dataset in datasets if isinstance(dataset, dict)}
    linked_service_by_name = {
        linked_service.get("name", ""): linked_service
        for linked_service in linked_services
        if isinstance(linked_service, dict)
    }

    details: list[PipelineDetail] = []
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            logger.warning("Skipping non-dictionary pipeline in pipeline_details computation")
            continue
        details.append(_build_pipeline_detail(pipeline, dataset_by_name, linked_service_by_name, triggers))
    return details


def _build_pipeline_detail(
    pipeline: dict,
    dataset_by_name: dict[str, dict],
    linked_service_by_name: dict[str, dict],
    triggers: list[dict],
) -> PipelineDetail:
    """Builds a single ``PipelineDetail`` by delegating each sub-computation to a helper.

    Args:
        pipeline: Pipeline definition in snake-case shape.
        dataset_by_name: Name → dataset index for the factory.
        linked_service_by_name: Name → linked-service index for the factory.
        triggers: All triggers in the factory.

    Returns:
        A populated ``PipelineDetail``.
    """
    pipeline_name = pipeline.get("name", "Unknown")
    activities = _collect_activities(pipeline.get("activities"))

    activity_counts = _classify_pipeline_activities(activities)
    referenced_dataset_names, dataset_counts = _classify_pipeline_datasets(activities, dataset_by_name)
    linked_service_names, linked_service_counts = _classify_pipeline_linked_services(
        activities=activities,
        referenced_dataset_names=referenced_dataset_names,
        dataset_by_name=dataset_by_name,
        linked_service_by_name=linked_service_by_name,
    )

    return PipelineDetail(
        pipeline_name=pipeline_name,
        activities=activity_counts,
        datasets=dataset_counts,
        linked_services=linked_service_counts,
        total_activities=activity_counts.total,
        total_datasets=dataset_counts.total,
        total_linked_services=linked_service_counts.total,
        total_triggers=_count_triggers_for_pipeline(pipeline_name, triggers),
        total_integration_runtimes=_count_integration_runtimes_for_pipeline(
            linked_service_names, linked_service_by_name
        ),
    )


def _classify_pipeline_activities(activities: list[dict]) -> ObjectCount:
    """Counts supported and unsupported activities within a single pipeline.

    Args:
        activities: Flat list of activity dicts (control-flow already expanded).

    Returns:
        ``ObjectCount`` with the total/supported/unsupported split.
    """
    supported = sum(1 for activity in activities if (activity.get("type") or "Unknown") in SUPPORTED_ACTIVITY_TYPES)
    total = len(activities)
    return ObjectCount(total=total, supported=supported, unsupported=total - supported)


def _classify_pipeline_datasets(
    activities: list[dict], dataset_by_name: dict[str, dict]
) -> tuple[set[str], ObjectCount]:
    """Collects every dataset name the activities reference, then classifies by supported type.

    Args:
        activities: Flat list of activity dicts.
        dataset_by_name: Name → dataset index used to filter ``reference_name``
            candidates and to look up dataset types.

    Returns:
        A tuple of ``(referenced_dataset_names, ObjectCount)``.  The set is
        returned so the caller can feed it into the linked-service classifier
        without re-walking the tree.
    """
    referenced = _collect_referenced_dataset_names(activities, dataset_by_name)
    supported, unsupported = _count_supported_dataset_refs(referenced, dataset_by_name)
    return referenced, ObjectCount(total=len(referenced), supported=supported, unsupported=unsupported)


def _classify_pipeline_linked_services(
    *,
    activities: list[dict],
    referenced_dataset_names: set[str],
    dataset_by_name: dict[str, dict],
    linked_service_by_name: dict[str, dict],
) -> tuple[set[str], ObjectCount]:
    """Resolves the linked services a pipeline reaches and classifies them.

    A pipeline reaches a linked service in two ways: directly (an activity
    such as ``WebActivity`` or ``SqlServerStoredProcedure`` carries a
    ``LinkedServiceReference``) or transitively (an activity references a
    dataset whose ``linked_service_name`` points at the linked service).
    Both paths are unioned before classification.

    Args:
        activities: Flat list of activity dicts.
        referenced_dataset_names: Datasets already known to be referenced by
            this pipeline; their ``linked_service_name`` fields contribute the
            transitive set of linked services reachable via datasets.
        dataset_by_name: Name → dataset index.
        linked_service_by_name: Name → linked-service index.

    Returns:
        ``(linked_service_names, ObjectCount)``.  The set is returned so the
        caller can pass it to :func:`_count_integration_runtimes_for_pipeline`.
    """
    linked_service_names = set(_collect_referenced_linked_service_names(activities, linked_service_by_name))
    linked_service_names |= _collect_linked_service_names_from_datasets(referenced_dataset_names, dataset_by_name)
    supported, unsupported = _count_supported_linked_service_refs(linked_service_names, linked_service_by_name)
    return linked_service_names, ObjectCount(
        total=len(linked_service_names), supported=supported, unsupported=unsupported
    )


def _collect_linked_service_names_from_datasets(
    referenced_dataset_names: set[str], dataset_by_name: dict[str, dict]
) -> set[str]:
    """Returns LS names reached transitively via each dataset's ``linked_service_name``.

    Args:
        referenced_dataset_names: Datasets to inspect.
        dataset_by_name: Name → dataset index.

    Returns:
        Unique LS names referenced by the given datasets.
    """
    found: set[str] = set()
    for dataset_name in referenced_dataset_names:
        dataset = dataset_by_name.get(dataset_name)
        if dataset is None:
            continue
        linked_service_reference = (dataset.get("properties") or {}).get("linked_service_name") or {}
        reference_name = (
            linked_service_reference.get("reference_name") if isinstance(linked_service_reference, dict) else None
        )
        if reference_name:
            found.add(reference_name)
    return found


def _count_supported_dataset_refs(
    referenced_dataset_names: set[str], dataset_by_name: dict[str, dict]
) -> tuple[int, int]:
    """Tallies ``(supported, unsupported)`` references by looking up each dataset's type.

    Args:
        referenced_dataset_names: Dataset names to classify.
        dataset_by_name: Name → dataset index.  Names not found are counted as
            unsupported (the reference is broken or refers to a resource
            outside this factory).

    Returns:
        ``(supported, unsupported)``.
    """
    supported = 0
    unsupported = 0
    for dataset_name in referenced_dataset_names:
        dataset = dataset_by_name.get(dataset_name)
        if dataset is None:
            unsupported += 1
            continue
        dataset_type = (dataset.get("properties") or {}).get("type") or "Unknown"
        if dataset_type in SUPPORTED_DATASET_TYPES:
            supported += 1
        else:
            unsupported += 1
    return supported, unsupported


def _count_supported_linked_service_refs(
    linked_service_names: set[str], linked_service_by_name: dict[str, dict]
) -> tuple[int, int]:
    """Tallies ``(supported, unsupported)`` references by looking up each linked service's type.

    Args:
        linked_service_names: Linked-service names to classify.
        linked_service_by_name: Name → linked-service index.  Unknown names
            count as unsupported.

    Returns:
        ``(supported, unsupported)``.
    """
    supported = 0
    unsupported = 0
    for linked_service_name in linked_service_names:
        linked_service = linked_service_by_name.get(linked_service_name)
        if linked_service is None:
            unsupported += 1
            continue
        linked_service_type = (linked_service.get("properties") or {}).get("type") or "Unknown"
        if linked_service_type in SUPPORTED_LINKED_SERVICE_TYPES:
            supported += 1
        else:
            unsupported += 1
    return supported, unsupported


def _count_integration_runtimes_for_pipeline(
    linked_service_names: set[str], linked_service_by_name: dict[str, dict]
) -> int:
    """Counts distinct integration runtimes reached via each linked service's ``connect_via``.

    Args:
        linked_service_names: Linked-service names the pipeline reaches.
        linked_service_by_name: Name → linked-service index.

    Returns:
        Count of unique integration-runtime names referenced.
    """
    integration_runtime_names: set[str] = set()
    for linked_service_name in linked_service_names:
        linked_service = linked_service_by_name.get(linked_service_name)
        if linked_service is None:
            continue
        connect_via = (linked_service.get("properties") or {}).get("connect_via") or {}
        reference_name = connect_via.get("reference_name") if isinstance(connect_via, dict) else None
        if reference_name:
            integration_runtime_names.add(reference_name)
    return len(integration_runtime_names)


def _collect_referenced_dataset_names(activities: list[dict], dataset_by_name: dict[str, dict]) -> set[str]:
    """Walks the activity tree and collects dataset references.

    Args:
        activities: Flat list of activity dicts.
        dataset_by_name: Index used to filter dataset candidates.

    Returns:
        Unique dataset names referenced anywhere in the activity tree.
    """
    found: set[str] = set()
    for activity in activities:
        for reference_name, reference_type in _iter_reference_pairs(activity):
            if reference_type == "DatasetReference" and reference_name in dataset_by_name:
                found.add(reference_name)
            elif reference_type is None and reference_name in dataset_by_name:
                # Some activity shapes drop the ``type`` field on references.
                # Fall back to a name-match.
                found.add(reference_name)
    return found


def _collect_referenced_linked_service_names(
    activities: list[dict], linked_service_by_name: dict[str, dict]
) -> set[str]:
    """Walks the activity tree to collect linked-service references.

    Args:
        activities: Flat list of activity dicts.
        linked_service_by_name: Index used to filter linked-service reference candidates.

    Returns:
        Unique linked-service names referenced directly by activities.
    """
    found: set[str] = set()
    for activity in activities:
        for reference_name, reference_type in _iter_reference_pairs(activity):
            if reference_type == "LinkedServiceReference" and reference_name in linked_service_by_name:
                found.add(reference_name)
    return found


def _iter_reference_pairs(value: Any):
    """Yields every ``(reference_name, type)`` pair found recursively in *value*.

    Args:
        value: Any dict / list / scalar.

    Yields:
        Pairs of ``(reference_name, type_or_None)`` discovered in *value*.
    """
    if isinstance(value, dict):
        reference_name = value.get("reference_name")
        reference_type = value.get("type")
        if isinstance(reference_name, str):
            yield reference_name, reference_type if isinstance(reference_type, str) else None
        for nested_value in value.values():
            yield from _iter_reference_pairs(nested_value)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_reference_pairs(item)


def _count_triggers_for_pipeline(pipeline_name: str, triggers: list[dict]) -> int:
    """Counts triggers that bind to a given pipeline.

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
        for bound_pipeline in bound_pipelines:
            if not isinstance(bound_pipeline, dict):
                continue
            pipeline_reference = bound_pipeline.get("pipeline_reference") or {}
            if isinstance(pipeline_reference, dict) and pipeline_reference.get("reference_name") == pipeline_name:
                count += 1
                break
    return count


def _count_activities(pipelines: list) -> tuple[ObjectCount, list[str]]:
    """Walks nested activities, classify supported/unsupported.

    Returns:
        A tuple of ``(ObjectCount, unsupported_type_names)``.
    """
    all_activities: list[dict] = []
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            logger.warning("Skipping non-dictionary pipeline")
            continue
        all_activities.extend(_collect_activities(pipeline.get("activities")))

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
    """Classifies datasets and builds details.

    Returns:
        A tuple of ``(ObjectCount, dataset_details, unsupported_type_names)``.
    """
    linked_service_type_map = _build_linked_service_type_map(linked_services)

    supported = 0
    unsupported = 0
    unsupported_types: set[str] = set()
    details: list[DatasetDetail] = []

    for dataset in datasets:
        properties = dataset.get("properties", {})
        dataset_type = properties.get("type") or "Unknown"
        linked_service_reference = properties.get("linked_service_name", {})
        linked_service_name = (
            linked_service_reference.get("reference_name") if isinstance(linked_service_reference, dict) else None
        )
        linked_service_type = linked_service_type_map.get(linked_service_name) if linked_service_name else None

        details.append(
            DatasetDetail(
                dataset_name=dataset.get("name", "Unknown"),
                dataset_type=dataset_type,
                linked_service_name=linked_service_name,
                linked_service_type=linked_service_type,
            )
        )

        if dataset_type in SUPPORTED_DATASET_TYPES:
            supported += 1
        else:
            unsupported += 1
            unsupported_types.add(dataset_type)

    total = supported + unsupported
    return ObjectCount(total, supported, unsupported), details, sorted(unsupported_types)


def _count_linked_services(linked_services: list[dict]) -> ObjectCount:
    """Count supported vs unsupported linked services."""
    supported = sum(
        1
        for linked_service in linked_services
        if linked_service.get("properties", {}).get("type") in SUPPORTED_LINKED_SERVICE_TYPES
    )
    total = len(linked_services)
    return ObjectCount(total, supported, total - supported)


def _build_integration_runtime_details(
    integration_runtimes: list[dict],
) -> tuple[ObjectCount, list[IntegrationRuntimeDetail]]:
    """Build integration runtime details and counts.

    Returns:
        A tuple of ``(ObjectCount, integration_runtime_details)``.
    """
    details: list[IntegrationRuntimeDetail] = []
    for integration_runtime in integration_runtimes:
        properties = integration_runtime.get("properties", {})
        node_count = None
        if properties.get("type") == "SelfHosted":
            node_count = properties.get("type_properties", {}).get("compute_properties", {}).get("number_of_nodes")
        details.append(
            IntegrationRuntimeDetail(
                name=integration_runtime.get("name", "Unknown"),
                runtime_type=properties.get("type", "Unknown"),
                node_count=node_count,
            )
        )
    total = len(integration_runtimes)
    return ObjectCount(total, total, 0), details


def _collect_activities(activities: list[dict] | None) -> list[dict]:
    """Recursively collects every activity (including nested ones) into a new list.

    Walks each activity's ``activities`` (ForEach inner) and
    ``if_true_activities`` / ``if_false_activities`` (IfCondition branches),
    appending the activity itself followed by its descendants in
    pre-order.

    Args:
        activities: List of activity dicts to process, or ``None``.

    Returns:
        A flat list of every activity dict found in *activities*.  An empty
        list is returned when the input is ``None`` or empty.
    """
    result: list[dict] = []
    for activity in activities or []:
        result.append(activity)
        # ForEach inner activities
        result.extend(_collect_activities(activity.get("activities")))
        # IfCondition branches
        for branch_key in ("if_true_activities", "if_false_activities"):
            result.extend(_collect_activities(activity.get(branch_key)))
    return result


def _build_linked_service_type_map(linked_services: list[dict]) -> dict[str, str | None]:
    """Build a name -> type lookup for linked services.

    Args:
        linked_services: Full list of linked service dicts.

    Returns:
        Mapping from linked service name to its type string.
    """
    return {
        linked_service.get("name", ""): linked_service.get("properties", {}).get("type")
        for linked_service in linked_services
    }

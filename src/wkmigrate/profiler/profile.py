"""Data models for Azure Data Factory profiling results."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ObjectCount:
    """Counts of total, supported, and unsupported objects."""

    total: int
    supported: int
    unsupported: int


@dataclass(slots=True)
class DatasetDetail:
    """Detail record for a single dataset."""

    dataset_name: str
    dataset_type: str
    linked_service_name: str | None
    linked_service_type: str | None


@dataclass(slots=True)
class IntegrationRuntimeDetail:
    """Detail record for a single integration runtime."""

    name: str
    runtime_type: str
    node_count: int | None = None


@dataclass(slots=True)
class PipelineDetail:
    """Per-pipeline breakdown of translatability and referenced resources.

    Helps users see *which* pipelines in a factory are most translatable by
    wkmigrate.  The supported/unsupported splits on activities, datasets, and
    linked services count only the resources this pipeline actually uses; the
    five integer totals make it easy to skim the dependency surface (e.g. a
    pipeline that pulls 12 datasets across 7 linked services and 2 self-hosted
    integration runtimes will surface much higher friction than one that runs
    against a single Delta dataset).
    """

    pipeline_name: str
    # Activities physically inside this pipeline (including those nested in
    # ForEach loops and IfCondition branches).
    activities: ObjectCount
    # Datasets referenced by this pipeline's activities.
    datasets: ObjectCount
    # Linked services referenced by this pipeline (directly or transitively
    # through its datasets).
    linked_services: ObjectCount
    # Total counts for the pipeline (mirrors ``.activities.total`` etc. plus
    # adds the trigger / integration-runtime totals that aren't broken down by
    # supported/unsupported).
    total_activities: int = 0
    total_datasets: int = 0
    total_linked_services: int = 0
    total_triggers: int = 0
    total_integration_runtimes: int = 0


@dataclass(slots=True)
class FactoryProfile:
    """Complete profile of an Azure Data Factory resource."""

    factory_name: str
    pipelines: ObjectCount
    activities: ObjectCount
    linked_services: ObjectCount
    datasets: ObjectCount
    triggers: ObjectCount
    integration_runtimes: ObjectCount
    dataset_details: list[DatasetDetail] = field(default_factory=list)
    integration_runtime_details: list[IntegrationRuntimeDetail] = field(default_factory=list)
    pipeline_details: list[PipelineDetail] = field(default_factory=list)
    unsupported_activity_types: list[str] = field(default_factory=list)
    unsupported_dataset_types: list[str] = field(default_factory=list)

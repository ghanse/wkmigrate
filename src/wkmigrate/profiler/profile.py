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
    unsupported_activity_types: list[str] = field(default_factory=list)
    unsupported_dataset_types: list[str] = field(default_factory=list)

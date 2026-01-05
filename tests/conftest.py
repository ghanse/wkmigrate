"""Pytest fixtures and lightweight client doubles."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import pytest

JSON_PATH = os.path.join(os.path.dirname(__file__), "resources", "json")
YAML_PATH = os.path.join(os.path.dirname(__file__), "resources", "yaml")


@dataclass
class MockFactoryClient:
    """Mock FactoryClient double backed by JSON fixtures."""

    test_json_path: str = JSON_PATH

    def get_pipeline(self, pipeline_name: str) -> dict:
        """Return a pipeline definition.

        Args:
            pipeline_name: Name of the pipeline to look up.

        Returns:
            Serialized pipeline definition as a ``dict``.

        Raises:
            ValueError: If no pipeline matches the provided name.
        """
        with open(f"{self.test_json_path}/test_pipelines.json", "rb") as file:
            pipelines = json.load(file)
        for pipeline in pipelines:
            if pipeline.get("name") == pipeline_name:
                return pipeline
        raise ValueError(f'No pipeline found with name "{pipeline_name}"')

    def get_trigger(self, pipeline_name: str) -> dict:
        """Return the trigger associated with a pipeline.

        Args:
            pipeline_name: Name of the pipeline the trigger belongs to.

        Returns:
            Trigger definition as a ``dict``.

        Raises:
            ValueError: If no trigger is associated with the pipeline.
        """
        with open(f"{self.test_json_path}/test_triggers.json", "rb") as file:
            triggers = json.load(file)
        for trigger in triggers:
            properties = trigger.get("properties")
            if not properties:
                continue
            pipelines = properties.get("pipelines") or []
            pipeline_names = [
                reference.get("reference_name")
                for pipeline in pipelines
                if (reference := pipeline.get("pipeline_reference")) is not None
                and reference.get("type") == "PipelineReference"
            ]
            if pipeline_name in pipeline_names:
                return trigger
        raise ValueError(f'No trigger found for pipeline with name "{pipeline_name}"')

    def get_dataset(self, dataset_name: str) -> dict:
        """
        Returns a dataset definition.

        Args:
            dataset_name: Dataset name to look up.

        Returns:
            Dataset definition including its linked service metadata as a ``dict``.

        Raises:
            ValueError: If no dataset matches ``dataset_name``.
        """
        with open(f"{self.test_json_path}/test_datasets.json", "rb") as file:
            datasets = json.load(file)
        for dataset in datasets:
            properties = dataset.get("properties")
            if not properties:
                return dataset
            linked_service_ref = properties.get("linked_service_name")
            if linked_service_ref is None:
                return dataset
            linked_service_name = linked_service_ref.get("reference_name")
            dataset["linked_service_definition"] = self.get_linked_service(linked_service_name)
            return dataset
        raise ValueError(f'No dataset found for factory with name "{dataset_name}"')

    def get_linked_service(self, linked_service_name: str) -> dict:
        """
        Returns a linked-service definition.

        Args:
            linked_service_name: Name of the linked service to load.

        Returns:
            Linked-service definition as a ``dict``.

        Raises:
            ValueError: If the linked service does not exist in fixtures.
        """
        with open(f"{self.test_json_path}/test_linked_services.json", "rb") as file:
            linked_services = json.load(file)
        for linked_service in linked_services:
            if linked_service.get("name") == linked_service_name:
                return linked_service
        raise ValueError(f'No linked service found with name "{linked_service_name}"')


@pytest.fixture
def mock_factory_client() -> MockFactoryClient:
    """Provides a FactoryClient double backed by JSON fixtures."""
    return MockFactoryClient()


@dataclass
class _MockJob:
    """Represents a Databricks job for test purposes."""

    job_id: int
    settings: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the job."""
        return {"job_id": self.job_id, "settings": self.settings}


class _MockJobsAPI:
    """Subset of WorkspaceClient.jobs used for testing."""

    def __init__(self) -> None:
        self._jobs: dict[int, _MockJob] = {}
        self._counter = 1

    def create(self, **payload: Any) -> Any:
        """Create a mock job entry."""
        job_id = self._counter
        self._counter += 1
        job = _MockJob(job_id=job_id, settings=payload.get("settings", {}))
        self._jobs[job_id] = job
        return type("JobResponse", (), {"job_id": job_id})()

    def list(self, name: str | None = None):
        """Yield stored jobs filtered by name."""
        for job in self._jobs.values():
            if name is None or job.settings.get("name") == name:
                yield type("JobSummary", (), {"job_id": job.job_id, "settings": job.settings})()

    def get(self, job_id: int) -> _MockJob:
        """Return a stored job by ID."""
        return self._jobs[job_id]


class _MockWorkspaceAPI:
    """Subset of WorkspaceClient.workspace methods for testing."""

    def __init__(self) -> None:
        self._files: set[str] = set()

    def mkdirs(self, path: str) -> None:
        """Record a directory creation call."""
        self._files.add(path)

    def import_(self, *, path: str, **_: Any) -> None:
        """Record a notebook import call."""
        self._files.add(path)

    def get_status(self, *, path: str) -> dict[str, str]:
        """Return a mock notebook status response."""
        if path not in self._files:
            raise FileNotFoundError(path)
        return {"path": path}


class _MockPipelinesAPI:
    """Subset of WorkspaceClient.pipelines used for testing."""

    def __init__(self) -> None:
        self._counter = 1

    def create(self, **_: Any) -> Any:
        """Return a mock pipeline creation response."""
        pipeline_id = f"pipeline-{self._counter}"
        self._counter += 1
        return type("PipelineResponse", (), {"pipeline_id": pipeline_id})()


class _MockScopesAPI:
    """Subset of WorkspaceClient.secrets operations for testing."""

    def __init__(self) -> None:
        self._scopes: dict[str, dict[str, str]] = {}

    def list_scopes(self) -> list[Any]:
        """Return existing scopes."""
        return [type("Scope", (), {"name": name})() for name in self._scopes]

    def create_scope(self, scope: str) -> None:
        """Create a secret scope."""
        self._scopes.setdefault(scope, {})

    def put_secret(self, *, scope: str, key: str, string_value: str) -> None:
        """Store a secret value."""
        self._scopes.setdefault(scope, {})[key] = string_value


@dataclass
class MockWorkspaceClient:
    """Mock WorkspaceClient double for testing."""

    jobs: _MockJobsAPI = field(default_factory=_MockJobsAPI)
    workspace: _MockWorkspaceAPI = field(default_factory=_MockWorkspaceAPI)
    pipelines: _MockPipelinesAPI = field(default_factory=_MockPipelinesAPI)
    secrets: _MockScopesAPI = field(default_factory=_MockScopesAPI)


@pytest.fixture
def mock_workspace_client() -> MockWorkspaceClient:
    """Provides a WorkspaceClient double for testing that materializes workflows."""
    return MockWorkspaceClient()

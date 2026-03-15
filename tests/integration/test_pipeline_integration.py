"""End-to-end integration tests for pipeline translation against a real Azure Data Factory.

These tests deploy ADF resources, read them back through wkmigrate's
``FactoryClient`` and ``FactoryDefinitionStore``, and verify that the
translated IR matches expectations. They require a live Azure subscription
with valid credentials provided via environment variables.

Mark: all tests in this module carry the ``integration`` marker so they can be
run in isolation with ``pytest -m integration``.
"""

from __future__ import annotations

import pytest

from azure.mgmt.datafactory.models import DatasetResource, LinkedServiceResource, PipelineResource

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    ForEachActivity,
    Pipeline,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# FactoryClient tests
# ---------------------------------------------------------------------------


class TestFactoryClientIntegration:
    """Validates that ``FactoryClient`` can read real ADF resources."""

    def test_list_pipelines(
        self,
        factory_client: FactoryClient,
        sample_pipeline: PipelineResource,
    ) -> None:
        """Listing pipelines returns the deployed test pipeline."""
        names = factory_client.list_pipelines()
        assert "integration_test_pipeline" in names

    def test_get_pipeline(
        self,
        factory_client: FactoryClient,
        sample_pipeline: PipelineResource,
    ) -> None:
        """Fetching a pipeline returns a dict with activities."""
        pipeline = factory_client.get_pipeline("integration_test_pipeline")
        assert isinstance(pipeline, dict)
        assert "activities" in pipeline or "properties" in pipeline

    def test_get_linked_service(
        self,
        factory_client: FactoryClient,
        sample_linked_service: LinkedServiceResource,
    ) -> None:
        """Fetching a linked service returns a dict with properties."""
        linked_service = factory_client.get_linked_service("test_blob_storage")
        assert isinstance(linked_service, dict)

    def test_get_dataset(
        self,
        factory_client: FactoryClient,
        sample_dataset: DatasetResource,
    ) -> None:
        """Fetching a dataset returns a dict with linked-service metadata."""
        dataset = factory_client.get_dataset("test_csv_dataset")
        assert isinstance(dataset, dict)


# ---------------------------------------------------------------------------
# FactoryDefinitionStore tests
# ---------------------------------------------------------------------------


class TestFactoryDefinitionStoreIntegration:
    """Validates end-to-end pipeline loading and translation via ``FactoryDefinitionStore``."""

    def test_load_pipeline(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """Loading a deployed pipeline produces a valid ``Pipeline`` IR."""
        result = factory_store.load("integration_test_pipeline")

        assert isinstance(result, Pipeline)
        assert result.name == "integration_test_pipeline"
        assert len(result.tasks) == 2

    def test_load_pipeline_activities_are_translated(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """Activities within the loaded pipeline are translated to the correct IR types."""
        result = factory_store.load("integration_test_pipeline")

        task_names = [task.name for task in result.tasks]
        assert "extract_data" in task_names
        assert "transform_data" in task_names

        for task in result.tasks:
            assert isinstance(task, DatabricksNotebookActivity)

    def test_load_pipeline_dependencies(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """Dependencies between activities are preserved in translation."""
        result = factory_store.load("integration_test_pipeline")

        transform_task = next(t for t in result.tasks if t.name == "transform_data")
        assert transform_task.depends_on is not None
        assert len(transform_task.depends_on) == 1
        assert transform_task.depends_on[0].task_key == "extract_data"

    def test_load_pipeline_parameters(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """Pipeline parameters are preserved in translation."""
        result = factory_store.load("integration_test_pipeline")

        assert result.parameters is not None
        assert len(result.parameters) >= 1

    def test_load_pipeline_tags(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """System tags are added to the translated pipeline."""
        result = factory_store.load("integration_test_pipeline")

        assert result.tags is not None
        assert "CREATED_BY_WKMIGRATE" in result.tags

    def test_load_foreach_pipeline(
        self,
        factory_store: FactoryDefinitionStore,
        sample_foreach_pipeline: PipelineResource,
    ) -> None:
        """Loading a ForEach pipeline produces the expected control-flow IR."""
        result = factory_store.load("integration_test_foreach_pipeline")

        assert isinstance(result, Pipeline)
        assert len(result.tasks) >= 1

        foreach_task = next(
            (t for t in result.tasks if isinstance(t, ForEachActivity)),
            None,
        )
        assert foreach_task is not None
        assert foreach_task.concurrency == 5

    def test_list_pipelines(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """``list_pipelines`` returns deployed pipeline names."""
        names = factory_store.list_pipelines()
        assert "integration_test_pipeline" in names

    def test_load_all(
        self,
        factory_store: FactoryDefinitionStore,
        sample_pipeline: PipelineResource,
    ) -> None:
        """``load_all`` translates all pipelines without error."""
        results = factory_store.load_all(pipeline_names=["integration_test_pipeline"])
        assert len(results) == 1
        assert all(isinstance(pipeline, Pipeline) for pipeline in results)

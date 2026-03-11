"""Tests for batch pipeline translation methods (load_all, to_jobs, to_asset_bundles)."""

import os

import pytest

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    Pipeline,
)


# ---------------------------------------------------------------------------
# FactoryDefinitionStore.list_pipelines
# ---------------------------------------------------------------------------


def test_list_pipelines_returns_all_pipeline_names(mock_factory_client) -> None:
    """list_pipelines should return every pipeline name from the factory."""
    assert mock_factory_client is not None

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    names = store.list_pipelines()
    assert isinstance(names, list)
    assert len(names) > 0
    assert "TEST_PIPELINE_NAME" in names


# ---------------------------------------------------------------------------
# FactoryDefinitionStore.load_all
# ---------------------------------------------------------------------------


def test_load_all_with_explicit_names(mock_factory_client) -> None:
    """load_all with an explicit list should return one Pipeline per name."""
    assert mock_factory_client is not None

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    pipelines = store.load_all(pipeline_names=["TEST_PIPELINE_NAME", "test_adf_pipeline_2"])
    assert len(pipelines) == 2
    assert all(isinstance(pipeline, Pipeline) for pipeline in pipelines)
    names = {pipeline.name for pipeline in pipelines}
    assert "TEST_PIPELINE_NAME" in names
    assert "test_adf_pipeline_2" in names


def test_load_all_defaults_to_all_pipelines(mock_factory_client) -> None:
    """load_all without arguments should translate every pipeline available."""
    assert mock_factory_client is not None

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    pipelines = store.load_all()
    assert isinstance(pipelines, list)
    # Should have at least one pipeline
    assert len(pipelines) >= 1
    assert all(isinstance(pipeline, Pipeline) for pipeline in pipelines)


def test_load_all_skips_failing_pipelines(mock_factory_client) -> None:
    """load_all should skip pipelines that fail to translate and return the rest."""
    assert mock_factory_client is not None

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    # Include a non-existent pipeline name alongside a valid one
    pipelines = store.load_all(pipeline_names=["TEST_PIPELINE_NAME", "DOES_NOT_EXIST"])
    assert len(pipelines) == 1
    assert pipelines[0].name == "TEST_PIPELINE_NAME"


def test_load_all_returns_empty_when_all_fail(mock_factory_client) -> None:
    """load_all should return an empty list when every pipeline fails."""
    assert mock_factory_client is not None

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    pipelines = store.load_all(pipeline_names=["DOES_NOT_EXIST_1", "DOES_NOT_EXIST_2"])
    assert pipelines == []


# ---------------------------------------------------------------------------
# WorkspaceDefinitionStore.to_jobs
# ---------------------------------------------------------------------------


def _make_workspace_store(mock_workspace_client) -> WorkspaceDefinitionStore:
    assert mock_workspace_client is not None
    return WorkspaceDefinitionStore(
        authentication_type="pat",
        host_name="https://example.com",
        pat="DUMMY_TOKEN",
    )


def _simple_pipeline(name: str = "test_pipeline") -> Pipeline:
    return Pipeline(
        name=name,
        parameters=None,
        schedule=None,
        tasks=[
            DatabricksNotebookActivity(
                name="task1",
                task_key="task1",
                notebook_path="/notebooks/etl",
            ),
        ],
        tags={},
    )


def test_to_jobs_creates_multiple_jobs(mock_workspace_client) -> None:
    """to_jobs should create one job per pipeline and return all job IDs."""
    store = _make_workspace_store(mock_workspace_client)

    pipelines = [_simple_pipeline("job_a"), _simple_pipeline("job_b"), _simple_pipeline("job_c")]
    job_ids = store.to_jobs(pipelines)

    assert len(job_ids) == 3
    assert all(isinstance(jid, int) for jid in job_ids)
    # IDs should be unique
    assert len(set(job_ids)) == 3


def test_to_jobs_empty_list(mock_workspace_client) -> None:
    """to_jobs with an empty list should return no job IDs."""
    store = _make_workspace_store(mock_workspace_client)

    job_ids = store.to_jobs([])
    assert job_ids == []


# ---------------------------------------------------------------------------
# WorkspaceDefinitionStore.to_asset_bundles
# ---------------------------------------------------------------------------


def test_to_asset_bundles_creates_subdirectories(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles should create one subdirectory per pipeline."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    pipelines = [_simple_pipeline("pipeline_a"), _simple_pipeline("pipeline_b")]
    store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "pipeline_a"))
    assert os.path.isdir(os.path.join(bundle_dir, "pipeline_b"))
    # Each subdirectory should contain a databricks.yml manifest
    assert os.path.isfile(os.path.join(bundle_dir, "pipeline_a", "databricks.yml"))
    assert os.path.isfile(os.path.join(bundle_dir, "pipeline_b", "databricks.yml"))


def test_to_asset_bundles_empty_list(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles with an empty list should not create any subdirectories."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    store.to_asset_bundles([], bundle_dir, download_notebooks=False)
    # The parent directory may or may not exist; it should have no pipeline subdirectories
    if os.path.exists(bundle_dir):
        assert os.listdir(bundle_dir) == []

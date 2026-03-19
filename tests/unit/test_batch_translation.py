"""Tests for batch pipeline translation methods (load_all, to_jobs, to_asset_bundles)."""

import logging
import os
from unittest.mock import patch

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    Pipeline,
)


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


def test_list_pipelines_returns_all_pipeline_names(mock_factory_store: FactoryDefinitionStore) -> None:
    """list_pipelines should return every pipeline name from the factory."""
    names = mock_factory_store.list_pipelines()
    assert isinstance(names, list)
    assert len(names) > 0
    assert "TEST_PIPELINE_NAME" in names


def test_load_all_with_explicit_names(mock_factory_store: FactoryDefinitionStore) -> None:
    """load_all with an explicit list should return one Pipeline per name."""
    pipelines = mock_factory_store.load_all(pipeline_names=["TEST_PIPELINE_NAME", "test_adf_pipeline_2"])
    assert len(pipelines) == 2
    assert all(isinstance(pipeline, Pipeline) for pipeline in pipelines)
    names = {pipeline.name for pipeline in pipelines}
    assert "TEST_PIPELINE_NAME" in names
    assert "test_adf_pipeline_2" in names


def test_load_all_defaults_to_all_pipelines(mock_factory_store: FactoryDefinitionStore) -> None:
    """load_all without arguments should translate every pipeline available."""
    pipelines = mock_factory_store.load_all()
    assert isinstance(pipelines, list)
    assert len(pipelines) >= 1
    assert all(isinstance(pipeline, Pipeline) for pipeline in pipelines)


def test_load_all_skips_failing_pipelines(mock_factory_store: FactoryDefinitionStore, caplog) -> None:
    """load_all should skip pipelines that fail to translate and log a warning."""
    with caplog.at_level(logging.WARNING):
        pipelines = mock_factory_store.load_all(pipeline_names=["TEST_PIPELINE_NAME", "DOES_NOT_EXIST"])
    assert len(pipelines) == 1
    assert pipelines[0].name == "TEST_PIPELINE_NAME"
    assert "DOES_NOT_EXIST" in caplog.text


def test_load_all_returns_empty_when_all_fail(mock_factory_store: FactoryDefinitionStore) -> None:
    """load_all should return an empty list when every pipeline fails."""
    pipelines = mock_factory_store.load_all(pipeline_names=["DOES_NOT_EXIST_1", "DOES_NOT_EXIST_2"])
    assert pipelines == []


def test_to_jobs_creates_multiple_jobs(mock_workspace_client) -> None:
    """to_jobs should create one job per pipeline and return all job IDs."""
    store = _make_workspace_store(mock_workspace_client)

    pipelines = [_simple_pipeline("job_a"), _simple_pipeline("job_b"), _simple_pipeline("job_c")]
    job_ids = store.to_jobs(pipelines)

    assert len(job_ids) == 3
    assert all(isinstance(jid, int) for jid in job_ids)
    assert len(set(job_ids)) == 3


def test_to_jobs_empty_list(mock_workspace_client) -> None:
    """to_jobs with an empty list should return no job IDs."""
    store = _make_workspace_store(mock_workspace_client)

    job_ids = store.to_jobs([])
    assert job_ids == []


def test_to_asset_bundles_creates_subdirectories(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles should create one subdirectory per pipeline."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    pipelines = [_simple_pipeline("pipeline_a"), _simple_pipeline("pipeline_b")]
    store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "pipeline_a"))
    assert os.path.isdir(os.path.join(bundle_dir, "pipeline_b"))
    assert os.path.isfile(os.path.join(bundle_dir, "pipeline_a", "databricks.yml"))
    assert os.path.isfile(os.path.join(bundle_dir, "pipeline_b", "databricks.yml"))


def test_to_asset_bundles_empty_list(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles with an empty list should not create any subdirectories."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    store.to_asset_bundles([], bundle_dir, download_notebooks=False)
    if os.path.exists(bundle_dir):
        assert os.listdir(bundle_dir) == []


def test_to_jobs_skips_failing_pipeline(mock_workspace_client, caplog) -> None:
    """to_jobs should skip a pipeline whose to_job call raises and log a warning."""
    store = _make_workspace_store(mock_workspace_client)
    pipelines = [_simple_pipeline("good_a"), _simple_pipeline("bad_one"), _simple_pipeline("good_b")]

    original_to_job = WorkspaceDefinitionStore.to_job

    def _patched_to_job(self, pipeline_definition):
        if pipeline_definition.name == "bad_one":
            raise RuntimeError("simulated failure")
        return original_to_job(self, pipeline_definition)

    with patch.object(WorkspaceDefinitionStore, "to_job", _patched_to_job):
        with caplog.at_level(logging.WARNING):
            job_ids = store.to_jobs(pipelines)

    assert len(job_ids) == 2
    assert all(isinstance(jid, int) for jid in job_ids)
    assert "bad_one" in caplog.text


def test_to_asset_bundles_skips_failing_pipeline(mock_workspace_client, tmp_path, caplog) -> None:
    """to_asset_bundles should skip a pipeline whose to_asset_bundle call raises and log a warning."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")
    pipelines = [_simple_pipeline("ok_pipeline"), _simple_pipeline("broken_pipeline")]

    original_to_asset_bundle = WorkspaceDefinitionStore.to_asset_bundle

    def _patched_to_asset_bundle(self, pipeline_definition, sub_directory, download_notebooks=True):
        if pipeline_definition.name == "broken_pipeline":
            raise RuntimeError("simulated failure")
        return original_to_asset_bundle(self, pipeline_definition, sub_directory, download_notebooks=download_notebooks)

    with patch.object(WorkspaceDefinitionStore, "to_asset_bundle", _patched_to_asset_bundle):
        with caplog.at_level(logging.WARNING):
            store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "ok_pipeline"))
    assert not os.path.exists(os.path.join(bundle_dir, "broken_pipeline"))
    assert "broken_pipeline" in caplog.text


def test_to_asset_bundles_sanitizes_special_characters(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles should replace special characters in pipeline names with underscores."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    pipelines = [_simple_pipeline("my.pipeline v2")]
    store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "my_pipeline_v2"))
    assert os.path.isfile(os.path.join(bundle_dir, "my_pipeline_v2", "databricks.yml"))


def test_to_asset_bundles_preserves_hyphens(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles should preserve hyphens in pipeline names."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    pipelines = [_simple_pipeline("my-pipeline")]
    store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "my-pipeline"))


def test_to_asset_bundles_handles_name_collision(mock_workspace_client, tmp_path, caplog) -> None:
    """to_asset_bundles should append a suffix when sanitized names collide."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    pipelines = [_simple_pipeline("my.pipeline"), _simple_pipeline("my pipeline")]
    with caplog.at_level(logging.WARNING):
        store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "my_pipeline"))
    assert os.path.isdir(os.path.join(bundle_dir, "my_pipeline_1"))
    assert "collides" in caplog.text


def test_to_asset_bundles_empty_name_raises(mock_workspace_client, tmp_path) -> None:
    """to_asset_bundles should raise ValueError when a pipeline name is empty."""
    import pytest

    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundles")

    pipelines = [_simple_pipeline("")]
    with pytest.raises(ValueError, match="empty after sanitization"):
        store.to_asset_bundles(pipelines, bundle_dir, download_notebooks=False)

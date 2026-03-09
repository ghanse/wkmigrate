"""Tests for definition store contracts and asset bundle generation."""

import os

import pytest
import yaml

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    ForEachActivity,
    Pipeline,
    RunJobActivity,
    WebActivity,
)


def test_factory_definition_store_requires_mandatory_fields() -> None:
    """FactoryDefinitionStore should validate required configuration fields."""
    with pytest.raises(ValueError):
        FactoryDefinitionStore(  # type: ignore[call-arg]
            tenant_id=None,
            client_id=None,
            client_secret=None,
            subscription_id=None,
            resource_group_name=None,
            factory_name=None,
        )


def test_workspace_definition_store_requires_auth_and_host() -> None:
    """WorkspaceDefinitionStore should validate authentication type and host name."""
    with pytest.raises(ValueError):
        WorkspaceDefinitionStore(  # type: ignore[call-arg]
            authentication_type="invalid",
            host_name=None,
        )


def test_factory_definition_store_uses_definition_store_interface(mock_factory_client) -> None:
    """FactoryDefinitionStore should behave as a DefinitionStore when wired with a mock client."""
    assert mock_factory_client is not None

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    assert isinstance(store, DefinitionStore)
    pipeline = store.load("TEST_PIPELINE_NAME")
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "TEST_PIPELINE_NAME"


def test_workspace_definition_store_uses_definition_store_interface(mock_workspace_client) -> None:
    """WorkspaceDefinitionStore should behave as a DefinitionStore when wired with a mock workspace client."""
    assert mock_workspace_client is not None

    store = WorkspaceDefinitionStore(
        authentication_type="pat",
        host_name="https://example.com",
        pat="DUMMY_TOKEN",
    )

    assert isinstance(store, DefinitionStore)
    assert hasattr(store, "to_job")
    assert hasattr(store, "to_asset_bundle")


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


def _foreach_pipeline() -> Pipeline:
    inner_pipeline = Pipeline(
        name="loop_inner_activities",
        parameters=None,
        schedule=None,
        tasks=[
            DatabricksNotebookActivity(name="inner_a", task_key="inner_a", notebook_path="/inner/a"),
            DatabricksNotebookActivity(name="inner_b", task_key="inner_b", notebook_path="/inner/b"),
        ],
        tags={},
    )
    return Pipeline(
        name="foreach_pipeline",
        parameters=None,
        schedule=None,
        tasks=[
            ForEachActivity(
                name="loop",
                task_key="loop",
                items_string='["x","y"]',
                for_each_task=RunJobActivity(
                    name="loop_inner_activities",
                    task_key="loop_inner_activities",
                    pipeline=inner_pipeline,
                ),
            ),
        ],
        tags={},
    )


def test_asset_bundle_creates_directory_structure(mock_workspace_client, tmp_path) -> None:
    """Asset bundle creates jobs, pipelines, and notebooks directories."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundle")
    store.to_asset_bundle(_simple_pipeline(), bundle_dir, download_notebooks=False)

    assert os.path.isdir(os.path.join(bundle_dir, "resources", "jobs"))
    assert os.path.isdir(os.path.join(bundle_dir, "resources", "pipelines"))
    assert os.path.isdir(os.path.join(bundle_dir, "notebooks"))


def test_asset_bundle_writes_job_yaml(mock_workspace_client, tmp_path) -> None:
    """Asset bundle writes a YAML job definition."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundle")
    store.to_asset_bundle(_simple_pipeline("my_job"), bundle_dir, download_notebooks=False)

    job_file = os.path.join(bundle_dir, "resources", "jobs", "my_job.yml")
    assert os.path.isfile(job_file)
    with open(job_file) as f:
        content = yaml.safe_load(f)
    assert "my_job" in content["resources"]["jobs"]


def test_asset_bundle_no_foreach_no_inner_jobs(mock_workspace_client, tmp_path) -> None:
    """Pipeline without ForEach produces no inner job YAML files."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundle")
    store.to_asset_bundle(_simple_pipeline(), bundle_dir, download_notebooks=False)

    jobs_dir = os.path.join(bundle_dir, "resources", "jobs")
    job_files = os.listdir(jobs_dir)
    assert len(job_files) == 1
    assert job_files[0] == "test_pipeline.yml"


def test_asset_bundle_foreach_writes_inner_job_yaml(mock_workspace_client, tmp_path) -> None:
    """Pipeline with ForEach writes both the main job and inner job YAML files."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundle")
    store.to_asset_bundle(_foreach_pipeline(), bundle_dir, download_notebooks=False)

    jobs_dir = os.path.join(bundle_dir, "resources", "jobs")
    job_files = sorted(os.listdir(jobs_dir))
    assert len(job_files) == 2
    assert any("foreach_pipeline" in f for f in job_files)
    assert any("loop_inner_activities" in f for f in job_files)


def test_asset_bundle_no_foreach_does_not_raise(mock_workspace_client, tmp_path) -> None:
    """Regression: pipeline without ForEach must not raise when iterating inner_jobs."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundle")
    store.to_asset_bundle(_simple_pipeline(), bundle_dir, download_notebooks=False)


def test_asset_bundle_manifest_written(mock_workspace_client, tmp_path) -> None:
    """Asset bundle writes a databricks.yml manifest."""
    store = _make_workspace_store(mock_workspace_client)
    bundle_dir = str(tmp_path / "bundle")
    store.to_asset_bundle(_simple_pipeline(), bundle_dir, download_notebooks=False)

    manifest = os.path.join(bundle_dir, "databricks.yml")
    assert os.path.isfile(manifest)


def test_to_job_web_activity_notebook_uploaded_and_dependency_checked(mock_workspace_client) -> None:
    """to_job with a Web activity uploads the generated notebook and checks it as a dependency."""
    store = _make_workspace_store(mock_workspace_client)
    pipeline = Pipeline(
        name="web_pipeline",
        parameters=None,
        schedule=None,
        tasks=[WebActivity(name="web_call", task_key="web_call", url="https://api.example.com", method="GET")],
        tags={},
    )
    job_id = store.to_job(pipeline)
    assert job_id is not None
    assert any("web_call" in path for path in mock_workspace_client.workspace._files)


def test_to_job_foreach_with_inner_notebook_recurses_dependency_check(mock_workspace_client) -> None:
    """to_job with a ForEach containing a notebook task recurses to check the inner notebook dependency."""
    store = _make_workspace_store(mock_workspace_client)
    pipeline = Pipeline(
        name="foreach_notebook_pipeline",
        parameters=None,
        schedule=None,
        tasks=[
            ForEachActivity(
                name="loop",
                task_key="loop",
                items_string='["x"]',
                for_each_task=DatabricksNotebookActivity(
                    name="inner", task_key="inner", notebook_path="/notebooks/inner"
                ),
            )
        ],
        tags={},
    )
    job_id = store.to_job(pipeline)
    assert job_id is not None

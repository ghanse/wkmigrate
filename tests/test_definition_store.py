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
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity, PreparedWorkflow
from wkmigrate.models.workflows.instructions import PipelineInstruction


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


# ---------------------------------------------------------------------------
# Override option tests
# ---------------------------------------------------------------------------


def test_overrides_default_to_empty_dict(mock_workspace_client) -> None:
    """WorkspaceDefinitionStore initialises with an empty overrides dict by default."""
    store = _make_workspace_store(mock_workspace_client)
    assert store.overrides == {}
    assert store.get_all_overrides() == {}


def test_set_and_get_override(mock_workspace_client) -> None:
    """set_override / get_override round-trips a single key."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_override('root_path', '/migrated')
    assert store.get_override('root_path') == '/migrated'


def test_get_override_returns_none_when_unset(mock_workspace_client) -> None:
    """get_override returns None for a valid key that has not been set."""
    store = _make_workspace_store(mock_workspace_client)
    assert store.get_override('catalog') is None


def test_set_all_overrides_replaces_existing(mock_workspace_client) -> None:
    """set_all_overrides replaces the entire overrides dictionary."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_override('catalog', 'old_catalog')
    store.set_all_overrides({'schema': 'new_schema'})
    assert store.get_override('schema') == 'new_schema'
    assert store.get_override('catalog') is None


def test_get_all_overrides_returns_copy(mock_workspace_client) -> None:
    """get_all_overrides returns a copy, not a reference to the internal dict."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_override('catalog', 'my_catalog')
    copy = store.get_all_overrides()
    copy['catalog'] = 'mutated'
    assert store.get_override('catalog') == 'my_catalog'


def test_invalid_override_key_raises_on_set(mock_workspace_client) -> None:
    """set_override raises ValueError for an unrecognised key."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid override key'):
        store.set_override('nonexistent_key', 'value')


def test_invalid_override_key_raises_on_get(mock_workspace_client) -> None:
    """get_override raises ValueError for an unrecognised key."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid override key'):
        store.get_override('nonexistent_key')


def test_invalid_override_key_raises_on_set_all(mock_workspace_client) -> None:
    """set_all_overrides raises ValueError when dict contains an invalid key."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid override'):
        store.set_all_overrides({'bad_key': 'value'})


def test_invalid_override_key_raises_on_init(mock_workspace_client) -> None:
    """Passing an invalid override key at construction time raises ValueError."""
    assert mock_workspace_client is not None
    with pytest.raises(ValueError, match='Invalid override'):
        WorkspaceDefinitionStore(
            authentication_type='pat',
            host_name='https://example.com',
            pat='DUMMY_TOKEN',
            overrides={'not_a_real_key': True},
        )


def test_overrides_can_be_passed_at_construction(mock_workspace_client) -> None:
    """Override options can be provided via the constructor."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        overrides={'root_path': '/prod', 'compute_type': 'serverless'},
    )
    assert store.get_override('root_path') == '/prod'
    assert store.get_override('compute_type') == 'serverless'


def test_files_to_delta_sinks_override_takes_precedence(mock_workspace_client) -> None:
    """The files_to_delta_sinks override takes precedence over the field."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        files_to_delta_sinks=False,
        overrides={'files_to_delta_sinks': True},
    )
    assert store._effective_files_to_delta_sinks() is True


def test_root_path_override_rewrites_notebook_paths(mock_workspace_client, tmp_path) -> None:
    """The root_path override rewrites notebook paths in the generated asset bundle."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        overrides={'root_path': '/migrated'},
    )
    bundle_dir = str(tmp_path / 'bundle')
    store.to_asset_bundle(_simple_pipeline(), bundle_dir, download_notebooks=False)

    job_file = os.path.join(bundle_dir, 'resources', 'jobs', 'test_pipeline.yml')
    with open(job_file) as f:
        content = yaml.safe_load(f)
    tasks = content['resources']['jobs']['test_pipeline']['tasks']
    notebook_path = tasks[0]['notebook_task']['notebook_path']
    assert notebook_path.startswith('/migrated/')


def test_compute_type_serverless_removes_new_cluster(mock_workspace_client, tmp_path) -> None:
    """The compute_type=serverless override strips new_cluster from tasks."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        overrides={'compute_type': 'serverless'},
    )
    pipeline = Pipeline(
        name='serverless_pipeline',
        parameters=None,
        schedule=None,
        tasks=[
            DatabricksNotebookActivity(
                name='task1',
                task_key='task1',
                notebook_path='/notebooks/etl',
                new_cluster={'spark_version': '13.3.x-scala2.12', 'num_workers': 2},
            ),
        ],
        tags={},
    )
    bundle_dir = str(tmp_path / 'bundle')
    store.to_asset_bundle(pipeline, bundle_dir, download_notebooks=False)

    job_file = os.path.join(bundle_dir, 'resources', 'jobs', 'serverless_pipeline.yml')
    with open(job_file) as f:
        content = yaml.safe_load(f)
    tasks = content['resources']['jobs']['serverless_pipeline']['tasks']
    assert 'new_cluster' not in tasks[0]


def test_to_job_with_overrides(mock_workspace_client) -> None:
    """to_job applies overrides and returns a valid job id."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        overrides={'compute_type': 'serverless'},
    )
    job_id = store.to_job(_simple_pipeline())
    assert job_id is not None


def test_invalid_compute_type_raises_on_set(mock_workspace_client) -> None:
    """set_override raises ValueError for an unrecognised compute_type value."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid compute_type'):
        store.set_override('compute_type', 'typo')


def test_invalid_compute_type_raises_on_init(mock_workspace_client) -> None:
    """Passing an invalid compute_type value at construction time raises ValueError."""
    assert mock_workspace_client is not None
    with pytest.raises(ValueError, match='Invalid compute_type'):
        WorkspaceDefinitionStore(
            authentication_type='pat',
            host_name='https://example.com',
            pat='DUMMY_TOKEN',
            overrides={'compute_type': 'invalid_type'},
        )


def test_invalid_compute_type_raises_on_set_all(mock_workspace_client) -> None:
    """set_all_overrides raises ValueError for an invalid compute_type value."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid compute_type'):
        store.set_all_overrides({'compute_type': 'bad_value'})


def test_catalog_schema_override_on_dlt_pipelines(mock_workspace_client) -> None:
    """catalog and schema overrides propagate to PipelineInstruction objects."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_all_overrides({'catalog': 'prod_catalog', 'schema': 'prod_schema'})

    task_ref = {'pipeline_task': {'pipeline_id': '__PIPELINE_ID__'}}
    instructions = [
        PipelineInstruction(task_ref=task_ref, file_path='/notebooks/copy', name='copy_pipeline'),
        PipelineInstruction(task_ref=task_ref, file_path='/notebooks/copy2', name='copy_pipeline_2'),
    ]

    # Verify defaults before override
    assert instructions[0].catalog == 'wkmigrate'
    assert instructions[0].target == 'wkmigrate'

    WorkspaceDefinitionStore._apply_catalog_schema_override(
        instructions, store.get_override('catalog'), store.get_override('schema')
    )

    for instr in instructions:
        assert instr.catalog == 'prod_catalog'
        assert instr.target == 'prod_schema'


def test_catalog_only_override_leaves_schema_unchanged(mock_workspace_client) -> None:
    """Setting only catalog leaves target (schema) at its default."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_override('catalog', 'new_catalog')

    instr = PipelineInstruction(task_ref={}, file_path='/notebooks/x', name='p', target='original_schema')
    WorkspaceDefinitionStore._apply_catalog_schema_override(
        [instr], store.get_override('catalog'), store.get_override('schema')
    )
    assert instr.catalog == 'new_catalog'
    assert instr.target == 'original_schema'


def test_root_path_override_rewrites_notebook_artifact_file_path(mock_workspace_client) -> None:
    """root_path override rewrites NotebookArtifact.file_path in all_notebooks."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_override('root_path', '/migrated')

    notebook = NotebookArtifact(file_path='/notebooks/etl.py', content='# etl')
    activity = PreparedActivity(
        task={'task_key': 'task1', 'notebook_task': {'notebook_path': '/notebooks/etl'}},
        notebooks=[notebook],
    )
    prepared = PreparedWorkflow(pipeline=_simple_pipeline().tasks[0], activities=[activity])
    # Manually set pipeline attr for the PreparedWorkflow (it expects a Pipeline but we
    # only need the shape for _apply_overrides)
    prepared.pipeline = _simple_pipeline()  # type: ignore[assignment]

    store._apply_overrides(prepared)

    assert notebook.file_path.startswith('/migrated/')
    assert '/notebooks/etl.py' in notebook.file_path


def test_root_path_override_recurses_into_for_each_task(mock_workspace_client) -> None:
    """root_path override rewrites notebook paths inside for_each_task nested tasks."""

    tasks: list[dict] = [
        {
            'task_key': 'outer',
            'for_each_task': {
                'task': {
                    'task_key': 'inner',
                    'notebook_task': {'notebook_path': '/original/notebook'},
                }
            },
        }
    ]

    WorkspaceDefinitionStore._apply_root_path_override(tasks, '/migrated')

    inner_task = tasks[0]['for_each_task']['task']
    assert inner_task['notebook_task']['notebook_path'] == '/migrated/original/notebook'


def test_compute_type_serverless_recurses_into_for_each_task(mock_workspace_client) -> None:
    """compute_type=serverless strips new_cluster from tasks inside for_each_task."""
    tasks: list[dict] = [
        {
            'task_key': 'outer',
            'for_each_task': {
                'task': {
                    'task_key': 'inner',
                    'notebook_task': {'notebook_path': '/notebooks/etl'},
                    'new_cluster': {'spark_version': '13.3.x-scala2.12', 'num_workers': 2},
                }
            },
        }
    ]

    WorkspaceDefinitionStore._apply_compute_type_override(tasks, 'serverless')

    inner_task = tasks[0]['for_each_task']['task']
    assert 'new_cluster' not in inner_task

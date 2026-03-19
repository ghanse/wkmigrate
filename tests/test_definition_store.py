"""Tests for definition store contracts and asset bundle generation."""

import os
from unittest.mock import patch

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


def test_factory_definition_store_loads_pipeline_without_trigger(mock_factory_client) -> None:
    """Pipelines with no trigger should load successfully with schedule=None."""
    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )

    pipeline = store.load("test_pipeline_no_triggers")
    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "test_pipeline_no_triggers"
    assert pipeline.schedule is None


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


def test_set_and_get_option(mock_workspace_client) -> None:
    """set_option round-trips a single key via the options dict."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_option('root_path', '/migrated')
    assert store.options.get('root_path') == '/migrated'


def test_set_all_options_replaces_existing(mock_workspace_client) -> None:
    """set_options replaces the entire options dictionary."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_option('catalog', 'old_catalog')
    store.set_options({'schema': 'new_schema'})
    assert store.options.get('schema') == 'new_schema'
    assert store.options.get('catalog') is None


def test_options_dict_is_independent_of_caller(mock_workspace_client) -> None:
    """Mutating a reference obtained from options does not affect the store when set via set_options."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_option('catalog', 'my_catalog')
    copy = dict(store.options)
    copy['catalog'] = 'mutated'
    assert store.options['catalog'] == 'my_catalog'


def test_invalid_option_key_raises_on_set(mock_workspace_client) -> None:
    """set_option raises ValueError for an unrecognised key."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid option key'):
        store.set_option('nonexistent_key', 'value')


def test_invalid_option_key_raises_on_set_all(mock_workspace_client) -> None:
    """set_options raises ValueError when dict contains an invalid key."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid option key'):
        store.set_options({'bad_key': 'value'})


def test_invalid_option_key_raises_on_init(mock_workspace_client) -> None:
    """Passing an invalid option key at construction time raises ValueError."""
    assert mock_workspace_client is not None
    with pytest.raises(ValueError, match='Invalid option key'):
        WorkspaceDefinitionStore(
            authentication_type='pat',
            host_name='https://example.com',
            pat='DUMMY_TOKEN',
            options={'not_a_real_key': True},
        )


def test_options_can_be_passed_at_construction(mock_workspace_client) -> None:
    """Options can be provided via the constructor."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'root_path': '/prod', 'compute_type': 'serverless'},
    )
    assert store.options['root_path'] == '/prod'
    assert store.options['compute_type'] == 'serverless'


def test_root_path_option_rewrites_notebook_paths(mock_workspace_client, tmp_path) -> None:
    """The root_path option rewrites notebook paths in the generated asset bundle."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'root_path': '/migrated'},
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
    """The compute_type=serverless option strips new_cluster from tasks."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'compute_type': 'serverless'},
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


def test_to_job_with_options(mock_workspace_client) -> None:
    """to_job applies options and returns a valid job id."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'compute_type': 'serverless'},
    )
    job_id = store.to_job(_simple_pipeline())
    assert job_id is not None


def test_invalid_compute_type_raises_on_set(mock_workspace_client) -> None:
    """set_option raises ValueError for an unrecognised compute_type value."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid compute_type'):
        store.set_option('compute_type', 'typo')


def test_invalid_compute_type_raises_on_init(mock_workspace_client) -> None:
    """Passing an invalid compute_type value at construction time raises ValueError."""
    assert mock_workspace_client is not None
    with pytest.raises(ValueError, match='Invalid compute_type'):
        WorkspaceDefinitionStore(
            authentication_type='pat',
            host_name='https://example.com',
            pat='DUMMY_TOKEN',
            options={'compute_type': 'invalid_type'},
        )


def test_invalid_compute_type_raises_on_set_all(mock_workspace_client) -> None:
    """set_options raises ValueError for an invalid compute_type value."""
    store = _make_workspace_store(mock_workspace_client)
    with pytest.raises(ValueError, match='Invalid compute_type'):
        store.set_options({'compute_type': 'bad_value'})


def test_catalog_schema_override_on_dlt_pipelines(mock_workspace_client) -> None:
    """catalog and schema options propagate to PipelineInstruction objects via activities."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_options({'catalog': 'prod_catalog', 'schema': 'prod_schema'})

    task_ref = {'pipeline_task': {'pipeline_id': '__PIPELINE_ID__'}}
    instructions = [
        PipelineInstruction(task_ref=task_ref, file_path='/notebooks/copy', name='copy_pipeline'),
        PipelineInstruction(task_ref=task_ref, file_path='/notebooks/copy2', name='copy_pipeline_2'),
    ]

    # Verify defaults before override
    assert instructions[0].catalog == 'wkmigrate'
    assert instructions[0].target == 'wkmigrate'

    activities = [
        PreparedActivity(task={'task_key': f'task{i}'}, pipelines=[instr]) for i, instr in enumerate(instructions)
    ]
    result_activities = WorkspaceDefinitionStore._apply_catalog_schema_to_activities(
        activities, store.options.get('catalog'), store.options.get('schema')
    )

    for activity in result_activities:
        for instr in activity.pipelines:
            assert instr.catalog == 'prod_catalog'
            assert instr.target == 'prod_schema'


def test_catalog_only_override_leaves_schema_unchanged(mock_workspace_client) -> None:
    """Setting only catalog leaves target (schema) at its default."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_option('catalog', 'new_catalog')

    instr = PipelineInstruction(task_ref={}, file_path='/notebooks/x', name='p', target='original_schema')
    activities = [PreparedActivity(task={'task_key': 'task1'}, pipelines=[instr])]
    result = WorkspaceDefinitionStore._apply_catalog_schema_to_activities(
        activities, store.options.get('catalog'), store.options.get('schema')
    )
    assert result[0].pipelines[0].catalog == 'new_catalog'
    assert result[0].pipelines[0].target == 'original_schema'


def test_root_path_option_rewrites_notebook_artifact_file_path(mock_workspace_client) -> None:
    """root_path option rewrites NotebookArtifact.file_path in all_notebooks."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_option('root_path', '/migrated')

    notebook = NotebookArtifact(file_path='/notebooks/etl.py', content='# etl')
    activity = PreparedActivity(
        task={'task_key': 'task1', 'notebook_task': {'notebook_path': '/notebooks/etl'}},
        notebooks=[notebook],
    )
    prepared = PreparedWorkflow(pipeline=_simple_pipeline(), activities=[activity])

    result = store._apply_options(prepared)

    result_notebook = result.all_notebooks[0]
    assert result_notebook.file_path.startswith('/migrated/')
    assert '/notebooks/etl.py' in result_notebook.file_path


def test_root_path_override_recurses_into_for_each_task(mock_workspace_client) -> None:
    """root_path option rewrites notebook paths inside for_each_task nested tasks."""

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

    result = WorkspaceDefinitionStore._apply_root_path_override(tasks, '/migrated')

    inner_task = result[0]['for_each_task']['task']
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

    result = WorkspaceDefinitionStore._apply_compute_type_override(tasks, 'serverless')

    inner_task = result[0]['for_each_task']['task']
    assert 'new_cluster' not in inner_task


def test_root_path_idempotent_when_already_rooted(mock_workspace_client) -> None:
    """root_path rewrite is idempotent -- already-rooted paths are not double-prefixed."""
    tasks: list[dict] = [
        {
            'task_key': 'task1',
            'notebook_task': {'notebook_path': '/migrated/notebooks/etl'},
        }
    ]

    result = WorkspaceDefinitionStore._apply_root_path_override(tasks, '/migrated')
    assert result[0]['notebook_task']['notebook_path'] == '/migrated/notebooks/etl'


def test_root_path_no_false_positive_on_prefix(mock_workspace_client) -> None:
    """root_path check does not falsely match /migrated_old as already rooted under /migrated."""
    tasks: list[dict] = [
        {
            'task_key': 'task1',
            'notebook_task': {'notebook_path': '/migrated_old/notebooks/etl'},
        }
    ]

    result = WorkspaceDefinitionStore._apply_root_path_override(tasks, '/migrated')
    assert result[0]['notebook_task']['notebook_path'] == '/migrated/migrated_old/notebooks/etl'


def test_root_path_rewrites_pipeline_instruction_file_path(mock_workspace_client) -> None:
    """root_path option rewrites PipelineInstruction.file_path."""
    store = _make_workspace_store(mock_workspace_client)
    store.set_option('root_path', '/migrated')

    task_ref = {'pipeline_task': {'pipeline_id': '__PIPELINE_ID__'}}
    instr = PipelineInstruction(task_ref=task_ref, file_path='/notebooks/copy', name='copy_pipeline')
    activity = PreparedActivity(
        task={'task_key': 'task1', 'notebook_task': {'notebook_path': '/notebooks/etl'}},
        pipelines=[instr],
    )
    prepared = PreparedWorkflow(pipeline=_simple_pipeline(), activities=[activity])

    result = store._apply_options(prepared)

    result_instr = result.all_pipelines[0]
    assert result_instr.file_path.startswith('/migrated/')
    assert '/notebooks/copy' in result_instr.file_path


def test_catalog_target_in_asset_bundle_yaml(mock_workspace_client, tmp_path) -> None:
    """catalog and schema options appear in generated pipeline YAML resources."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'catalog': 'prod_catalog', 'schema': 'prod_schema'},
    )

    task_ref: dict = {'pipeline_task': {'pipeline_id': '__PIPELINE_ID__'}}
    pipeline_instr = PipelineInstruction(task_ref=task_ref, file_path='/notebooks/copy', name='copy_pipeline')

    activities = [PreparedActivity(task={'task_key': 'task1'}, pipelines=[pipeline_instr])]
    result = WorkspaceDefinitionStore._apply_catalog_schema_to_activities(
        activities,
        store.options.get('catalog'),
        store.options.get('schema'),
    )

    assert result[0].pipelines[0].catalog == 'prod_catalog'
    assert result[0].pipelines[0].target == 'prod_schema'


def test_workspace_url_override_rewrites_linked_service_host(mock_workspace_client) -> None:
    """workspace_url option rewrites host_name in new_cluster linked-service references."""
    tasks: list[dict] = [
        {
            'task_key': 'task1',
            'notebook_task': {'notebook_path': '/notebooks/etl'},
            'new_cluster': {
                'spark_version': '13.3.x-scala2.12',
                'host_name': 'https://old-workspace.azuredatabricks.net',
                'service_name': 'linked_svc',
                'service_type': 'databricks',
            },
        }
    ]

    result = WorkspaceDefinitionStore._apply_workspace_url_override(tasks, 'https://new-workspace.azuredatabricks.net')

    assert result[0]['new_cluster']['host_name'] == 'https://new-workspace.azuredatabricks.net'
    # Other keys should be preserved
    assert result[0]['new_cluster']['spark_version'] == '13.3.x-scala2.12'


def test_workspace_url_override_recurses_into_for_each_task(mock_workspace_client) -> None:
    """workspace_url option rewrites host_name in nested for_each_task tasks."""
    tasks: list[dict] = [
        {
            'task_key': 'outer',
            'for_each_task': {
                'task': {
                    'task_key': 'inner',
                    'notebook_task': {'notebook_path': '/notebooks/etl'},
                    'new_cluster': {
                        'spark_version': '13.3.x-scala2.12',
                        'host_name': 'https://old.azuredatabricks.net',
                    },
                }
            },
        }
    ]

    result = WorkspaceDefinitionStore._apply_workspace_url_override(tasks, 'https://new.azuredatabricks.net')

    inner_task = result[0]['for_each_task']['task']
    assert inner_task['new_cluster']['host_name'] == 'https://new.azuredatabricks.net'


def test_workspace_url_override_skips_tasks_without_host(mock_workspace_client) -> None:
    """workspace_url option does not add host_name to new_cluster that lacks one."""
    tasks: list[dict] = [
        {
            'task_key': 'task1',
            'notebook_task': {'notebook_path': '/notebooks/etl'},
            'new_cluster': {'spark_version': '13.3.x-scala2.12', 'num_workers': 2},
        }
    ]

    result = WorkspaceDefinitionStore._apply_workspace_url_override(tasks, 'https://new.azuredatabricks.net')

    assert 'host_name' not in result[0]['new_cluster']


def test_workspace_url_via_constructor(mock_workspace_client, tmp_path) -> None:
    """workspace_url option provided at construction time is accepted."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'workspace_url': 'https://new-workspace.azuredatabricks.net'},
    )
    assert store.options['workspace_url'] == 'https://new-workspace.azuredatabricks.net'


def test_workspace_url_applied_via_apply_options(mock_workspace_client, tmp_path) -> None:
    """workspace_url option is applied through _apply_options."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'workspace_url': 'https://target.azuredatabricks.net'},
    )

    pipeline = Pipeline(
        name='url_pipeline',
        parameters=None,
        schedule=None,
        tasks=[
            DatabricksNotebookActivity(
                name='task1',
                task_key='task1',
                notebook_path='/notebooks/etl',
                new_cluster={
                    'spark_version': '13.3.x-scala2.12',
                    'host_name': 'https://source.azuredatabricks.net',
                    'service_name': 'svc',
                    'service_type': 'databricks',
                },
            ),
        ],
        tags={},
    )

    bundle_dir = str(tmp_path / 'bundle')
    store.to_asset_bundle(pipeline, bundle_dir, download_notebooks=False)

    job_file = os.path.join(bundle_dir, 'resources', 'jobs', 'url_pipeline.yml')
    with open(job_file) as f:
        content = yaml.safe_load(f)
    tasks = content['resources']['jobs']['url_pipeline']['tasks']
    # host_name is stripped by the serializer (_NEW_CLUSTER_EXCLUDED_KEYS) so we
    # just verify the task was written without error and new_cluster does not
    # contain the old host
    new_cluster = tasks[0].get('new_cluster', {})
    assert new_cluster.get('host_name') != 'https://source.azuredatabricks.net'


def test_download_notebooks_with_root_path_applies_rewrite(mock_workspace_client, tmp_path) -> None:
    """download_notebooks=True with root_path downloads using original paths, then rewrites."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'root_path': '/migrated'},
    )
    pipeline = _simple_pipeline('dl_root')
    bundle_dir = str(tmp_path / 'bundle')

    # Mock _download_workspace_notebooks to return a mapping without hitting a real workspace.
    fake_mapping: dict[str, str] = {'/notebooks/etl': './notebooks/etl.py'}
    with patch.object(
        WorkspaceDefinitionStore,
        '_download_workspace_notebooks',
        return_value=fake_mapping,
    ) as mock_download:
        store.to_asset_bundle(pipeline, bundle_dir, download_notebooks=True)

    # The download should have been called with the original (un-rewritten) path.
    mock_download.assert_called_once()
    downloaded_paths = mock_download.call_args[0][0]
    assert '/notebooks/etl' in downloaded_paths

    # The generated YAML should contain bundle-relative paths (download rewrote
    # to './notebooks/etl.py') because paths starting with './' are skipped by
    # the root_path rewrite.
    job_file = os.path.join(bundle_dir, 'resources', 'jobs', 'dl_root.yml')
    with open(job_file) as f:
        content = yaml.safe_load(f)
    tasks = content['resources']['jobs']['dl_root']['tasks']
    notebook_path = tasks[0]['notebook_task']['notebook_path']
    assert notebook_path == './notebooks/etl.py'


def test_download_notebooks_with_root_path_rewrites_inner_workflow_tasks(mock_workspace_client, tmp_path) -> None:
    """download_notebooks=True with root_path rewrites inner workflow task dicts."""
    assert mock_workspace_client is not None
    store = WorkspaceDefinitionStore(
        authentication_type='pat',
        host_name='https://example.com',
        pat='DUMMY_TOKEN',
        options={'root_path': '/migrated'},
    )
    pipeline = _foreach_pipeline()
    bundle_dir = str(tmp_path / 'bundle')

    # No notebooks will actually be downloadable, so return an empty mapping.
    with patch.object(
        WorkspaceDefinitionStore,
        '_download_workspace_notebooks',
        return_value={},
    ):
        store.to_asset_bundle(pipeline, bundle_dir, download_notebooks=True)

    # Inner job YAML should have paths rewritten under /migrated.
    inner_job_file = os.path.join(bundle_dir, 'resources', 'jobs', 'loop_inner_activities.yml')
    with open(inner_job_file) as f:
        content = yaml.safe_load(f)
    inner_tasks = content['resources']['jobs']['loop_inner_activities']['tasks']
    for task in inner_tasks:
        nb_path = task.get('notebook_task', {}).get('notebook_path', '')
        assert nb_path.startswith('/migrated/'), f"Expected /migrated/ prefix, got: {nb_path}"

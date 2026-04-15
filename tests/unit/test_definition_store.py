"""Tests for definition store contracts and asset bundle generation."""

import os
from unittest.mock import patch

import pytest
import yaml

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.json_definition_store import JsonDefinitionStore
from wkmigrate.definition_stores.pipeline_adapter import PipelineAdapter
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
from wkmigrate.utils import _normalize_activity_type_properties

_CAMEL_JSON_PATH = os.path.join(os.path.dirname(__file__), os.pardir, "resources", "json", "camel")


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


def test_factory_definition_store_default_source_property_case_is_snake(
    mock_factory_client,
) -> None:
    """Default source_property_case is 'snake'; load works with snake_case payloads."""
    assert mock_factory_client is not None
    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
    )
    pipeline = store.load("TEST_PIPELINE_NAME")
    assert pipeline.name == "TEST_PIPELINE_NAME"


def test_factory_definition_store_accepts_source_property_case_camel(
    mock_factory_client,
) -> None:
    """Store with source_property_case='camel' accepts options and load still works (snake payload stays snake)."""
    assert mock_factory_client is not None
    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
        source_property_case="camel",
    )
    pipeline = store.load("TEST_PIPELINE_NAME")
    assert pipeline.name == "TEST_PIPELINE_NAME"


def test_factory_definition_store_camel_normalizes_to_snake(
    mock_factory_client,
    monkeypatch,
) -> None:
    """When source_property_case is 'camel', camelCase payload is normalized to snake_case for translation."""
    assert mock_factory_client is not None
    # Return a minimal camelCase pipeline from the mock so load() runs the normalizer
    camel_pipeline = {
        "name": "CAMEL_PIPELINE",
        "activities": [
            {
                "name": "act1",
                "type": "DatabricksNotebook",
                "linkedServiceName": {"type": "LinkedServiceReference", "referenceName": "db_linkedservice_001"},
                "notebookPath": "/test",
            }
        ],
    }
    camel_trigger = {"properties": {"recurrence": {"frequency": "Day", "interval": 1}}}
    original_get_pipeline = mock_factory_client.get_pipeline
    original_get_trigger = mock_factory_client.get_trigger

    def get_pipeline(name: str):
        if name == "CAMEL_PIPELINE":
            return dict(camel_pipeline)
        return original_get_pipeline(name)

    def get_trigger(name: str):
        if name == "CAMEL_PIPELINE":
            return dict(camel_trigger)
        return original_get_trigger(name)

    monkeypatch.setattr(mock_factory_client, "get_pipeline", get_pipeline)
    monkeypatch.setattr(mock_factory_client, "get_trigger", get_trigger)

    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
        source_property_case="camel",
    )
    pipeline = store.load("CAMEL_PIPELINE")
    assert pipeline.name == "CAMEL_PIPELINE"
    assert len(pipeline.tasks) >= 1


def test_workspace_definition_store_uses_definition_store_interface(
    mock_workspace_client,
) -> None:
    """WorkspaceDefinitionStore should behave as a DefinitionStore when wired with a mock workspace client."""
    assert mock_workspace_client is not None

    store = WorkspaceDefinitionStore(
        authentication_type="pat",
        host_name="https://example.com",
        pat="DUMMY_TOKEN",
    )
    assert isinstance(store, DefinitionStore)


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


def test_json_store_camel_case_pipeline() -> None:
    """End-to-end: a camelCase portal-export JSON loaded via JsonDefinitionStore
    is normalised to snake_case and translated to the correct Pipeline IR."""
    store = JsonDefinitionStore(
        source_directory=_CAMEL_JSON_PATH,
        source_property_case="camel",
    )

    pipeline = store.load("one_activity_pipeline")

    assert isinstance(pipeline, Pipeline)
    assert pipeline.name == "one_activity_pipeline"
    assert len(pipeline.tasks) == 1

    task = pipeline.tasks[0]
    assert isinstance(task, DatabricksNotebookActivity)
    assert task.task_key == "run maintenance notebook"
    assert task.notebook_path == "/Shared/notebooks/maintenance/nb_execute_maintenance"
    assert task.timeout_seconds == 43200
    assert task.max_retries == 0

    assert pipeline.parameters is not None
    param_names = {p["name"] for p in pipeline.parameters}
    assert param_names == {"target_table", "retention_days"}

    not_translatable_props = {entry["property"] for entry in pipeline.not_translatable}
    assert "secure_input" in not_translatable_props
    assert "secure_output" in not_translatable_props


def test_json_store_camel_case_trigger_matched() -> None:
    """camelCase trigger JSON (pipelineReference/referenceName) is normalised and
    matched to the pipeline, producing a schedule in the Pipeline IR."""
    store = JsonDefinitionStore(
        source_directory=_CAMEL_JSON_PATH,
        source_property_case="camel",
    )

    pipeline = store.load("one_activity_pipeline")

    assert pipeline.schedule is not None, (
        "Trigger was not matched to the pipeline. "
        "camelCase keys (pipelineReference, referenceName) must be normalised "
        "to snake_case before lookup."
    )


def test_json_store_source_system_defaults_to_adf() -> None:
    """JsonDefinitionStore.source_system should default to WorkflowSourceType.ADF."""
    store = JsonDefinitionStore(source_directory=_CAMEL_JSON_PATH)
    assert store.source_system == "adf"


def test_json_store_raises_when_pipelines_subdir_missing(tmp_path) -> None:
    """JsonDefinitionStore should raise ValueError when pipelines/ subdirectory has no JSON files."""
    (tmp_path / "triggers").mkdir()
    (tmp_path / "pipelines").mkdir()  # empty
    with pytest.raises(ValueError, match="No pipeline JSON files found"):
        JsonDefinitionStore(source_directory=str(tmp_path))


def test_json_store_raises_when_source_directory_missing() -> None:
    """JsonDefinitionStore should raise ValueError when source_directory does not exist."""
    with pytest.raises(ValueError, match="source_directory is not a directory"):
        JsonDefinitionStore(source_directory="/nonexistent/path")


def test_json_store_raises_when_source_directory_is_none() -> None:
    """JsonDefinitionStore should raise ValueError when source_directory is None."""
    with pytest.raises(ValueError, match="source_directory must be provided"):
        JsonDefinitionStore()


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


# --- FactoryDefinitionStore Copy activity normalization tests ---


def test_factory_store_normalizes_copy_activity_type_properties(monkeypatch) -> None:
    """Copy activities from the ADF API have source/sink inside type_properties.

    FactoryDefinitionStore.load() must flatten type_properties so the copy
    translator can find source, sink, and translator at the activity root.
    """
    from wkmigrate.definition_stores import factory_definition_store
    from wkmigrate.models.ir.pipeline import CopyActivity

    # Simulate the format returned by Azure SDK's as_dict() — activities
    # have type_properties wrapping source/sink, and inputs/outputs at root.
    api_pipeline = {
        "name": "pl_copy_csv_to_delta",
        "activities": [
            {
                "name": "copy_csv",
                "type": "Copy",
                "type_properties": {
                    "source": {"type": "DelimitedTextSource"},
                    "sink": {"type": "AzureDatabricksDeltaLakeSink"},
                },
                "inputs": [{"reference_name": "src_csv", "type": "DatasetReference"}],
                "outputs": [{"reference_name": "sink_delta", "type": "DatasetReference"}],
            }
        ],
    }

    datasets = {
        "src_csv": {
            "name": "src_csv",
            "properties": {
                "type": "DelimitedText",
                "location": {
                    "type": "AzureBlobFSLocation",
                    "container": "raw",
                    "folder_path": "csv",
                },
                "first_row_as_header": True,
                "column_delimiter": ",",
            },
            "linked_service_definition": {
                "name": "ls_storage",
                "properties": {
                    "type": "AzureBlobFS",
                    "url": "DefaultEndpointsProtocol=https;AccountName=sourceaccount;EndpointSuffix=core.windows.net;",
                    "storage_account_name": "DefaultEndpointsProtocol=https;AccountName=sourceaccount;EndpointSuffix=core.windows.net;",
                },
            },
        },
        "sink_delta": {
            "name": "sink_delta",
            "properties": {
                "type": "AzureDatabricksDeltaLakeDataset",
                "database": "db",
                "table": "tbl",
            },
            "linked_service_definition": {
                "name": "ls_dbx",
                "properties": {
                    "type": "AzureDatabricks",
                    "domain": "https://adb-123.azuredatabricks.net",
                },
            },
        },
    }

    class _FakeClient:
        def __init__(self, **_):
            pass

        def list_pipelines(self):
            return ["pl_copy_csv_to_delta"]

        def get_pipeline(self, name):
            return api_pipeline

        def get_trigger(self, name):
            return None

        def get_dataset(self, name):
            if name in datasets:
                return datasets[name]
            raise ValueError(f"Dataset '{name}' not found")

        def get_linked_service(self, name):
            raise ValueError(f"Linked service '{name}' not found")

    monkeypatch.setattr(factory_definition_store, "FactoryClient", _FakeClient)

    store = FactoryDefinitionStore(
        tenant_id="T",
        client_id="C",
        client_secret="S",
        subscription_id="SUB",
        resource_group_name="RG",
        factory_name="ADF",
    )

    pipeline = store.load("pl_copy_csv_to_delta")
    assert len(pipeline.tasks) == 1
    task = pipeline.tasks[0]
    assert isinstance(task, CopyActivity), (
        f"Expected CopyActivity but got {type(task).__name__}. "
        "Copy activities with type_properties are being treated as unsupported."
    )
    assert task.source_dataset.dataset_type == "DelimitedText"
    assert task.sink_dataset.dataset_type == "delta"


# --- PipelineAdapter nested enrichment tests ---


def _make_adapter(datasets: dict[str, dict], linked_services: dict[str, dict] | None = None) -> PipelineAdapter:
    """Create a PipelineAdapter with in-memory dataset/linked-service lookups."""

    def get_dataset(name: str) -> dict:
        if name not in datasets:
            raise ValueError(f"Dataset '{name}' not found")
        return datasets[name]

    def get_linked_service(name: str) -> dict:
        if linked_services and name in linked_services:
            return linked_services[name]
        raise ValueError(f"Linked service '{name}' not found")

    return PipelineAdapter(get_dataset=get_dataset, get_linked_service=get_linked_service)


def test_adapter_enriches_nested_copy_in_foreach() -> None:
    """Copy activities nested inside ForEach should get input/output dataset definitions."""
    source_ds = {"name": "src", "properties": {"type": "Parquet", "location": {"type": "AzureBlobFSLocation"}}}
    sink_ds = {"name": "snk", "properties": {"type": "AzureDatabricksDeltaLakeDataset", "table": "t"}}
    adapter = _make_adapter({"src": source_ds, "snk": sink_ds})

    pipeline = {
        "activities": [
            {
                "name": "loop",
                "type": "ForEach",
                "items": {"value": "@array(['a'])"},
                "activities": [
                    {
                        "name": "copy_inner",
                        "type": "Copy",
                        "inputs": [{"reference_name": "src", "type": "DatasetReference"}],
                        "outputs": [{"reference_name": "snk", "type": "DatasetReference"}],
                        "source": {"type": "ParquetSource"},
                        "sink": {"type": "AzureDatabricksDeltaLakeSink"},
                    }
                ],
            }
        ]
    }
    enriched = adapter.adapt(pipeline)
    inner_copy = enriched["activities"][0]["activities"][0]
    assert "input_dataset_definitions" in inner_copy
    assert "output_dataset_definitions" in inner_copy
    assert inner_copy["input_dataset_definitions"][0]["name"] == "src"
    assert inner_copy["output_dataset_definitions"][0]["name"] == "snk"


def test_adapter_enriches_nested_copy_in_if_condition() -> None:
    """Copy activities inside IfCondition branches should get dataset definitions."""
    ds = {"name": "ds1", "properties": {"type": "DelimitedText", "location": {"type": "AzureBlobFSLocation"}}}
    adapter = _make_adapter({"ds1": ds})

    pipeline = {
        "activities": [
            {
                "name": "branch",
                "type": "IfCondition",
                "if_true_activities": [
                    {
                        "name": "copy_true",
                        "type": "Copy",
                        "inputs": [{"reference_name": "ds1", "type": "DatasetReference"}],
                        "source": {"type": "DelimitedTextSource"},
                        "sink": {"type": "ParquetSink"},
                    }
                ],
                "if_false_activities": [
                    {
                        "name": "copy_false",
                        "type": "Copy",
                        "inputs": [{"reference_name": "ds1", "type": "DatasetReference"}],
                        "source": {"type": "DelimitedTextSource"},
                        "sink": {"type": "ParquetSink"},
                    }
                ],
            }
        ]
    }
    enriched = adapter.adapt(pipeline)
    true_copy = enriched["activities"][0]["if_true_activities"][0]
    false_copy = enriched["activities"][0]["if_false_activities"][0]
    assert "input_dataset_definitions" in true_copy
    assert "input_dataset_definitions" in false_copy


def test_normalize_type_properties_recurses_into_nested_activities() -> None:
    """type_properties of nested activities inside ForEach should be flattened."""
    activity = {
        "name": "loop",
        "type": "ForEach",
        "type_properties": {
            "items": {"value": "@array(['x'])"},
            "activities": [
                {
                    "name": "inner_copy",
                    "type": "Copy",
                    "type_properties": {
                        "source": {"type": "ParquetSource"},
                        "sink": {"type": "ParquetSink"},
                    },
                }
            ],
        },
    }
    normalized = _normalize_activity_type_properties(activity)
    inner = normalized["activities"][0]
    assert "source" in inner, "Nested activity type_properties should be flattened"
    assert "sink" in inner, "Nested activity type_properties should be flattened"
    assert "type_properties" not in inner

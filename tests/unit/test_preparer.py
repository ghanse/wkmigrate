"""Unit tests for the preparer layer (workflow and activity preparation)."""

from __future__ import annotations

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    Authentication,
    ColumnMapping,
    CopyActivity,
    ExecutePipelineActivity,
    ForEachActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    WebActivity,
)
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
from wkmigrate.preparers.execute_pipeline_activity_preparer import prepare_execute_pipeline_activity
from wkmigrate.preparers.for_each_activity_preparer import prepare_for_each_activity
from wkmigrate.preparers.lookup_activity_preparer import prepare_lookup_activity
from wkmigrate.preparers.preparer import prepare_workflow
from wkmigrate.preparers.run_job_activity_preparer import prepare_run_job_activity
from wkmigrate.preparers.web_activity_preparer import prepare_web_activity


_CSV_SOURCE = {
    "type": "csv",
    "dataset_name": "my_csv",
    "service_name": "my_blob",
    "storage_account_name": "mystorageacct",
    "container": "raw",
    "folder_path": "data/input",
}

_CSV_SINK = {
    "type": "csv",
    "dataset_name": "my_sink_csv",
    "service_name": "my_blob",
    "storage_account_name": "mystorageacct",
    "container": "curated",
    "folder_path": "data/output",
}


def _make_lookup_activity(name: str = "LookupTest") -> LookupActivity:
    return LookupActivity(
        name=name,
        task_key=name.lower(),
        source_dataset=_CSV_SOURCE,
        source_properties={"type": "csv"},
    )


def _make_web_activity_with_auth() -> WebActivity:
    return WebActivity(
        name="WebCall",
        task_key="web_call",
        url="https://api.example.com",
        method="GET",
        headers={},
        body=None,
        authentication=Authentication(
            auth_type="basic",
            username="admin",
            password_secret_key="admin_password",
        ),
    )


def test_lookup_preparer_default_scope_in_notebook() -> None:
    """prepare_lookup_activity uses DEFAULT_CREDENTIALS_SCOPE when none is supplied."""
    activity = _make_lookup_activity()

    result = prepare_lookup_activity(activity)

    notebook_content = result.notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_lookup_preparer_custom_scope_in_notebook() -> None:
    """prepare_lookup_activity uses the supplied credentials_scope in the notebook."""
    activity = _make_lookup_activity()

    result = prepare_lookup_activity(activity, credentials_scope="custom_vault")

    notebook_content = result.notebooks[0].content
    assert 'scope="custom_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_web_preparer_default_scope_in_notebook() -> None:
    """prepare_web_activity uses DEFAULT_CREDENTIALS_SCOPE when none is supplied."""
    activity = _make_web_activity_with_auth()

    result = prepare_web_activity(activity)

    notebook_content = result.notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_web_preparer_custom_scope_in_notebook() -> None:
    """prepare_web_activity uses the supplied credentials_scope in the notebook."""
    activity = _make_web_activity_with_auth()

    result = prepare_web_activity(activity, credentials_scope="enterprise_vault")

    notebook_content = result.notebooks[0].content
    assert 'scope="enterprise_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_prepare_workflow_default_scope_threads_to_lookup() -> None:
    """prepare_workflow uses DEFAULT_CREDENTIALS_SCOPE in generated notebooks by default."""
    pipeline = _make_pipeline_with_lookup()

    result = prepare_workflow(pipeline)

    notebook_content = result.activities[0].notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_prepare_workflow_custom_scope_threads_to_lookup() -> None:
    """prepare_workflow passes credentials_scope down to activity notebooks."""
    pipeline = _make_pipeline_with_lookup()

    result = prepare_workflow(pipeline, credentials_scope="pipeline_vault")

    notebook_content = result.activities[0].notebooks[0].content
    assert 'scope="pipeline_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_copy_preparer_default_scope_in_notebook() -> None:
    """prepare_copy_activity uses DEFAULT_CREDENTIALS_SCOPE when none is supplied."""
    activity = _make_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    notebook_content = result.notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_copy_preparer_custom_scope_in_notebook() -> None:
    """prepare_copy_activity uses the supplied credentials_scope in the notebook."""
    activity = _make_copy_activity()

    result = prepare_copy_activity(
        activity,
        default_files_to_delta_sinks=None,
        credentials_scope="copy_vault",
    )

    notebook_content = result.notebooks[0].content
    assert 'scope="copy_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_for_each_preparer_default_scope_in_inner_notebook() -> None:
    """prepare_for_each_activity passes DEFAULT_CREDENTIALS_SCOPE to the inner preparer."""
    activity = _make_for_each_with_lookup()

    result = prepare_for_each_activity(activity, default_files_to_delta_sinks=None)

    notebook_content = result.notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_for_each_preparer_custom_scope_in_inner_notebook() -> None:
    """prepare_for_each_activity forwards credentials_scope to the inner activity notebook."""
    activity = _make_for_each_with_lookup()

    result = prepare_for_each_activity(
        activity,
        default_files_to_delta_sinks=None,
        credentials_scope="foreach_vault",
    )

    notebook_content = result.notebooks[0].content
    assert 'scope="foreach_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_run_job_preparer_default_scope_in_inner_notebook() -> None:
    """prepare_run_job_activity passes DEFAULT_CREDENTIALS_SCOPE into the nested workflow."""
    activity = _make_run_job_with_lookup_pipeline()

    result = prepare_run_job_activity(activity, default_files_to_delta_sinks=None)

    assert result.inner_workflow is not None
    notebook_content = result.inner_workflow.activities[0].notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_run_job_preparer_custom_scope_in_inner_notebook() -> None:
    """prepare_run_job_activity forwards credentials_scope into nested prepared notebooks."""
    activity = _make_run_job_with_lookup_pipeline()

    result = prepare_run_job_activity(
        activity,
        default_files_to_delta_sinks=None,
        credentials_scope="nested_job_vault",
    )

    assert result.inner_workflow is not None
    notebook_content = result.inner_workflow.activities[0].notebooks[0].content
    assert 'scope="nested_job_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_workspace_store_uses_default_credentials_scope_when_option_unset(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """With no credentials_scope option, the store still prepares notebooks using the default scope."""
    assert workspace_definition_store.options.get("credentials_scope") is None

    prepared = workspace_definition_store._prepare_workflow(_make_pipeline_with_lookup())

    notebook_content = prepared.activities[0].notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_credentials_scope_option_reflects_set_option(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """After set_option, credentials_scope is readable from options."""
    workspace_definition_store.set_option("credentials_scope", "prod_secrets")

    assert workspace_definition_store.options.get("credentials_scope") == "prod_secrets"


def test_workspace_store_credentials_scope_appears_in_prepared_notebook(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """Configured credentials_scope is reflected in notebook content from _prepare_workflow."""
    workspace_definition_store.set_option("credentials_scope", "store_vault")
    pipeline = _make_pipeline_with_lookup()

    prepared = workspace_definition_store._prepare_workflow(pipeline)

    notebook_content = prepared.activities[0].notebooks[0].content
    assert 'scope="store_vault"' in notebook_content


def test_custom_credentials_scope_flows_to_secret_instructions(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """Custom credentials_scope should appear in SecretInstruction.scope for copy activities."""
    workspace_definition_store.set_option("credentials_scope", "custom_vault")
    pipeline = Pipeline(
        name="test_copy_pipeline",
        tasks=[_make_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    prepared = workspace_definition_store._prepare_workflow(pipeline)

    secrets = prepared.activities[0].secrets
    if secrets:
        for secret in secrets:
            assert secret.scope == "custom_vault", f"Expected scope 'custom_vault' but got '{secret.scope}'"


def test_collect_data_source_secrets_uses_provided_scope() -> None:
    """collect_data_source_secrets should use the provided credentials_scope."""
    from wkmigrate.parsers.dataset_parsers import collect_data_source_secrets

    definition = {
        "type": "abfs",
        "service_name": "my_storage",
        "provider_type": "abfs",
        "storage_account_key": "fake_key",
    }
    secrets = collect_data_source_secrets(definition, credentials_scope="my_scope")

    assert len(secrets) > 0
    for secret in secrets:
        assert secret.scope == "my_scope"


def _make_pipeline_with_lookup() -> Pipeline:
    return Pipeline(
        name="test_pipeline",
        tasks=[_make_lookup_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )


def _make_copy_activity(name: str = "CopyTest") -> CopyActivity:
    return CopyActivity(
        name=name,
        task_key=name.lower(),
        source_dataset=_CSV_SOURCE,
        sink_dataset=_CSV_SINK,
        source_properties={"type": "csv"},
        sink_properties={"type": "csv"},
        column_mapping=[
            ColumnMapping(
                source_column_name="col_a",
                sink_column_name="col_a",
                sink_column_type="string",
            )
        ],
    )


def _make_for_each_with_lookup(name: str = "ForEachTest") -> ForEachActivity:
    return ForEachActivity(
        name=name,
        task_key=name.lower(),
        items_string="@pipeline().parameters.batch_items",
        for_each_task=_make_lookup_activity("InnerLookup"),
    )


def _make_run_job_with_lookup_pipeline(name: str = "RunJobTest") -> RunJobActivity:
    return RunJobActivity(
        name=name,
        task_key=name.lower(),
        pipeline=_make_pipeline_with_lookup(),
    )


def _make_execute_pipeline_with_child(name: str = "ExecPipelineTest") -> ExecutePipelineActivity:
    child_pipeline = Pipeline(
        name="child_pipeline",
        tasks=[_make_lookup_activity("ChildLookup")],
        parameters=None,
        schedule=None,
        tags={},
    )
    return ExecutePipelineActivity(
        name=name,
        task_key=name.lower(),
        pipeline_name="child_pipeline",
        pipeline=child_pipeline,
        parameters={"env": "prod"},
    )


def _make_execute_pipeline_without_child(name: str = "ExecPipelineNoChild") -> ExecutePipelineActivity:
    return ExecutePipelineActivity(
        name=name,
        task_key=name.lower(),
        pipeline_name="unresolved_pipeline",
        pipeline=None,
        parameters={"batch_id": "123"},
    )


def test_execute_pipeline_with_child_produces_inner_workflow() -> None:
    """Resolved child pipeline produces an inner workflow with __INNER_JOB__ placeholder."""
    activity = _make_execute_pipeline_with_child()

    result = prepare_execute_pipeline_activity(activity, default_files_to_delta_sinks=None)

    assert result.inner_workflow is not None
    assert len(result.inner_workflow.activities) == 1
    run_job_task = result.task.get("run_job_task")
    assert isinstance(run_job_task, dict)
    assert run_job_task.get("job_id") == "__INNER_JOB__:child_pipeline"
    assert run_job_task.get("job_parameters") == {"env": "prod"}


def test_execute_pipeline_without_child_produces_placeholder() -> None:
    """Unresolved child pipeline emits a placeholder job_id template."""
    activity = _make_execute_pipeline_without_child()

    result = prepare_execute_pipeline_activity(activity, default_files_to_delta_sinks=None)

    assert result.inner_workflow is None
    run_job_task = result.task.get("run_job_task")
    assert run_job_task is not None
    assert "job_id_for_unresolved_pipeline" in str(run_job_task.get("job_id"))


def test_execute_pipeline_default_scope_in_inner_notebook() -> None:
    """Inner workflow notebooks use DEFAULT_CREDENTIALS_SCOPE by default."""
    activity = _make_execute_pipeline_with_child()

    result = prepare_execute_pipeline_activity(activity, default_files_to_delta_sinks=None)

    assert result.inner_workflow is not None
    notebook_content = result.inner_workflow.activities[0].notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_execute_pipeline_custom_scope_in_inner_notebook() -> None:
    """Custom credentials_scope is forwarded into nested prepared notebooks."""
    activity = _make_execute_pipeline_with_child()

    result = prepare_execute_pipeline_activity(
        activity,
        default_files_to_delta_sinks=None,
        credentials_scope="exec_vault",
    )

    assert result.inner_workflow is not None
    notebook_content = result.inner_workflow.activities[0].notebooks[0].content
    assert 'scope="exec_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


def test_run_job_activity_emits_dict_run_job_task() -> None:
    """RunJobActivity run_job_task is a dict with __INNER_JOB__ job_id placeholder."""
    activity = _make_run_job_with_lookup_pipeline()

    result = prepare_run_job_activity(activity, default_files_to_delta_sinks=None)

    run_job_task = result.task.get("run_job_task")
    assert isinstance(run_job_task, dict), "run_job_task must be a dict, not a plain string"
    assert run_job_task.get("job_id") == f"__INNER_JOB__:{activity.name}"


def test_prepare_workflow_dispatches_execute_pipeline() -> None:
    """prepare_workflow routes ExecutePipelineActivity through the preparer."""
    pipeline = Pipeline(
        name="parent_pipeline",
        tasks=[_make_execute_pipeline_with_child()],
        parameters=None,
        schedule=None,
        tags={},
    )

    result = prepare_workflow(pipeline)

    assert len(result.activities) == 1
    assert result.activities[0].inner_workflow is not None


def test_assign_inner_job_ids_resolves_run_job_activity_placeholder(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """_assign_inner_job_ids resolves __INNER_JOB__: placeholders in RunJobActivity dict format."""
    tasks = [
        {"task_key": "run_inner", "run_job_task": {"job_id": "__INNER_JOB__:child_job"}},
    ]
    job_id_map = {"child_job": 42}

    workspace_definition_store._assign_inner_job_ids(tasks, job_id_map)

    assert tasks[0]["run_job_task"]["job_id"] == 42


def test_assign_inner_job_ids_preserves_job_parameters(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """_assign_inner_job_ids resolves __INNER_JOB__: placeholder while preserving job_parameters."""
    tasks = [
        {
            "task_key": "exec_pipeline",
            "run_job_task": {"job_id": "__INNER_JOB__:child_pipeline", "job_parameters": {"env": "prod"}},
        },
    ]
    job_id_map = {"child_pipeline": 99}

    workspace_definition_store._assign_inner_job_ids(tasks, job_id_map)

    assert tasks[0]["run_job_task"]["job_id"] == 99
    assert tasks[0]["run_job_task"]["job_parameters"] == {"env": "prod"}


def test_assign_inner_job_refs_resolves_dict_placeholder(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """_assign_inner_job_refs replaces __INNER_JOB__: placeholders with bundle resource refs."""
    tasks = [
        {"task_key": "run_inner", "run_job_task": {"job_id": "__INNER_JOB__:child_job"}},
    ]

    workspace_definition_store._assign_inner_job_refs(tasks)

    assert tasks[0]["run_job_task"]["job_id"] == "${resources.jobs.child_job.id}"

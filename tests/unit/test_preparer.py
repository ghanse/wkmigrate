"""Unit tests for the preparer layer (workflow and activity preparation)."""

from __future__ import annotations

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    Authentication,
    ColumnMapping,
    CopyActivity,
    DeleteActivity,
    ForEachActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    WebActivity,
)
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
from wkmigrate.preparers.delete_activity_preparer import prepare_delete_activity
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


def _make_delete_activity(name: str = "DeleteTest") -> DeleteActivity:
    return DeleteActivity(
        name=name,
        task_key=name.lower(),
        dataset_name="StagingDataset",
        folder_path="data/staging",
        recursive=True,
    )


def test_delete_preparer_produces_notebook() -> None:
    """prepare_delete_activity produces a notebook with dbutils.fs.rm."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.notebooks is not None
    assert len(result.notebooks) == 1
    notebook_content = result.notebooks[0].content
    assert "dbutils.fs.rm" in notebook_content
    assert "data/staging" in notebook_content


def test_delete_preparer_notebook_path() -> None:
    """prepare_delete_activity sets the correct notebook path."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.notebooks[0].file_path == "/wkmigrate/delete_activity_notebooks/deletetest"


def test_delete_preparer_task_has_notebook_task() -> None:
    """prepare_delete_activity creates a task with a notebook_task entry."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert "notebook_task" in result.task
    assert result.task["notebook_task"]["notebook_path"] == "/wkmigrate/delete_activity_notebooks/deletetest"


def test_delete_preparer_recursive_flag_in_notebook() -> None:
    """prepare_delete_activity includes the recursive flag in the notebook content."""
    activity = DeleteActivity(
        name="NonRecursiveDelete",
        task_key="non_recursive_delete",
        dataset_name="TestDataset",
        recursive=False,
    )

    result = prepare_delete_activity(activity)

    notebook_content = result.notebooks[0].content
    assert "recurse=False" in notebook_content


def test_delete_preparer_wildcard_in_notebook() -> None:
    """prepare_delete_activity includes wildcard logic in the notebook when wildcard is set."""
    activity = DeleteActivity(
        name="WildcardDelete",
        task_key="wildcard_delete",
        dataset_name="TestDataset",
        wildcard_file_name="*.csv",
        recursive=True,
    )

    result = prepare_delete_activity(activity)

    notebook_content = result.notebooks[0].content
    assert "fnmatch" in notebook_content
    assert "*.csv" in notebook_content


def test_prepare_workflow_dispatches_delete_activity() -> None:
    """prepare_workflow correctly dispatches a DeleteActivity."""
    pipeline = Pipeline(
        name="test_delete_pipeline",
        tasks=[_make_delete_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    result = prepare_workflow(pipeline)

    assert len(result.activities) == 1
    assert result.activities[0].notebooks is not None
    assert "dbutils.fs.rm" in result.activities[0].notebooks[0].content

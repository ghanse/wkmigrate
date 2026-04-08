"""Unit tests for the preparer layer (workflow and activity preparation)."""

from __future__ import annotations

import warnings

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
from wkmigrate.not_translatable import NotTranslatableWarning
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
    """Notebook contains dbutils.fs.rm and the resolved folder path."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.notebooks is not None
    assert len(result.notebooks) == 1
    notebook_content = result.notebooks[0].content
    assert "dbutils.fs.rm" in notebook_content
    assert "data/staging" in notebook_content


def test_delete_preparer_notebook_path() -> None:
    """Notebook artifact path uses the activity task_key."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.notebooks[0].file_path == "/wkmigrate/delete_activity_notebooks/deletetest"


def test_delete_preparer_task_has_notebook_task() -> None:
    """Task payload includes notebook_task pointing to the generated notebook."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert "notebook_task" in result.task
    assert result.task["notebook_task"]["notebook_path"] == "/wkmigrate/delete_activity_notebooks/deletetest"


def test_delete_preparer_recursive_flag_in_notebook() -> None:
    """Notebook respects recursive=False in the dbutils.fs.rm call."""
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
    """Notebook uses fnmatch filtering when wildcard_file_name is set."""
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


def test_delete_preparer_wildcard_folder_only() -> None:
    """Notebook filters folders by wildcard_folder_path when wildcard_file_name is absent."""
    activity = DeleteActivity(
        name="FolderWildcardDelete",
        task_key="folder_wildcard_delete",
        dataset_name="TestDataset",
        folder_path="data/archive",
        recursive=True,
        wildcard_folder_path="2023-*",
    )

    result = prepare_delete_activity(activity)

    notebook_content = result.notebooks[0].content
    assert "fnmatch" in notebook_content
    assert "2023-*" in notebook_content
    assert "folders = dbutils.fs.ls(path)" in notebook_content
    assert "fnmatch.fnmatch(folder.name, wildcard_folder_path)" in notebook_content
    assert "dbutils.fs.rm(folder.path" in notebook_content


def test_delete_preparer_two_level_wildcard() -> None:
    """Notebook uses two-level listing when both wildcard_folder_path and wildcard_file_name are set."""
    activity = DeleteActivity(
        name="TwoLevelDelete",
        task_key="two_level_delete",
        dataset_name="TestDataset",
        folder_path="data/raw",
        recursive=True,
        wildcard_file_name="*.csv",
        wildcard_folder_path="data/raw/*",
    )

    result = prepare_delete_activity(activity)

    notebook_content = result.notebooks[0].content
    assert "fnmatch" in notebook_content
    assert "wildcard_folder_path" in notebook_content
    assert "wildcard_file_name" in notebook_content
    assert "folders = dbutils.fs.ls(path)" in notebook_content
    assert "files = dbutils.fs.ls(folder.path)" in notebook_content
    assert "fnmatch.fnmatch(folder.name, wildcard_folder_path)" in notebook_content
    assert "fnmatch.fnmatch(f.name, wildcard_file_name)" in notebook_content


def test_delete_preparer_missing_folder_path_emits_warning() -> None:
    """Unresolved folder_path emits NotTranslatableWarning and inserts a TODO placeholder."""
    activity = DeleteActivity(
        name="NoPathDelete",
        task_key="no_path_delete",
        dataset_name="StagingDataset",
        recursive=True,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = prepare_delete_activity(activity)

    warning_messages = [w for w in caught if issubclass(w.category, NotTranslatableWarning)]
    assert len(warning_messages) == 1
    assert "StagingDataset" in str(warning_messages[0].message)
    assert "could not be resolved" in str(warning_messages[0].message)

    notebook_content = result.notebooks[0].content
    assert "TODO" in notebook_content
    assert "UNRESOLVED_PATH_FOR_StagingDataset" in notebook_content


def test_prepare_workflow_dispatches_delete_activity() -> None:
    """prepare_workflow routes DeleteActivity to prepare_delete_activity."""
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


# --- setup_tasks tests ---


def test_delete_preparer_produces_setup_task_with_folder_path() -> None:
    """When folder_path is set, a setup task creating a UC external volume is returned."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.setup_tasks is not None
    assert len(result.setup_tasks) == 1
    setup = result.setup_tasks[0]
    assert "setup_volume_" in setup.task["task_key"]
    assert setup.notebooks is not None
    assert "CREATE EXTERNAL VOLUME IF NOT EXISTS" in setup.notebooks[0].content


def test_delete_preparer_no_setup_task_without_folder_path() -> None:
    """When folder_path is None, no setup tasks are emitted."""
    activity = DeleteActivity(
        name="NoPathDelete",
        task_key="no_path_delete",
        dataset_name="StagingDataset",
        recursive=True,
    )

    result = prepare_delete_activity(activity)

    assert result.setup_tasks is None


def test_delete_preparer_setup_task_uses_custom_catalog_schema() -> None:
    """Setup task respects provided catalog and schema."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity, catalog="prod_catalog", schema="prod_schema")

    assert result.setup_tasks is not None
    notebook_content = result.setup_tasks[0].notebooks[0].content
    assert "prod_catalog" in notebook_content
    assert "prod_schema" in notebook_content


def test_delete_preparer_setup_task_defaults_catalog_schema() -> None:
    """Setup task falls back to default catalog/schema when not provided."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.setup_tasks is not None
    notebook_content = result.setup_tasks[0].notebooks[0].content
    assert "'main'" in notebook_content
    assert "'default'" in notebook_content


def test_delete_preparer_setup_notebook_path() -> None:
    """Setup notebook uses a distinct path under /wkmigrate/setup_notebooks/."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.setup_tasks is not None
    assert result.setup_tasks[0].notebooks[0].file_path == "/wkmigrate/setup_notebooks/create_volume_deletetest"


def test_prepare_workflow_collects_setup_tasks() -> None:
    """prepare_workflow aggregates setup_tasks from individual activities."""
    pipeline = Pipeline(
        name="test_delete_pipeline",
        tasks=[_make_delete_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    result = prepare_workflow(pipeline)

    assert result.setup_tasks is not None
    assert len(result.setup_tasks) == 1
    assert "CREATE EXTERNAL VOLUME IF NOT EXISTS" in result.setup_tasks[0].notebooks[0].content


def test_prepare_workflow_no_setup_tasks_without_delete() -> None:
    """prepare_workflow returns None for setup_tasks when no activities produce them."""
    pipeline = _make_pipeline_with_lookup()

    result = prepare_workflow(pipeline)

    assert result.setup_tasks is None


def test_all_setup_tasks_property_includes_activity_level() -> None:
    """PreparedWorkflow.all_setup_tasks includes setup tasks from individual activities."""
    activity = _make_delete_activity()

    pipeline = Pipeline(
        name="test_pipeline",
        tasks=[activity],
        parameters=None,
        schedule=None,
        tags={},
    )
    workflow = prepare_workflow(pipeline)

    assert len(workflow.all_setup_tasks) >= 1
    assert any("CREATE EXTERNAL VOLUME" in st.notebooks[0].content for st in workflow.all_setup_tasks if st.notebooks)


def test_setup_task_notebook_is_idempotent_sql() -> None:
    """The setup notebook uses IF NOT EXISTS so it is safe to rerun."""
    activity = _make_delete_activity()

    result = prepare_delete_activity(activity)

    assert result.setup_tasks is not None
    content = result.setup_tasks[0].notebooks[0].content
    assert "IF NOT EXISTS" in content

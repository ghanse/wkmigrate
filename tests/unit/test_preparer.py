"""Unit tests for the preparer layer (workflow and activity preparation)."""

from __future__ import annotations

import pytest

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    Authentication,
    ColumnMapping,
    CopyActivity,
    ForEachActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    WebActivity,
)
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
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


# --- Lakeflow Connect managed ingestion tests ---

_SQL_SOURCE = {
    "type": "sqlserver",
    "dataset_name": "my_sql_table",
    "service_name": "my_sql_server",
    "host": "myserver.database.windows.net",
    "database": "mydb",
    "schema_name": "dbo",
    "table_name": "customers",
    "user_name": "admin",
    "authentication_type": "sql",
    "dbtable": "dbo.customers",
}

_DELTA_SINK = {
    "type": "delta",
    "dataset_name": "my_delta_table",
    "service_name": "my_databricks",
    "database_name": "default",
    "table_name": "customers",
}


def _make_sql_to_delta_copy_activity(name: str = "CopySqlToDelta") -> CopyActivity:
    return CopyActivity(
        name=name,
        task_key=name.lower(),
        source_dataset=_SQL_SOURCE,
        sink_dataset=_DELTA_SINK,
        source_properties={"type": "sqlserver"},
        sink_properties={"type": "delta"},
        column_mapping=[
            ColumnMapping(
                source_column_name="id",
                sink_column_name="id",
                sink_column_type="int",
            ),
            ColumnMapping(
                source_column_name="name",
                sink_column_name="name",
                sink_column_type="string",
            ),
        ],
    )


def test_copy_preparer_lakeflow_connect_sql_to_delta_produces_managed_ingestion() -> None:
    """When use_lakeflow_connect=True with SQL source and Delta sink, a managed ingestion pipeline is created."""
    activity = _make_sql_to_delta_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.managed_ingestion_pipelines is not None
    assert len(result.managed_ingestion_pipelines) == 1
    instruction = result.managed_ingestion_pipelines[0]
    assert instruction.source_type == "sqlserver"
    assert instruction.source_host == "myserver.database.windows.net"
    assert instruction.source_database == "mydb"
    assert instruction.source_schema == "dbo"
    assert instruction.source_table == "customers"
    assert instruction.connection_name == "my_sql_server"
    assert instruction.sink_catalog == "wkmigrate"
    assert instruction.sink_schema == "default"
    assert instruction.sink_table == "customers"
    assert instruction.pipeline_name == "copysqltodelta_lakeflow_connect"
    assert result.setup_task is not None
    assert "connection_setup" in result.setup_task["task_key"]


def test_copy_preparer_lakeflow_connect_sql_to_delta_has_setup_notebook() -> None:
    """Managed ingestion produces a setup notebook artifact."""
    activity = _make_sql_to_delta_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.notebooks is not None
    assert len(result.notebooks) == 1
    notebook = result.notebooks[0]
    assert "lakeflow_connect" in notebook.file_path
    assert "Lakeflow Connect" in notebook.content
    assert "myserver.database.windows.net" in notebook.content


def test_copy_preparer_lakeflow_connect_sql_to_delta_has_pipeline_task() -> None:
    """Managed ingestion creates a pipeline_task in the prepared task."""
    activity = _make_sql_to_delta_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert "pipeline_task" in result.task
    assert result.pipelines is None  # No DLT pipeline, only managed ingestion


def test_copy_preparer_lakeflow_connect_false_uses_standard_copy() -> None:
    """When use_lakeflow_connect=False, SQL-to-Delta produces a standard copy notebook."""
    activity = _make_sql_to_delta_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=False)

    assert result.managed_ingestion_pipelines is None
    assert result.notebooks is not None
    assert "copy_data_notebooks" in result.notebooks[0].file_path


def test_copy_preparer_lakeflow_connect_csv_source_not_eligible() -> None:
    """When use_lakeflow_connect=True but source is CSV, standard copy is used."""
    activity = _make_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.managed_ingestion_pipelines is None
    assert result.notebooks is not None


def test_managed_ingestion_instruction_ingestion_source_type() -> None:
    """ingestion_source_type maps lowercase source_type to SDK enum values."""
    activity = _make_sql_to_delta_copy_activity()
    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)
    instruction = result.managed_ingestion_pipelines[0]

    assert instruction.ingestion_source_type == "SQLSERVER"


def test_managed_ingestion_instruction_to_configuration_dict() -> None:
    """to_configuration_dict returns expected wkmigrate.* metadata keys."""
    activity = _make_sql_to_delta_copy_activity()
    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)
    instruction = result.managed_ingestion_pipelines[0]

    config = instruction.to_configuration_dict()
    assert config["wkmigrate.source.type"] == "sqlserver"
    assert config["wkmigrate.source.host"] == "myserver.database.windows.net"
    assert config["wkmigrate.sink.catalog"] == "wkmigrate"


@pytest.mark.parametrize(
    "source_type,source_host,service_name,expected_ingestion_source_type",
    [
        ("sqlserver", "sql.database.windows.net", "my_sqlserver_svc", "SQLSERVER"),
        ("postgresql", "pg.database.azure.com", "my_pg_svc", "POSTGRESQL"),
        ("mysql", "mysql.database.azure.com", "my_mysql_svc", "MYSQL"),
    ],
    ids=["sqlserver", "postgresql", "mysql"],
)
def test_copy_preparer_lakeflow_connect_parametrized_source_types(
    source_type: str,
    source_host: str,
    service_name: str,
    expected_ingestion_source_type: str,
) -> None:
    """Managed ingestion pipeline is created for all supported source types."""
    source = {
        "type": source_type,
        "dataset_name": f"my_{source_type}_table",
        "service_name": service_name,
        "host": source_host,
        "database": "testdb",
        "schema_name": "public",
        "table_name": "orders",
        "user_name": "admin",
        "authentication_type": "sql",
        "dbtable": "public.orders",
    }
    activity = CopyActivity(
        name=f"Copy{source_type.title()}ToDelta",
        task_key=f"copy_{source_type}_to_delta",
        source_dataset=source,
        sink_dataset=_DELTA_SINK,
        source_properties={"type": source_type},
        sink_properties={"type": "delta"},
        column_mapping=[
            ColumnMapping(source_column_name="id", sink_column_name="id", sink_column_type="int"),
        ],
    )

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.managed_ingestion_pipelines is not None
    assert len(result.managed_ingestion_pipelines) == 1
    instruction = result.managed_ingestion_pipelines[0]
    assert instruction.source_type == source_type
    assert instruction.source_host == source_host
    assert instruction.connection_name == service_name
    assert instruction.ingestion_source_type == expected_ingestion_source_type


def test_workspace_store_use_lakeflow_connect_option_default_is_false(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """By default, use_lakeflow_connect is False."""
    assert workspace_definition_store._effective_use_lakeflow_connect() is False


def test_workspace_store_use_lakeflow_connect_option_can_be_set(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """use_lakeflow_connect can be set via set_option."""
    workspace_definition_store.set_option("use_lakeflow_connect", True)

    assert workspace_definition_store._effective_use_lakeflow_connect() is True


def test_workspace_store_prepare_workflow_lakeflow_connect_produces_managed_ingestion(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """When use_lakeflow_connect is set, _prepare_workflow produces managed ingestion artifacts."""
    workspace_definition_store.set_option("use_lakeflow_connect", True)
    pipeline = Pipeline(
        name="test_sql_to_delta",
        tasks=[_make_sql_to_delta_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    prepared = workspace_definition_store._prepare_workflow(pipeline)

    managed_pipelines = prepared.all_managed_ingestion_pipelines
    assert len(managed_pipelines) == 1
    assert managed_pipelines[0].source_type == "sqlserver"


def test_prepare_workflow_lakeflow_connect_threads_to_copy_activity() -> None:
    """prepare_workflow passes use_lakeflow_connect down to copy activity preparers."""
    pipeline = Pipeline(
        name="test_sql_to_delta",
        tasks=[_make_sql_to_delta_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    result = prepare_workflow(pipeline, use_lakeflow_connect=True)

    managed_pipelines = result.all_managed_ingestion_pipelines
    assert len(managed_pipelines) == 1
    assert managed_pipelines[0].pipeline_name == "copysqltodelta_lakeflow_connect"


def test_for_each_preparer_threads_lakeflow_connect_to_inner_copy() -> None:
    """prepare_for_each_activity passes use_lakeflow_connect to the inner copy activity."""
    inner_copy = _make_sql_to_delta_copy_activity("InnerCopy")
    activity = ForEachActivity(
        name="ForEachWithCopy",
        task_key="for_each_with_copy",
        items_string="@pipeline().parameters.tables",
        for_each_task=inner_copy,
    )

    result = prepare_for_each_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.managed_ingestion_pipelines is not None
    assert len(result.managed_ingestion_pipelines) == 1
    assert result.managed_ingestion_pipelines[0].source_type == "sqlserver"


def test_run_job_preparer_threads_lakeflow_connect_to_inner_workflow() -> None:
    """prepare_run_job_activity passes use_lakeflow_connect into the nested workflow."""
    inner_pipeline = Pipeline(
        name="inner_pipeline",
        tasks=[_make_sql_to_delta_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )
    activity = RunJobActivity(
        name="RunJobWithCopy",
        task_key="run_job_with_copy",
        pipeline=inner_pipeline,
    )

    result = prepare_run_job_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.inner_workflow is not None
    managed = result.inner_workflow.all_managed_ingestion_pipelines
    assert len(managed) == 1
    assert managed[0].source_type == "sqlserver"


# --- Nested ForEach/RunJob propagation tests ---


def test_for_each_lakeflow_connect_propagates_setup_task() -> None:
    """ForEach with inner LC copy produces setup_task on the prepared activity."""
    inner_copy = _make_sql_to_delta_copy_activity("InnerCopySetup")
    activity = ForEachActivity(
        name="ForEachSetup",
        task_key="for_each_setup",
        items_string="@pipeline().parameters.tables",
        for_each_task=inner_copy,
    )

    result = prepare_for_each_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.managed_ingestion_pipelines is not None
    assert result.setup_task is not None
    assert "connection_setup" in result.setup_task["task_key"]


def test_run_job_lakeflow_connect_propagates_setup_tasks_via_inner_workflow() -> None:
    """RunJob with inner LC copy collects setup tasks via inner workflow."""
    inner_pipeline = Pipeline(
        name="inner_pipeline_setup",
        tasks=[_make_sql_to_delta_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )
    activity = RunJobActivity(
        name="RunJobSetup",
        task_key="run_job_setup",
        pipeline=inner_pipeline,
    )

    result = prepare_run_job_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.inner_workflow is not None
    setup_tasks = result.inner_workflow.all_setup_tasks
    assert len(setup_tasks) == 1
    assert "connection_setup" in setup_tasks[0]["task_key"]


def test_prepare_workflow_all_setup_tasks_collects_across_activities() -> None:
    """prepare_workflow collects all_setup_tasks from top-level activities."""
    pipeline = Pipeline(
        name="test_setup_tasks",
        tasks=[_make_sql_to_delta_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    result = prepare_workflow(pipeline, use_lakeflow_connect=True)

    assert len(result.all_setup_tasks) == 1
    assert "connection_setup" in result.all_setup_tasks[0]["task_key"]


# --- Source-type-specific schema defaults ---


def test_mysql_source_schema_defaults_to_empty() -> None:
    """MySQL source schema should default to empty string, not 'dbo'."""
    source = {
        "type": "mysql",
        "dataset_name": "my_mysql_table",
        "service_name": "my_mysql_svc",
        "host": "mysql.example.com",
        "database": "testdb",
        "table_name": "orders",
    }
    activity = CopyActivity(
        name="CopyMysql",
        task_key="copy_mysql",
        source_dataset=source,
        sink_dataset=_DELTA_SINK,
        source_properties={"type": "mysql"},
        sink_properties={"type": "delta"},
        column_mapping=[
            ColumnMapping(source_column_name="id", sink_column_name="id", sink_column_type="int"),
        ],
    )

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    instruction = result.managed_ingestion_pipelines[0]
    assert instruction.source_schema == ""


def test_postgresql_source_schema_defaults_to_public() -> None:
    """PostgreSQL source schema should default to 'public', not 'dbo'."""
    source = {
        "type": "postgresql",
        "dataset_name": "my_pg_table",
        "service_name": "my_pg_svc",
        "host": "pg.example.com",
        "database": "testdb",
        "table_name": "orders",
    }
    activity = CopyActivity(
        name="CopyPg",
        task_key="copy_pg",
        source_dataset=source,
        sink_dataset=_DELTA_SINK,
        source_properties={"type": "postgresql"},
        sink_properties={"type": "delta"},
        column_mapping=[
            ColumnMapping(source_column_name="id", sink_column_name="id", sink_column_type="int"),
        ],
    )

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    instruction = result.managed_ingestion_pipelines[0]
    assert instruction.source_schema == "public"


# --- Validation tests ---


def test_lakeflow_connect_missing_required_fields_raises() -> None:
    """Missing required fields on LC-eligible activities should raise ValueError."""
    source = {
        "type": "sqlserver",
        "dataset_name": "my_sql_table",
        "service_name": "my_sql_server",
        # Missing host, database, table_name
    }
    sink = {
        "type": "delta",
        "dataset_name": "my_delta",
        "service_name": "my_databricks",
        # Missing database_name, table_name
    }
    activity = CopyActivity(
        name="CopyBroken",
        task_key="copy_broken",
        source_dataset=source,
        sink_dataset=sink,
        source_properties={"type": "sqlserver"},
        sink_properties={"type": "delta"},
        column_mapping=None,
    )

    with pytest.raises(ValueError, match="missing required fields"):
        prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)


def test_lakeflow_connect_eligible_skips_column_mapping_validation() -> None:
    """LC-eligible activities should not fail on missing column_mapping."""
    activity = CopyActivity(
        name="CopyNoMapping",
        task_key="copy_no_mapping",
        source_dataset=_SQL_SOURCE,
        sink_dataset=_DELTA_SINK,
        source_properties={"type": "sqlserver"},
        sink_properties={"type": "delta"},
        column_mapping=None,
    )

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    assert result.managed_ingestion_pipelines is not None


# --- WDS catalog/schema option applies to managed ingestion ---


def test_workspace_store_catalog_schema_applies_to_managed_ingestion(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """catalog/schema options should rewrite managed ingestion instruction sink_catalog/sink_schema."""
    workspace_definition_store.set_option("use_lakeflow_connect", True)
    workspace_definition_store.set_option("catalog", "production")
    workspace_definition_store.set_option("schema", "analytics")
    pipeline = Pipeline(
        name="test_mi_options",
        tasks=[_make_sql_to_delta_copy_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )

    prepared = workspace_definition_store._prepare_workflow(pipeline)

    mi = prepared.all_managed_ingestion_pipelines[0]
    assert mi.sink_catalog == "production"
    assert mi.sink_schema == "analytics"


# --- Setup notebook content ---


def test_setup_notebook_contains_create_connection() -> None:
    """Setup notebook should contain CREATE CONNECTION IF NOT EXISTS."""
    activity = _make_sql_to_delta_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None, use_lakeflow_connect=True)

    notebook = result.notebooks[0]
    assert "CREATE CONNECTION IF NOT EXISTS" in notebook.content

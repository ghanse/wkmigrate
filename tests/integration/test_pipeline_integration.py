"""End-to-end integration tests for pipeline translation against a real Azure Data Factory.

These tests deploy ADF resources, read them back through wkmigrate's
``FactoryClient`` and ``FactoryDefinitionStore``, and verify that the
translated IR matches expectations. They require a live Azure subscription
with valid credentials provided via environment variables.

Mark: all tests in this module carry the ``integration`` marker so they can be
run in isolation with ``pytest -m integration``.
"""

from __future__ import annotations

import pytest

from azure.mgmt.datafactory.models import DatasetResource, LinkedServiceResource, PipelineResource

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.models.ir.pipeline import (
    CopyActivity,
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    WebActivity,
)

pytestmark = pytest.mark.integration


def test_list_pipelines(
    factory_client: FactoryClient,
    sample_pipeline: PipelineResource,
) -> None:
    """Listing pipelines returns the deployed test pipeline."""
    names = factory_client.list_pipelines()
    assert "integration_test_pipeline" in names


def test_get_pipeline(
    factory_client: FactoryClient,
    sample_pipeline: PipelineResource,
) -> None:
    """Fetching a pipeline returns a dict with activities."""
    pipeline = factory_client.get_pipeline("integration_test_pipeline")
    assert isinstance(pipeline, dict)
    assert "activities" in pipeline or "properties" in pipeline


def test_get_linked_service(
    factory_client: FactoryClient,
    sample_linked_service: LinkedServiceResource,
) -> None:
    """Fetching a linked service returns a dict with properties."""
    linked_service = factory_client.get_linked_service("test_blob_storage")
    assert isinstance(linked_service, dict)


def test_get_dataset(
    factory_client: FactoryClient,
    sample_dataset: DatasetResource,
) -> None:
    """Fetching a dataset returns a dict with linked-service metadata."""
    dataset = factory_client.get_dataset("test_csv_dataset")
    assert isinstance(dataset, dict)


def test_load_pipeline(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """Loading a deployed pipeline produces a valid ``Pipeline`` IR."""
    result = factory_store.load("integration_test_pipeline")

    assert isinstance(result, Pipeline)
    assert result.name == "integration_test_pipeline"
    assert len(result.tasks) == 2


def test_load_pipeline_activities_are_translated(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """Activities within the loaded pipeline are translated to the correct IR types."""
    result = factory_store.load("integration_test_pipeline")

    task_names = [task.name for task in result.tasks]
    assert "extract_data" in task_names
    assert "transform_data" in task_names

    for task in result.tasks:
        assert isinstance(task, DatabricksNotebookActivity)


def test_load_pipeline_dependencies(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """Dependencies between activities are preserved in translation."""
    result = factory_store.load("integration_test_pipeline")

    transform_task = next(t for t in result.tasks if t.name == "transform_data")
    assert transform_task.depends_on is not None
    assert len(transform_task.depends_on) == 1
    assert transform_task.depends_on[0].task_key == "extract_data"


def test_load_pipeline_parameters(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """Pipeline parameters are preserved in translation."""
    result = factory_store.load("integration_test_pipeline")

    assert result.parameters is not None
    assert len(result.parameters) >= 1


def test_load_pipeline_tags(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """System tags are added to the translated pipeline."""
    result = factory_store.load("integration_test_pipeline")

    assert result.tags is not None
    assert "CREATED_BY_WKMIGRATE" in result.tags


def test_load_foreach_pipeline(
    factory_store: FactoryDefinitionStore,
    sample_foreach_pipeline: PipelineResource,
) -> None:
    """Loading a ForEach pipeline produces the expected control-flow IR."""
    result = factory_store.load("integration_test_foreach_pipeline")

    assert isinstance(result, Pipeline)
    assert len(result.tasks) == 1

    foreach_task = next(
        (t for t in result.tasks if isinstance(t, ForEachActivity)),
        None,
    )
    assert foreach_task is not None
    assert foreach_task.concurrency == 5


def test_load_all(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """``load_all`` translates all pipelines without error."""
    results = factory_store.load_all(pipeline_names=["integration_test_pipeline"])
    assert len(results) == 1
    assert all(isinstance(pipeline, Pipeline) for pipeline in results)


def test_unsupported_activity_creates_placeholder(
    factory_store: FactoryDefinitionStore,
    sample_unsupported_pipeline: PipelineResource,
) -> None:
    """An unsupported activity type produces a placeholder notebook with /UNSUPPORTED_ADF_ACTIVITY."""
    result = factory_store.load("integration_test_unsupported_pipeline")

    assert isinstance(result, Pipeline)
    assert len(result.tasks) == 1

    placeholder = next(
        (t for t in result.tasks if t.name == "unsupported_function_call"),
        None,
    )
    assert placeholder is not None
    assert isinstance(placeholder, DatabricksNotebookActivity)
    assert placeholder.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_unsupported_property_raises_not_translatable_warning(
    factory_store: FactoryDefinitionStore,
    sample_unsupported_pipeline: PipelineResource,
) -> None:
    """A ``secure_input`` policy property populates ``not_translatable`` on the Pipeline IR."""
    result = factory_store.load("integration_test_unsupported_pipeline")

    assert isinstance(result, Pipeline)
    assert len(result.not_translatable) >= 1

    warning_props = [entry.get("property") for entry in result.not_translatable]
    assert "secure_input" in warning_props


def test_notebook_activity_translates(
    factory_store: FactoryDefinitionStore,
    sample_pipeline: PipelineResource,
) -> None:
    """DatabricksNotebook activities translate to ``DatabricksNotebookActivity``."""
    result = factory_store.load("integration_test_pipeline")
    notebook_tasks = [t for t in result.tasks if isinstance(t, DatabricksNotebookActivity)]
    assert len(notebook_tasks) == 2
    assert all(t.notebook_path is not None for t in notebook_tasks)


def test_foreach_activity_translates(
    factory_store: FactoryDefinitionStore,
    sample_foreach_pipeline: PipelineResource,
) -> None:
    """ForEach activities translate to ``ForEachActivity`` with nested tasks."""
    result = factory_store.load("integration_test_foreach_pipeline")
    foreach_tasks = [t for t in result.tasks if isinstance(t, ForEachActivity)]
    assert len(foreach_tasks) == 1
    assert foreach_tasks[0].for_each_task is not None


def test_spark_jar_activity_translates(
    factory_store: FactoryDefinitionStore,
    spark_jar_pipeline: PipelineResource,
) -> None:
    """DatabricksSparkJar activities translate to ``SparkJarActivity``."""
    result = factory_store.load("integration_test_spark_jar_pipeline")

    assert isinstance(result, Pipeline)
    spark_jar_tasks = [t for t in result.tasks if isinstance(t, SparkJarActivity)]
    assert len(spark_jar_tasks) == 1
    assert spark_jar_tasks[0].main_class_name == "com.example.Main"


def test_spark_python_activity_translates(
    factory_store: FactoryDefinitionStore,
    spark_python_pipeline: PipelineResource,
) -> None:
    """DatabricksSparkPython activities translate to ``SparkPythonActivity``."""
    result = factory_store.load("integration_test_spark_python_pipeline")

    assert isinstance(result, Pipeline)
    spark_python_tasks = [t for t in result.tasks if isinstance(t, SparkPythonActivity)]
    assert len(spark_python_tasks) == 1
    assert spark_python_tasks[0].python_file == "dbfs:/scripts/etl.py"


def test_databricks_job_activity_translates(
    factory_store: FactoryDefinitionStore,
    databricks_job_pipeline: PipelineResource,
) -> None:
    """DatabricksJob activities translate to ``RunJobActivity``."""
    result = factory_store.load("integration_test_databricks_job_pipeline")

    assert isinstance(result, Pipeline)
    job_tasks = [t for t in result.tasks if isinstance(t, RunJobActivity)]
    assert len(job_tasks) == 1
    assert job_tasks[0].existing_job_id == "12345"


def test_web_activity_translates(
    factory_store: FactoryDefinitionStore,
    web_activity_pipeline: PipelineResource,
) -> None:
    """WebActivity translates to ``WebActivity`` with correct URL and method."""
    result = factory_store.load("integration_test_web_activity_pipeline")

    assert isinstance(result, Pipeline)
    web_tasks = [t for t in result.tasks if isinstance(t, WebActivity)]
    assert len(web_tasks) == 1
    assert web_tasks[0].url == "https://httpbin.org/get"
    assert web_tasks[0].method == "GET"


def test_lookup_activity_translates(
    factory_store: FactoryDefinitionStore,
    lookup_pipeline: PipelineResource,
) -> None:
    """Lookup activities translate to ``LookupActivity``."""
    result = factory_store.load("integration_test_lookup_pipeline")

    assert isinstance(result, Pipeline)
    lookup_tasks = [t for t in result.tasks if isinstance(t, LookupActivity)]
    assert len(lookup_tasks) == 1
    assert lookup_tasks[0].first_row_only is True


def test_if_condition_activity_translates(
    factory_store: FactoryDefinitionStore,
    if_condition_pipeline: PipelineResource,
) -> None:
    """IfCondition activities translate to ``IfConditionActivity`` with child branches."""
    result = factory_store.load("integration_test_if_condition_pipeline")

    assert isinstance(result, Pipeline)
    if_tasks = [t for t in result.tasks if isinstance(t, IfConditionActivity)]
    assert len(if_tasks) == 1
    assert len(if_tasks[0].child_activities) == 2


def test_set_variable_activity_translates(
    factory_store: FactoryDefinitionStore,
    set_variable_pipeline: PipelineResource,
) -> None:
    """SetVariable activities translate to ``SetVariableActivity``."""
    result = factory_store.load("integration_test_set_variable_pipeline")

    assert isinstance(result, Pipeline)
    set_var_tasks = [t for t in result.tasks if isinstance(t, SetVariableActivity)]
    assert len(set_var_tasks) == 1
    assert set_var_tasks[0].variable_name == "output_path"


def test_copy_abfs_csv_to_parquet(
    factory_store: FactoryDefinitionStore,
    copy_abfs_pipeline: PipelineResource,
) -> None:
    """Copy from ABFS CSV to ABFS Parquet produces a ``CopyActivity`` with valid datasets."""
    result = factory_store.load("integration_test_copy_abfs_pipeline")

    assert isinstance(result, Pipeline)
    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].source_dataset is not None
    assert copy_tasks[0].sink_dataset is not None


def test_copy_s3_to_abfs(
    factory_store: FactoryDefinitionStore,
    copy_s3_pipeline: PipelineResource,
) -> None:
    """Copy from S3 to ABFS produces a ``CopyActivity`` with an S3 source dataset."""
    result = factory_store.load("integration_test_copy_s3_pipeline")

    assert isinstance(result, Pipeline)
    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].source_dataset is not None
    assert copy_tasks[0].sink_dataset is not None


def test_copy_gcs_to_abfs(
    factory_store: FactoryDefinitionStore,
    copy_gcs_pipeline: PipelineResource,
) -> None:
    """Copy from GCS to ABFS produces a ``CopyActivity`` with a GCS source dataset."""
    result = factory_store.load("integration_test_copy_gcs_pipeline")

    assert isinstance(result, Pipeline)
    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].source_dataset is not None
    assert copy_tasks[0].sink_dataset is not None


def test_copy_azure_blob_to_abfs(
    factory_store: FactoryDefinitionStore,
    copy_azure_blob_pipeline: PipelineResource,
) -> None:
    """Copy from Azure Blob to ABFS produces a ``CopyActivity`` with a Blob source dataset."""
    result = factory_store.load("integration_test_copy_azure_blob_pipeline")

    assert isinstance(result, Pipeline)
    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].source_dataset is not None
    assert copy_tasks[0].sink_dataset is not None


def test_copy_sql_to_abfs(
    factory_store: FactoryDefinitionStore,
    copy_sql_pipeline: PipelineResource,
) -> None:
    """Copy from Azure SQL to ABFS produces a ``CopyActivity`` with a SQL source dataset."""
    result = factory_store.load("integration_test_copy_sql_pipeline")

    assert isinstance(result, Pipeline)
    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].source_dataset is not None
    assert copy_tasks[0].sink_dataset is not None

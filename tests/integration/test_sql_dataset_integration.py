"""Integration tests for SQL dataset translation.

These tests verify that SQL dataset translators produce the expected IR types
when processing real ADF pipeline definitions containing SQL copy activities.
They require a live Azure subscription with valid credentials.

Mark: all tests in this module carry the ``integration`` marker so they can be
run in isolation with ``pytest -m integration``.
"""

from __future__ import annotations

import pytest

from azure.mgmt.datafactory.models import PipelineResource

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.models.ir.datasets import SqlTableDataset
from wkmigrate.models.ir.pipeline import CopyActivity, Pipeline

pytestmark = pytest.mark.integration


def test_sql_dataset_translates_to_sql_table(
    factory_store: FactoryDefinitionStore,
    copy_sql_pipeline: PipelineResource,
) -> None:
    """SQL datasets produce SqlTableDataset IR."""
    result = factory_store.load("integration_test_copy_sql_pipeline")

    assert isinstance(result, Pipeline)
    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].source_dataset is not None
    assert isinstance(copy_tasks[0].source_dataset, SqlTableDataset)


def test_sql_source_dataset_has_sqlserver_type(
    factory_store: FactoryDefinitionStore,
    copy_sql_pipeline: PipelineResource,
) -> None:
    """SQL source dataset has dataset_type 'sqlserver'."""
    result = factory_store.load("integration_test_copy_sql_pipeline")

    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    source = copy_tasks[0].source_dataset
    assert source is not None
    assert source.dataset_type == "sqlserver"


def test_sql_copy_activity_has_sink_dataset(
    factory_store: FactoryDefinitionStore,
    copy_sql_pipeline: PipelineResource,
) -> None:
    """SQL copy activity has a valid sink dataset."""
    result = factory_store.load("integration_test_copy_sql_pipeline")

    copy_tasks = [t for t in result.tasks if isinstance(t, CopyActivity)]
    assert len(copy_tasks) == 1
    assert copy_tasks[0].sink_dataset is not None

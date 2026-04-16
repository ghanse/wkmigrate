"""Translator for Delta table dataset definitions.

This module normalizes Delta table dataset payloads into ``DeltaTableDataset``
objects, parsing Databricks linked-service metadata and table properties.
"""

from wkmigrate.models.ir.datasets import DeltaTableDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators.utils import get_linked_service_definition
from wkmigrate.supported_types import translates_dataset
from wkmigrate.translators.linked_service_translators import translate_databricks_cluster_spec


@translates_dataset("AzureDatabricksDeltaLakeDataset")
def translate_delta_table_dataset(dataset: dict) -> DeltaTableDataset | UnsupportedValue:
    """
    Translates a Delta table dataset definition into a ``DeltaTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Delta table dataset as a ``DeltaTableDataset`` object.
    """
    linked_service_definition = get_linked_service_definition(dataset)
    if isinstance(linked_service_definition, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service_definition.message)

    linked_service = translate_databricks_cluster_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dataset_properties = dataset.get("properties", {})
    return DeltaTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type="delta",
        database_name=dataset_properties.get("database"),
        table_name=dataset_properties.get("table"),
        catalog_name=dataset_properties.get("catalog"),
        service_name=linked_service.service_name,
    )

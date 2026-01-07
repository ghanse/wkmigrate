"""Enumerations describing JSON pipeline sources."""

from enum import StrEnum


class JSONSourceType(StrEnum):
    """
    Supported JSON definition sources for definition stores.

    Valid options:
        * ``DATABRICKS_WORKFLOW``: JSON files that contain Databricks Jobs or Workflows definitions.
        * ``DATA_FACTORY_PIPELINE``: JSON files that contain Azure Data Factory pipeline definitions.
    """

    DATABRICKS_WORKFLOW = "DATABRICKS_WORKFLOW"
    DATA_FACTORY_PIPELINE = "DATA_FACTORY_PIPELINE"

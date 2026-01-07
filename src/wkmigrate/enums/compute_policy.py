"""Enumerations for supported compute policy values."""

from enum import StrEnum


class ComputePolicy(StrEnum):
    """
    Preferred compute policy for notebook tasks.

    Valid options:
        * ``USE_SERVERLESS``: Run tasks on Databricks serverless compute when available.
        * ``USE_CLASSIC``: Run tasks on classic (non-serverless) compute clusters.
    """

    USE_SERVERLESS = "USE_SERVERLESS"
    USE_CLASSIC = "USE_CLASSIC"

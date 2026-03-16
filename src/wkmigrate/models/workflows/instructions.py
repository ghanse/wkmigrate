"""This module defines representational classes for Databricks workflow instructions."""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(slots=True)
class PipelineInstruction:
    """
    Represents a declarative pipeline that must be created.

    Attributes:
        task_ref: Reference to the Databricks task dictionary that will consume the pipeline.
        file_path: Workspace path where the pipeline's notebook or script is stored.
        name: Name to assign to the Databricks pipeline.
        catalog: Unity Catalog name for the pipeline target. Defaults to ``\"wkmigrate\"``.
        target: Schema (database) name for the pipeline target. Defaults to ``\"wkmigrate\"``.
    """

    task_ref: dict
    file_path: str
    name: str
    catalog: str = 'wkmigrate'
    target: str = 'wkmigrate'

    @property
    def local_identifier(self) -> str:
        """Returns the local identifier for the pipeline."""
        base = self.name or "pipeline"
        return f"{base}_local_pipeline"


@dataclass(slots=True)
class SecretInstruction:
    """
    Represents a secret value that must exist in Databricks.

    Attributes:
        scope: Name of the Databricks secret scope that will store the secret.
        key: Secret key name within the scope.
        service_name: Logical source system or service associated with the secret.
        service_type: Type of backing service (for example ``sqlserver`` or ``csv``).
        provided_value: Secret value obtained from source metadata, if available.
    """

    scope: str
    key: str
    service_name: str | None
    service_type: str | None
    provided_value: str | None

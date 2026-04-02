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


_SOURCE_TYPE_MAP: dict[str, str] = {
    "sqlserver": "SQLSERVER",
    "postgresql": "POSTGRESQL",
    "mysql": "MYSQL",
}


@dataclass(slots=True)
class ManagedIngestionInstruction:
    """
    Represents a Lakeflow Connect managed ingestion pipeline to be created.

    When a Copy activity reads from a supported SQL database and writes to a
    Delta Lake table, the activity can be replaced by a Lakeflow Connect
    managed ingestion pipeline instead of a notebook-based copy.

    Attributes:
        task_ref: Reference to the Databricks task dictionary that will consume the pipeline.
        pipeline_name: Name to assign to the managed ingestion pipeline.
        connection_name: Databricks connection name derived from the source linked service.
        source_type: Source database type (for example ``sqlserver``, ``postgresql``, ``mysql``).
        source_host: Hostname of the source database server.
        source_database: Database name on the source server.
        source_schema: Schema name within the source database.
        source_table: Table name within the source schema.
        sink_catalog: Unity Catalog name for the target Delta table.
        sink_schema: Schema (database) name for the target Delta table.
        sink_table: Table name for the target Delta table.
    """

    task_ref: dict
    pipeline_name: str
    connection_name: str
    source_type: str
    source_host: str
    source_database: str
    source_schema: str
    source_table: str
    sink_catalog: str
    sink_schema: str
    sink_table: str

    @property
    def ingestion_source_type(self) -> str:
        """Returns the SDK ``IngestionSourceType`` string for this source."""
        return _SOURCE_TYPE_MAP.get(self.source_type, self.source_type.upper())

    def to_configuration_dict(self) -> dict[str, str]:
        """Returns the pipeline configuration metadata dict for diagnostics and tracing."""
        return {
            "wkmigrate.source.type": self.source_type,
            "wkmigrate.source.host": self.source_host,
            "wkmigrate.source.database": self.source_database,
            "wkmigrate.source.schema": self.source_schema,
            "wkmigrate.source.table": self.source_table,
            "wkmigrate.sink.catalog": self.sink_catalog,
            "wkmigrate.sink.schema": self.sink_schema,
            "wkmigrate.sink.table": self.sink_table,
        }

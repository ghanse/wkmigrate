"""This module defines internal representations for datasets.

Datasets in this module represent the source and sink datasets used by various pipeline activities
(e.g. Copy Data, Lookup Value). Each dataset contains metadata about the dataset's type, name, and
parameters. Datasets are translated from ADF payloads into internal representations that can be used
to generate Databricks Lakeflow jobs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Dataset:
    """
    Base class representing a parsed dataset.

    Attributes:
        dataset_name: Logical name of the dataset as defined in ADF.
        dataset_type: Normalized dataset type (for example ``csv``, ``delta``, ``sqlserver``).
        service_name: Name of the backing service or linked service associated with this dataset.
    """

    dataset_name: str
    dataset_type: str
    service_name: str | None = None


@dataclass
class FileDataset(Dataset):
    """
    Dataset definition for file-based sources and sinks in an ABFS/ADLS storage account.

    Attributes:
        container: Storage container name hosting the files.
        folder_path: Directory or path prefix inside the container.
        storage_account_name: Storage account name for ABFS/ADLS access.
        url: Fully qualified URL to the storage endpoint, when available.
        format_options: File-format specific options (delimiter, header flags, compression, and so on).
        records_per_file: Maximum number of rows per file when writing out data.
    """

    container: str | None = None
    folder_path: str | None = None
    storage_account_name: str | None = None
    url: str | None = None
    format_options: dict[str, Any] = field(default_factory=dict)
    records_per_file: int | None = None


@dataclass
class DeltaTableDataset(Dataset):
    """
    Dataset definition for Delta tables accessible from a Databricks cluster.

    Attributes:
        database_name: Name of the Hive/Unity Catalog database containing the table.
        table_name: Name of the Delta table within the database.
        catalog_name: Catalog name when using Unity Catalog.
    """

    database_name: str | None = None
    table_name: str | None = None
    catalog_name: str | None = None


@dataclass
class SqlTableDataset(Dataset):
    """
    Dataset definition for JDBC-accessible tables in a relational database.

    Attributes:
        schema_name: Database schema that contains the table.
        table_name: Table name referenced by the dataset.
        dbtable: Fully qualified ``schema.table`` name used by JDBC.
        host: Hostname or address of the database server.
        database: Logical database name within the server.
        user_name: Username used for JDBC authentication.
        authentication_type: Authentication mechanism used by the connection.
        connection_options: Additional JDBC options (for example fetch size, partitioning).
    """

    schema_name: str | None = None
    table_name: str | None = None
    dbtable: str | None = None
    host: str | None = None
    database: str | None = None
    user_name: str | None = None
    authentication_type: str | None = None
    connection_options: dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetProperties:
    """
    Container for dataset property metadata produced during parsing.

    Attributes:
        dataset_type: Normalized dataset type string matching the associated ``Dataset``.
        options: Dictionary of format- or connection-specific options derived from ADF properties.
    """

    dataset_type: str
    options: dict[str, Any] = field(default_factory=dict)

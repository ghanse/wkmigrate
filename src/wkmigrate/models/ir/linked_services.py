"""Linked service IR models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class LinkedService:
    """
    Base class representing a translated linked service.

    Attributes:
        service_name: Logical name of the linked service in ADF.
        service_type: ADF linked-service type string (for example ``AzureSqlDatabase`` or ``AzureDatabricks``).
    """

    service_name: str
    service_type: str


@dataclass
class SqlLinkedService(LinkedService):
    """
    Linked-service metadata for SQL/JDBC connections to a relational database.

    Attributes:
        host: Hostname or address of the database server.
        database: Logical database name within the server.
        user_name: Username used to authenticate with the database.
        authentication_type: Authentication mechanism (for example ``SQL`` or ``AAD``).
    """

    host: str
    database: str
    user_name: str | None = None
    authentication_type: str | None = None


@dataclass
class AbfsLinkedService(LinkedService):
    """
    Linked-service metadata for ABFS/ADLS storage accounts.

    Attributes:
        storage_account_name: Storage account name for the ABFS endpoint.
        url: Fully qualified base URL for the storage account.
    """

    storage_account_name: str | None = None
    url: str | None = None


@dataclass
class DatabricksClusterLinkedService(LinkedService):
    """
    Linked-service metadata describing a Databricks workspace/cluster.

    Attributes:
        host_name: Databricks workspace hostname (for example ``adb-<id>.<region>.azuredatabricks.net``).
        node_type_id: Default worker node type identifier.
        spark_version: Runtime version string for the cluster.
        custom_tags: Custom cluster tags applied at creation time.
        driver_node_type_id: Node type for the driver, when different from workers.
        spark_conf: Spark configuration dictionary applied to the cluster.
        spark_env_vars: Environment variables made available to Spark.
        init_scripts: List of init script descriptors attached to the cluster.
        cluster_log_conf: Cluster log configuration dictionary.
        autoscale: Autoscaling configuration specifying ``min_workers`` and ``max_workers``.
        num_workers: Fixed number of workers when autoscaling is disabled.
        pat: Personal access token used for workspace authentication, when applicable.
    """

    host_name: str | None = None
    node_type_id: str | None = None
    spark_version: str | None = None
    custom_tags: dict[str, str] | None = None
    driver_node_type_id: str | None = None
    spark_conf: dict[str, Any] | None = None
    spark_env_vars: dict[str, str] | None = None
    init_scripts: list[dict[str, Any]] | None = None
    cluster_log_conf: dict[str, Any] | None = None
    autoscale: dict[str, int] | None = None
    num_workers: int | None = None
    pat: str | None = None


@dataclass
class UnsupportedLinkedService(LinkedService):
    """
    IR representation for a linked service that cannot be translated.

    Attributes:
        message: Description of why the linked service is unsupported.
        adf_definition: Raw ADF linked-service payload that could not be parsed.
    """

    message: str | None = None
    adf_definition: dict[str, Any] | None = None

"""This module defines translators for Azure Data Factory linked service definitions.

Translators in this module normalize linked service payloads into internal representations.
Translators must validate required fields, coerce connection settings, and emit an ``UnsupportedValue``
objects for any unparsable inputs.
"""

from uuid import uuid4
from wkmigrate.enums.init_script_type import InitScriptType
from wkmigrate.utils import append_system_tags, extract_group
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.models.ir.linked_services import AbfsLinkedService, DatabricksClusterLinkedService, SqlLinkedService


def translate_abfs_spec(abfs_spec: dict) -> AbfsLinkedService | UnsupportedValue:
    """
    Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

    Args:
        abfs_spec: Linked-service definition from Azure Data Factory.

    Returns:
        ABFS linked-service metadata as a ``AbfsLinkedService`` object.
    """
    if not abfs_spec:
        return UnsupportedValue(value=abfs_spec, message="Missing ABFS linked service definition")

    properties = abfs_spec.get("properties", {})
    url = _parse_storage_account_connection_string(properties.get("url"))

    if isinstance(url, UnsupportedValue):
        return UnsupportedValue(
            value=abfs_spec, message=f"Invalid property 'url' in ABFS linked service definition; {url.message}"
        )

    storage_account_name = _parse_storage_account_name(properties.get("storage_account_name"))
    if isinstance(storage_account_name, UnsupportedValue):
        return UnsupportedValue(
            value=abfs_spec,
            message=f"Invalid property 'storage_account_name' in ABFS linked service definition; {storage_account_name.message}",
        )

    return AbfsLinkedService(
        service_name=abfs_spec.get("name", str(uuid4())),
        service_type="abfs",
        url=url,
        storage_account_name=storage_account_name,
    )


def translate_databricks_cluster_spec(cluster_spec: dict) -> DatabricksClusterLinkedService | UnsupportedValue:
    """
    Parses a Databricks linked service definition into a ``DatabricksClusterLinkedService`` object.

    Args:
        cluster_spec: Linked-service definition from Azure Data Factory.

    Returns:
        Databricks cluster linked-service metadata as a ``DatabricksClusterLinkedService`` object.
    """
    if not cluster_spec:
        return UnsupportedValue(value=cluster_spec, message="Missing Databricks linked service definition")

    properties = cluster_spec.get("properties", {})

    autoscale = _parse_autoscale_policy(properties.get("new_cluster_num_of_worker"))
    if isinstance(autoscale, UnsupportedValue):
        return UnsupportedValue(value=cluster_spec, message=autoscale.message)

    num_workers = _parse_number_of_workers(properties.get("new_cluster_num_of_worker"))
    if isinstance(num_workers, UnsupportedValue):
        return UnsupportedValue(value=cluster_spec, message=num_workers.message)

    return DatabricksClusterLinkedService(
        service_name=cluster_spec.get("name", str(uuid4())),
        service_type="databricks",
        host_name=properties.get("domain"),
        node_type_id=properties.get("new_cluster_node_type"),
        spark_version=properties.get("new_cluster_version"),
        custom_tags=append_system_tags(properties.get("new_cluster_custom_tags", {})),
        driver_node_type_id=properties.get("new_cluster_driver_node_type"),
        spark_conf=properties.get("new_cluster_spark_conf"),
        spark_env_vars=properties.get("new_cluster_spark_env_vars"),
        init_scripts=_parse_init_scripts(properties.get("new_cluster_init_scripts", [])),
        cluster_log_conf=_parse_log_conf(properties.get("new_cluster_log_destination")),
        autoscale=autoscale,
        num_workers=num_workers,
        pat=properties.get("pat"),
    )


def translate_sql_server_spec(sql_server_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses a SQL Server linked service definition into an ``SqlLinkedService`` object.

    Args:
        sql_server_spec: Linked-service definition from Azure Data Factory.

    Returns:
        SQL Server linked-service metadata as a ``SqlLinkedService`` object.
    """
    if not sql_server_spec:
        return UnsupportedValue(value=sql_server_spec, message="Missing SQL Server linked service definition")

    properties = sql_server_spec.get("properties", {})
    return SqlLinkedService(
        service_name=sql_server_spec.get("name", str(uuid4())),
        service_type="sqlserver",
        host=properties.get("server"),
        database=properties.get("database"),
        user_name=properties.get("user_name"),
        authentication_type=properties.get("authentication_type"),
    )


def _parse_log_conf(cluster_log_destination: str | None) -> dict | None:
    """
    Parses a cluster log configuration from a DBFS destination into a dictionary of log settings.

    Args:
        cluster_log_destination: Cluster log delivery path in DBFS.

    Returns:
        Cluster log configuration as a ``dict``.
    """
    if cluster_log_destination is None:
        return None
    return {"dbfs": {"destination": cluster_log_destination}}


def _parse_number_of_workers(num_workers: str | None) -> int | UnsupportedValue | None:
    """
    Parses a static cluster size from the linked-service payload into an integer.

    Args:
        num_workers: Number of workers, represented as a string.

    Returns:
        Parsed worker count as an ``int``, or ``None`` if autoscaling is used.
    """
    if num_workers is None or ":" in num_workers:
        return None
    try:
        return int(num_workers)
    except ValueError:
        return UnsupportedValue(value=num_workers, message=f"Invalid number of workers '{num_workers}'")


def _parse_autoscale_policy(autoscale_policy: str | None) -> dict | UnsupportedValue | None:
    """
    Parses a Databricks autoscaling policy (e.g., ``"1:4"``) into a dictionary of worker counts.

    Args:
        autoscale_policy: Autoscaling range encoded as ``"min:max"``.

    Returns:
        Dictionary with ``min_workers`` / ``max_workers`` as a ``dict``.
    """
    if autoscale_policy is None:
        return None
    try:
        autoscale_num_workers = autoscale_policy.split(":")
        return {
            "min_workers": int(autoscale_num_workers[0]),
            "max_workers": int(autoscale_num_workers[1]),
        }
    except ValueError:
        return UnsupportedValue(value=autoscale_policy, message=f"Invalid autoscale policy '{autoscale_policy}'")


def _parse_init_scripts(init_scripts: list[str] | None) -> list[dict] | None:
    """
    Parses the init-script list included in a linked-service definition into a list of init script definitions.

    Args:
        init_scripts: Paths to init scripts.

    Returns:
        List of init script definitions as a ``list[dict]``.
    """
    if init_scripts is None:
        return None
    return [
        {_get_init_script_type(init_script_path=init_script): {"destination": init_script}}
        for init_script in init_scripts
    ]


def _get_init_script_type(init_script_path: str) -> str:
    """
    Determines the init script type from its path prefix.

    Args:
        init_script_path: Init script path string.

    Returns:
        Init script type as a ``str``.
    """
    if init_script_path.startswith("dbfs:"):
        return InitScriptType.DBFS.value
    if init_script_path.startswith("/Volumes"):
        return InitScriptType.VOLUMES.value
    return InitScriptType.WORKSPACE.value


def _parse_storage_account_connection_string(connection_string: str) -> str | UnsupportedValue:
    """
    Parses an Azure Storage account connection string into a URL.

    Args:
        connection_string: Azure Storage connection string.

    Returns:
        Blob endpoint URL extracted from the connection string as a ``str``.
    """
    account_name = extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")
    protocol = extract_group(connection_string, r"DefaultEndpointsProtocol=([a-zA-Z0-9]+);")
    suffix = extract_group(connection_string, r"EndpointSuffix=([a-zA-Z0-9\.]+);")

    if isinstance(account_name, UnsupportedValue):
        return UnsupportedValue(
            value=connection_string,
            message=f"Could not parse Storage Account name from connection string '{connection_string}'",
        )
    if isinstance(protocol, UnsupportedValue):
        return UnsupportedValue(
            value=connection_string, message=f"Could not parse Protocol from connection string '{connection_string}'"
        )
    if isinstance(suffix, UnsupportedValue):
        return UnsupportedValue(
            value=connection_string, message=f"Could not parse Suffix from connection string '{connection_string}'"
        )
    return f"{protocol}://{account_name}.blob.{suffix}/"


def _parse_storage_account_name(connection_string: str) -> str | UnsupportedValue:
    """
    Parses the storage account name from a connection string into a string.

    Args:
        connection_string: Azure Storage connection string.

    Returns:
        Storage account name as a ``str``.
    """
    return extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")

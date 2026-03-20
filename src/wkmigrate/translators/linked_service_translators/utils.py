"""Shared helpers for linked-service translators.

These utilities are used by multiple linked-service translators to parse
connection strings, cluster configurations, and init-script definitions.
"""

from wkmigrate.enums.init_script_type import InitScriptType
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import extract_group


def parse_log_conf(cluster_log_destination: str | None) -> dict | None:
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


def parse_number_of_workers(num_workers: str | None) -> int | dict[str, int] | UnsupportedValue | None:
    """
    Parses a static cluster size from the linked-service payload into an integer.

    Args:
        num_workers: Number of workers, represented as a string.

    Returns:
        Parsed worker count as an ``int``, or ``None`` if autoscaling is used.
    """
    if num_workers is None:
        return None
    try:
        if ":" in num_workers:
            return {
                "min_workers": int(num_workers.split(":")[0]),
                "max_workers": int(num_workers.split(":")[1]),
            }
        return int(num_workers)
    except ValueError:
        return UnsupportedValue(value=num_workers, message=f"Invalid number of workers '{num_workers}'")


def parse_init_scripts(init_scripts: list[str] | None) -> list[dict] | None:
    """
    Parses the init-script list included in a linked-service definition into a list of init script definitions.

    Args:
        init_scripts: Paths to init scripts.

    Returns:
        List of init script definitions as a ``list[dict]``.
    """
    if not init_scripts:
        return None
    return [
        {_get_init_script_type(init_script_path=init_script): {"destination": init_script}}
        for init_script in init_scripts
    ]


def parse_storage_account_connection_string(connection_string: str) -> str | UnsupportedValue:
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


def parse_storage_account_name(connection_string: str) -> str | UnsupportedValue:
    """
    Parses the storage account name from a connection string into a string.

    Args:
        connection_string: Azure Storage connection string.

    Returns:
        Storage account name as a ``str``.
    """
    return extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")


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

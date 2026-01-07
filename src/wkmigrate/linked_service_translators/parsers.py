"""Helpers for parsing Databricks linked-service payloads."""

import re
from wkmigrate.enums.init_script_type import InitScriptType


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


def parse_number_of_workers(num_workers: str | None) -> int | None:
    """
    Parses a static cluster size from the linked-service payload into an integer.

    Args:
        num_workers: Number of workers, represented as a string.

    Returns:
        Parsed worker count as an ``int``, or ``None`` if autoscaling is used.
    """
    if num_workers is None or ":" in num_workers:
        return None
    return int(num_workers)


def parse_autoscale_policy(autoscale_policy: str | None) -> dict | None:
    """
    Parses a Databricks autoscaling policy (e.g., ``"1:4"``) into a dictionary of worker counts.

    Args:
        autoscale_policy: Autoscaling range encoded as ``"min:max"``.

    Returns:
        Dictionary with ``min_workers`` / ``max_workers`` as a ``dict``.
    """
    if autoscale_policy is None or ":" not in autoscale_policy:
        return None
    autoscale_num_workers = autoscale_policy.split(":")
    return {
        "min_workers": int(autoscale_num_workers[0]),
        "max_workers": int(autoscale_num_workers[1]),
    }


def parse_init_scripts(init_scripts: list[str] | None) -> list[dict] | None:
    """
    Parses the init-script list included in a linked-service definition into a list of init script definitions.

    Args:
        init_scripts: Paths to init scripts.

    Returns:
        List of init script definitions as a ``list[dict]``.
    """
    if init_scripts is None or len(init_scripts) == 0:
        return None
    return [
        {_get_init_script_type(init_script_path=init_script): {"destination": init_script}}
        for init_script in init_scripts
    ]


def parse_storage_account_connection_string(connection_string: str) -> str:
    """
    Parses an Azure Storage account connection string into a URL.

    Args:
        connection_string: Azure Storage connection string.

    Returns:
        Blob endpoint URL extracted from the connection string as a ``str``.
    """
    account_name = _extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")
    protocol = _extract_group(connection_string, r"DefaultEndpointsProtocol=([a-zA-Z0-9]+);")
    suffix = _extract_group(connection_string, r"EndpointSuffix=([a-zA-Z0-9\.]+);")
    return f"{protocol}://{account_name}.blob.{suffix}/"


def parse_storage_account_name(connection_string: str) -> str:
    """
    Parses the storage account name from a connection string into a string.

    Args:
        connection_string: Azure Storage connection string.

    Returns:
        Storage account name as a ``str``.
    """
    return _extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")


def _extract_group(input_string: str, regex: str) -> str:
    """
    Extracts a regex group from an input string.

    Args:
        input_string: Input string to search.
        regex: Regex pattern to match.

    Returns:
        Extracted group as a ``str``.
    """
    match = re.search(pattern=regex, string=input_string)
    if match is None:
        raise ValueError(f"No match for regex '{regex}' found in input string '{input_string}'")
    return match.group(1)


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

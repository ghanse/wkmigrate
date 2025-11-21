"""This module defines methods for parsing cluster spec objects to the Databricks SDK's object model."""

import re
from wkmigrate.enums.init_script_type import InitScriptType


def parse_log_conf(cluster_log_destination: str | None) -> dict | None:
    """Parses a cluster log configuration from a path to DBFS.
    :parameter: cluster_log_destination: Cluster log delivery path in DBFS as an ``str``
    :return: Cluster log configuration as a ``dict``
    """
    if cluster_log_destination is None:
        return None
    return {"dbfs": {"destination": cluster_log_destination}}


def parse_number_of_workers(num_workers: str | None) -> int | None:
    """Parses a cluster number of workers from a string (e.g. '4').
    :parameter num_workers: Number of workers as a ``str``
    :return: Number of workers as an ``int``
    """
    if num_workers is None or ":" in num_workers:
        return None
    return int(num_workers)


def parse_autoscale_policy(autoscale_policy: str | None) -> dict | None:
    """Parses a cluster autoscale policy from a string (e.g. '1:4').
    :parameter autoscale_policy: Autoscale policy as a ``str``
    :return: Min and max worker counts as a ``tuple[int, int]``
    """
    if autoscale_policy is None or ":" not in autoscale_policy:
        return None
    autoscale_num_workers = autoscale_policy.split(":")
    return {
        "min_workers": int(autoscale_num_workers[0]),
        "max_workers": int(autoscale_num_workers[1]),
    }


def parse_init_scripts(init_scripts: list[str] | None) -> list[dict] | None:
    """Parses a list of cluster init script definitions to the Databricks SDK's object model.
    :parameter init_scripts: List of init script paths
    :return: List of init script objects with ``type`` and ``destination`` properties
    """
    if init_scripts is None or len(init_scripts) == 0:
        return None
    return [
        {_get_init_script_type(init_script_path=init_script): {"destination": init_script}}
        for init_script in init_scripts
    ]


def parse_storage_account_connection_string(connection_string: str) -> str:
    """Parses an Azure Storage account connection string.
    :parameter connection_string: Azure Storage account connection string as a ``str``
    :return: Azure Storage account URL as a ``str``
    """
    account_name = _extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")
    protocol = _extract_group(connection_string, r"DefaultEndpointsProtocol=([a-zA-Z0-9]+);")
    suffix = _extract_group(connection_string, r"EndpointSuffix=([a-zA-Z0-9\.]+);")
    return f"{protocol}://{account_name}.blob.{suffix}/"


def parse_storage_account_name(connection_string: str) -> str:
    """Parses an Azure Storage account name from the URL.
    :parameter connection_string: Azure Storage account URL as a ``str``
    :return: Azure Storage account name as a ``str``
    """
    return _extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")


def _extract_group(input_string: str, regex: str) -> str:
    match = re.search(pattern=regex, string=input_string)
    if match is None:
        raise ValueError(f"No match for regex '{regex}' found in input string '{input_string}'")
    return match.group(1)


def _get_init_script_type(init_script_path: str) -> str:
    """Gets an init script type from the path.
    :parameter: init_script_path: Init script path as a ``str``
    :return: Init script type as a ``str``
    """
    if init_script_path.startswith("dbfs:"):
        return InitScriptType.DBFS.value
    if init_script_path.startswith("/Volumes"):
        return InitScriptType.VOLUMES.value
    return InitScriptType.WORKSPACE.value

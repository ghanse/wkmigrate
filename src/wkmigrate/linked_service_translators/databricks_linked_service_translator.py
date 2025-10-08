"""This module defines methods for translating Databricks cluster services from data pipeline definitions."""

from wkmigrate.linked_service_translators.parsers import (
    parse_autoscale_policy,
    parse_init_scripts,
    parse_log_conf,
    parse_number_of_workers,
)
from wkmigrate.utils import append_system_tags, identity, translate


mapping = {
    "host_name": {"key": "domain", "parser": identity},
    "node_type_id": {"key": "new_cluster_node_type", "parser": identity},
    "spark_version": {"key": "new_cluster_version", "parser": identity},
    "custom_tags": {"key": "new_cluster_custom_tags", "parser": append_system_tags},
    "driver_node_type_id": {"key": "new_cluster_driver_node_type", "parser": identity},
    "spark_conf": {"key": "new_cluster_spark_conf", "parser": identity},
    "spark_env_vars": {"key": "new_cluster_spark_env_vars", "parser": identity},
    "init_scripts": {"key": "new_cluster_init_scripts", "parser": parse_init_scripts},
    "cluster_log_conf": {
        "key": "new_cluster_log_destination",
        "parser": parse_log_conf,
    },
    "autoscale": {"key": "new_cluster_num_of_worker", "parser": parse_autoscale_policy},
    "num_workers": {
        "key": "new_cluster_num_of_worker",
        "parser": parse_number_of_workers,
    },
    "pat": {"key": "pat", "parser": identity},
}


def translate_cluster_spec(cluster_spec: dict | None) -> dict | None:
    """Translates a Databricks linked service in Data Factory's object model to a Databricks cluster
    specification in the Databricks SDK object model.
    :parameter cluster_spec: Databricks linked service definition as a ``dict``
    :return: Databricks cluster specification as a ``dict``
    """
    if cluster_spec is None:
        return None
    # Get the cluster properties:
    properties = cluster_spec.get("properties")
    # Translate the properties:
    translated_properties = translate(properties, mapping)
    result = {"service_name": cluster_spec.get("name")}
    if translated_properties is not None:
        result.update(translated_properties)
    return result

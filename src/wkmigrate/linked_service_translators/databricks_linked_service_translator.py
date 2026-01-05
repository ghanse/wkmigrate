"""This module defines methods for translating Databricks cluster services from data pipeline definitions."""

from wkmigrate.linked_service_translators.parsers import (
    parse_autoscale_policy,
    parse_init_scripts,
    parse_log_conf,
    parse_number_of_workers,
)
from wkmigrate.models.ir.linked_services import DatabricksClusterLinkedService
from wkmigrate.utils import append_system_tags


def translate_cluster_spec(cluster_spec: dict) -> DatabricksClusterLinkedService:
    """
    Parses a Databricks linked service definition into a ``DatabricksClusterLinkedService`` object.

    Args:
        cluster_spec: Linked-service definition from Azure Data Factory.

    Returns:
        Databricks cluster linked-service metadata as a ``DatabricksClusterLinkedService`` object.
    """
    properties = cluster_spec.get("properties", {})
    return DatabricksClusterLinkedService(
        service_name=cluster_spec.get("name", ""),
        service_type="databricks",
        host_name=properties.get("host_name", ""),
        node_type_id=properties.get("node_type_id", ""),
        spark_version=properties.get("spark_version", ""),
        custom_tags=append_system_tags(properties.get("custom_tags", {})),
        driver_node_type_id=properties.get("driver_node_type_id", ""),
        spark_conf=properties.get("spark_conf", {}),
        spark_env_vars=properties.get("spark_env_vars", {}),
        init_scripts=parse_init_scripts(properties.get("init_scripts", [])),
        cluster_log_conf=parse_log_conf(properties.get("cluster_log_conf", "")),
        autoscale=parse_autoscale_policy(properties.get("autoscale", "")),
        num_workers=parse_number_of_workers(properties.get("num_workers", 0)),
        pat=properties.get("pat", ""),
    )

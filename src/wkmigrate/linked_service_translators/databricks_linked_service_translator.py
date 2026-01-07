"""This module defines methods for translating Databricks cluster services from data pipeline definitions."""

from uuid import uuid4
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
    if not cluster_spec:
        raise ValueError("Missing Databricks linked service definition")
    properties = cluster_spec.get("properties", {})
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
        init_scripts=parse_init_scripts(properties.get("new_cluster_init_scripts", [])),
        cluster_log_conf=parse_log_conf(properties.get("new_cluster_log_destination")),
        autoscale=parse_autoscale_policy(properties.get("new_cluster_num_of_worker")),
        num_workers=parse_number_of_workers(properties.get("new_cluster_num_of_worker")),
        pat=properties.get("pat"),
    )

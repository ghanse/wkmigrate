"""Translator for Databricks cluster linked service definitions.

This module normalizes Databricks linked-service payloads into
``DatabricksClusterLinkedService`` objects, parsing cluster sizing,
init scripts, log configuration, and Spark settings.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import DatabricksClusterLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.linked_service_translators.utils import (
    parse_init_scripts,
    parse_log_conf,
    parse_number_of_workers,
)
from wkmigrate.utils import append_system_tags


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

    num_workers = parse_number_of_workers(properties.get("new_cluster_num_of_worker"))
    if isinstance(num_workers, UnsupportedValue):
        return UnsupportedValue(value=cluster_spec, message=num_workers.message)

    autoscale_size = num_workers if isinstance(num_workers, dict) else None
    fixed_size = num_workers if isinstance(num_workers, int) else None

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
        autoscale=autoscale_size,
        num_workers=fixed_size,
        pat=properties.get("pat"),
    )

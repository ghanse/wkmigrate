from contextlib import nullcontext as does_not_raise
import pytest
from wkmigrate.linked_service_translators.databricks_linked_service_translator import (
    translate_cluster_spec,
)
from wkmigrate.linked_service_translators.sql_server_linked_service_translator import (
    translate_sql_server_spec,
)
from wkmigrate.models.ir.linked_services import DatabricksClusterLinkedService, SqlLinkedService


class TestLinkedServiceTranslator:
    """Unit tests for linked service translator methods."""

    @pytest.mark.parametrize(
        "linked_service_definition, expected_result, context",
        [
            (None, None, pytest.raises(ValueError, match="Missing Databricks linked service definition")),
            ({}, None, pytest.raises(ValueError, match="Missing Databricks linked service definition")),
            (
                {
                    "name": "databricks-linked-service",
                    "properties": {
                        "domain": "mydomain.databricks.com",
                        "new_cluster_node_type": "Standard_DS3_v2",
                        "new_cluster_version": "7.3.x-scala2.12",
                        "new_cluster_custom_tags": {"env": "test"},
                        "new_cluster_driver_node_type": "Standard_DS4_v2",
                        "new_cluster_spark_conf": {"spark.executor.memory": "4g"},
                        "new_cluster_spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                        "new_cluster_init_scripts": [
                            "/Users/test@databricks.com/init_scripts/init.sh",
                            "dbfs:/FileStore/init_scripts/init.sh",
                            "/Volumes/test/init_scripts/init.sh",
                        ],
                        "new_cluster_log_destination": "dbfs:/cluster-logs",
                        "new_cluster_num_of_worker": "2:8",
                    },
                },
                DatabricksClusterLinkedService(
                    service_name="databricks-linked-service",
                    service_type="databricks",
                    host_name="mydomain.databricks.com",
                    node_type_id="Standard_DS3_v2",
                    spark_version="7.3.x-scala2.12",
                    custom_tags={"env": "test", "CREATED_BY_WKMIGRATE": ""},
                    driver_node_type_id="Standard_DS4_v2",
                    spark_conf={"spark.executor.memory": "4g"},
                    spark_env_vars={"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                    init_scripts=[
                        {"workspace": {"destination": "/Users/test@databricks.com/init_scripts/init.sh"}},
                        {"dbfs": {"destination": "dbfs:/FileStore/init_scripts/init.sh"}},
                        {"volumes": {"destination": "/Volumes/test/init_scripts/init.sh"}},
                    ],
                    cluster_log_conf={"dbfs": {"destination": "dbfs:/cluster-logs"}},
                    autoscale={"min_workers": 2, "max_workers": 8},
                ),
                does_not_raise(),
            ),
            (
                {
                    "name": "databricks-linked-service",
                    "properties": {
                        "domain": "mydomain.databricks.com",
                        "new_cluster_node_type": "Standard_DS3_v2",
                        "new_cluster_version": "7.3.x-scala2.12",
                        "new_cluster_num_of_worker": "4",
                    },
                },
                DatabricksClusterLinkedService(
                    service_name="databricks-linked-service",
                    service_type="databricks",
                    host_name="mydomain.databricks.com",
                    node_type_id="Standard_DS3_v2",
                    spark_version="7.3.x-scala2.12",
                    num_workers=4,
                    custom_tags={"CREATED_BY_WKMIGRATE": ""},
                ),
                does_not_raise(),
            ),
            (
                {
                    "name": "databricks-linked-service",
                    "properties": {
                        "domain": "mydomain.databricks.com",
                        "new_cluster_node_type": "Standard_DS3_v2",
                        "new_cluster_version": "7.3.x-scala2.12",
                        "new_cluster_num_of_worker": "1:4",
                    },
                },
                DatabricksClusterLinkedService(
                    service_name="databricks-linked-service",
                    service_type="databricks",
                    host_name="mydomain.databricks.com",
                    node_type_id="Standard_DS3_v2",
                    spark_version="7.3.x-scala2.12",
                    autoscale={"min_workers": 1, "max_workers": 4},
                    custom_tags={"CREATED_BY_WKMIGRATE": ""},
                ),
                does_not_raise(),
            ),
        ],
    )
    def test_translate_cluster_spec_parses_result(self, linked_service_definition, expected_result, context):
        with context:
            result = translate_cluster_spec(linked_service_definition)
            assert result == expected_result

    @pytest.mark.parametrize(
        "linked_service_definition, expected_result, context",
        [
            (None, None, pytest.raises(ValueError, match="Missing SQL Server linked service definition")),
            ({}, None, pytest.raises(ValueError, match="Missing SQL Server linked service definition")),
            (
                {
                    "name": "sql-linked-service",
                    "properties": {
                        "server": "myserver.database.windows.net",
                        "database": "mydatabase",
                        "user_name": "admin",
                        "authentication_type": "SQL Authentication",
                    },
                },
                SqlLinkedService(
                    service_name="sql-linked-service",
                    service_type="sqlserver",
                    host="myserver.database.windows.net",
                    database="mydatabase",
                    user_name="admin",
                    authentication_type="SQL Authentication",
                ),
                does_not_raise(),
            ),
            (
                {
                    "name": "sql-linked-service",
                    "properties": {
                        "server": "myserver.database.windows.net",
                        "database": "mydatabase",
                    },
                },
                SqlLinkedService(
                    service_name="sql-linked-service",
                    service_type="sqlserver",
                    host="myserver.database.windows.net",
                    database="mydatabase",
                ),
                does_not_raise(),
            ),
        ],
    )
    def test_translate_sql_server_spec_parses_result(self, linked_service_definition, expected_result, context):
        with context:
            result = translate_sql_server_spec(linked_service_definition)
            assert result == expected_result

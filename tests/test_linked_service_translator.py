"""Parametrized tests for linked service translator methods."""

from contextlib import nullcontext as does_not_raise

import pytest

from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.linked_service_translators import (
    translate_azure_blob_spec,
    translate_databricks_cluster_spec,
    translate_gcs_spec,
    translate_s3_spec,
    translate_sql_server_spec,
)
from wkmigrate.models.ir.linked_services import (
    AzureBlobLinkedService,
    DatabricksClusterLinkedService,
    GcsLinkedService,
    S3LinkedService,
    SqlLinkedService,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue


@pytest.mark.parametrize(
    "linked_service_definition, expected_result, context",
    [
        (
            None,
            UnsupportedValue(value=None, message="Missing Databricks linked service definition"),
            does_not_raise(),
        ),
        (
            {},
            UnsupportedValue(value={}, message="Missing Databricks linked service definition"),
            does_not_raise(),
        ),
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
def test_translate_cluster_spec_parses_result(linked_service_definition, expected_result, context):
    with context:
        result = translate_databricks_cluster_spec(linked_service_definition)
        assert result == expected_result


@pytest.mark.parametrize(
    "linked_service_definition, expected_result, context",
    [
        (
            None,
            UnsupportedValue(value=None, message="Missing SQL Server linked service definition"),
            does_not_raise(),
        ),
        (
            {},
            UnsupportedValue(value={}, message="Missing SQL Server linked service definition"),
            does_not_raise(),
        ),
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
def test_translate_sql_server_spec_parses_result(linked_service_definition, expected_result, context):
    with context:
        result = translate_sql_server_spec(linked_service_definition)
        assert result == expected_result


@pytest.mark.parametrize(
    "linked_service_definition, expected_result, context",
    [
        (
            None,
            UnsupportedValue(value=None, message="Missing S3 linked service definition"),
            does_not_raise(),
        ),
        (
            {},
            UnsupportedValue(value={}, message="Missing S3 linked service definition"),
            does_not_raise(),
        ),
        (
            {
                "name": "s3-linked-service",
                "properties": {
                    "access_key_id": "MY_ACCESS_KEY_ID",
                    "service_url": "https://s3.amazonaws.com",
                },
            },
            S3LinkedService(
                service_name="s3-linked-service",
                service_type="s3",
                access_key_id="MY_ACCESS_KEY_ID",
                service_url="https://s3.amazonaws.com",
            ),
            does_not_raise(),
        ),
        (
            {
                "name": "s3-minimal",
                "properties": {},
            },
            S3LinkedService(
                service_name="s3-minimal",
                service_type="s3",
            ),
            does_not_raise(),
        ),
    ],
)
def test_translate_s3_spec_parses_result(linked_service_definition, expected_result, context):
    with context:
        result = translate_s3_spec(linked_service_definition)
        assert result == expected_result


@pytest.mark.parametrize(
    "linked_service_definition, expected_result, context",
    [
        (
            None,
            UnsupportedValue(value=None, message="Missing GCS linked service definition"),
            does_not_raise(),
        ),
        (
            {},
            UnsupportedValue(value={}, message="Missing GCS linked service definition"),
            does_not_raise(),
        ),
        (
            {
                "name": "gcs-linked-service",
                "properties": {
                    "access_key_id": "my-gcs-key",
                    "service_url": "https://storage.googleapis.com",
                },
            },
            GcsLinkedService(
                service_name="gcs-linked-service",
                service_type="gcs",
                access_key_id="my-gcs-key",
                service_url="https://storage.googleapis.com",
            ),
            does_not_raise(),
        ),
        (
            {
                "name": "gcs-minimal",
                "properties": {},
            },
            GcsLinkedService(
                service_name="gcs-minimal",
                service_type="gcs",
            ),
            does_not_raise(),
        ),
    ],
)
def test_translate_gcs_spec_parses_result(linked_service_definition, expected_result, context):
    with context:
        result = translate_gcs_spec(linked_service_definition)
        assert result == expected_result


@pytest.mark.parametrize(
    "linked_service_definition, expected_result, context",
    [
        (
            None,
            UnsupportedValue(value=None, message="Missing Azure Blob linked service definition"),
            does_not_raise(),
        ),
        (
            {},
            UnsupportedValue(value={}, message="Missing Azure Blob linked service definition"),
            does_not_raise(),
        ),
        (
            {
                "name": "blob-linked-service",
                "properties": {
                    "connection_string": (
                        "DefaultEndpointsProtocol=https;AccountName=myblobaccount;" "EndpointSuffix=core.windows.net;"
                    ),
                    "storage_account_name": "myblobaccount",
                },
            },
            AzureBlobLinkedService(
                service_name="blob-linked-service",
                service_type="azure_blob",
                url="https://myblobaccount.blob.core.windows.net/",
                storage_account_name="myblobaccount",
            ),
            does_not_raise(),
        ),
        (
            {
                "name": "blob-sas",
                "properties": {
                    "sas_uri": "https://myaccount.blob.core.windows.net/?sv=2021-06-08&sig=abc",
                },
            },
            AzureBlobLinkedService(
                service_name="blob-sas",
                service_type="azure_blob",
                url="https://myaccount.blob.core.windows.net/?sv=2021-06-08&sig=abc",
            ),
            does_not_raise(),
        ),
        (
            {
                "name": "blob-anon",
                "properties": {
                    "authentication_type": "Anonymous",
                    "container_url": "https://myaccount.blob.core.windows.net/public-data",
                },
            },
            AzureBlobLinkedService(
                service_name="blob-anon",
                service_type="azure_blob",
                url="https://myaccount.blob.core.windows.net/public-data",
            ),
            does_not_raise(),
        ),
        (
            {
                "name": "blob-anon-missing-url",
                "properties": {
                    "authentication_type": "Anonymous",
                },
            },
            UnsupportedValue(
                value={
                    "name": "blob-anon-missing-url",
                    "properties": {
                        "authentication_type": "Anonymous",
                    },
                },
                message="Missing property 'container_url' in storage account properties",
            ),
            does_not_raise(),
        ),
        (
            {
                "name": "blob-no-conn",
                "properties": {},
            },
            UnsupportedValue(
                value={
                    "name": "blob-no-conn",
                    "properties": {},
                },
                message="Missing property 'container_url', 'sas_uri', or 'service_endpoint'"
                " in storage account properties",
            ),
            does_not_raise(),
        ),
    ],
)
def test_translate_azure_blob_spec_parses_result(linked_service_definition, expected_result, context):
    with context:
        result = translate_azure_blob_spec(linked_service_definition)
        assert result == expected_result


def test_translate_azure_blob_spec_service_endpoint_warns():
    """Test that service_endpoint auth emits a NotTranslatableWarning."""
    spec = {
        "name": "blob-endpoint",
        "properties": {
            "service_endpoint": "https://myaccount.blob.core.windows.net/",
        },
    }

    with pytest.warns(
        NotTranslatableWarning,
        match="Cannot use service principal or managed identity authentication",
    ):
        result = translate_azure_blob_spec(spec)

    assert isinstance(result, AzureBlobLinkedService)
    assert result.url == "https://myaccount.blob.core.windows.net/"
    assert result.service_name == "blob-endpoint"

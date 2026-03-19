"""Comprehensive tests for linked service translators using JSON fixtures.

This module tests all linked service translators against realistic ADF payloads
loaded from JSON fixture files. Tests cover Databricks cluster configurations,
SQL Server connections, ABFS storage accounts, and cloud file services (S3, GCS, ADLS).
"""

from __future__ import annotations

from tests.conftest import get_fixture
from wkmigrate.models.ir.linked_services import (
    AbfsLinkedService,
    AzureBlobLinkedService,
    DatabricksClusterLinkedService,
    GcsLinkedService,
    S3LinkedService,
    SqlLinkedService,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.linked_service_translators import (
    translate_abfs_spec,
    translate_azure_blob_spec,
    translate_databricks_cluster_spec,
    translate_gcs_spec,
    translate_s3_spec,
    translate_sql_server_spec,
)


def test_autoscaling_cluster(linked_service_fixtures: list[dict]) -> None:
    """Test translation of Databricks linked service with autoscaling."""
    fixture = get_fixture(linked_service_fixtures, "databricks_autoscaling")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.host_name == fixture["expected"]["host_name"]
    assert result.node_type_id == fixture["expected"]["node_type_id"]
    assert result.spark_version == fixture["expected"]["spark_version"]
    assert result.autoscale == fixture["expected"]["autoscale"]
    assert result.driver_node_type_id == fixture["expected"]["driver_node_type_id"]


def test_autoscaling_cluster_custom_tags(linked_service_fixtures: list[dict]) -> None:
    """Test that custom tags include system tags."""
    fixture = get_fixture(linked_service_fixtures, "databricks_autoscaling")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert "CREATED_BY_WKMIGRATE" in result.custom_tags
    assert result.custom_tags["environment"] == "production"


def test_autoscaling_cluster_spark_config(linked_service_fixtures: list[dict]) -> None:
    """Test that Spark configuration is preserved."""
    fixture = get_fixture(linked_service_fixtures, "databricks_autoscaling")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.spark_conf == fixture["expected"]["spark_conf"]
    assert result.spark_env_vars == fixture["expected"]["spark_env_vars"]


def test_autoscaling_cluster_init_scripts(linked_service_fixtures: list[dict]) -> None:
    """Test that init scripts are parsed with correct types."""
    fixture = get_fixture(linked_service_fixtures, "databricks_autoscaling")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.init_scripts is not None
    assert len(result.init_scripts) == 3

    # Workspace init script
    assert "workspace" in result.init_scripts[0]
    # DBFS init script
    assert "dbfs" in result.init_scripts[1]
    # Volumes init script
    assert "volumes" in result.init_scripts[2]


def test_autoscaling_cluster_log_conf(linked_service_fixtures: list[dict]) -> None:
    """Test that cluster log configuration is preserved."""
    fixture = get_fixture(linked_service_fixtures, "databricks_autoscaling")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.cluster_log_conf == fixture["expected"]["cluster_log_conf"]


def test_fixed_cluster_size(linked_service_fixtures: list[dict]) -> None:
    """Test translation of Databricks linked service with fixed cluster size."""
    fixture = get_fixture(linked_service_fixtures, "databricks_fixed")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.num_workers == fixture["expected"]["num_workers"]
    assert result.autoscale is None


def test_minimal_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of Databricks linked service with minimal configuration."""
    fixture = get_fixture(linked_service_fixtures, "databricks_minimal")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.node_type_id == fixture["expected"]["node_type_id"]
    assert result.spark_version == fixture["expected"]["spark_version"]
    assert "CREATED_BY_WKMIGRATE" in result.custom_tags


def test_invalid_worker_count_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that invalid worker count returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "databricks_invalid_workers")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_databricks_null_input_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that null input returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "databricks_null")
    result = translate_databricks_cluster_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_full_sql_server_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of SQL Server linked service with full configuration."""
    fixture = get_fixture(linked_service_fixtures, "sql_full")
    result = translate_sql_server_spec(fixture["input"])

    assert isinstance(result, SqlLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.host == fixture["expected"]["host"]
    assert result.database == fixture["expected"]["database"]
    assert result.user_name == fixture["expected"]["user_name"]
    assert result.authentication_type == fixture["expected"]["authentication_type"]


def test_minimal_sql_server_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of SQL Server linked service with minimal configuration."""
    fixture = get_fixture(linked_service_fixtures, "sql_minimal")
    result = translate_sql_server_spec(fixture["input"])

    assert isinstance(result, SqlLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.host == fixture["expected"]["host"]
    assert result.database == fixture["expected"]["database"]
    assert result.user_name is None
    assert result.authentication_type is None


def test_sql_null_input_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that null input returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "sql_null")
    result = translate_sql_server_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_full_abfs_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of ABFS linked service with full configuration."""
    fixture = get_fixture(linked_service_fixtures, "abfs_full")
    result = translate_abfs_spec(fixture["input"])

    assert isinstance(result, AbfsLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.url == fixture["expected"]["url"]
    assert result.storage_account_name == fixture["expected"]["storage_account_name"]


def test_invalid_connection_string_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that invalid connection string returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "abfs_invalid")
    result = translate_abfs_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_abfs_null_input_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that null input returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "abfs_null")
    result = translate_abfs_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_single_worker_as_string() -> None:
    """Test parsing of single worker count as string."""
    spec = {
        "name": "test-cluster",
        "properties": {
            "domain": "https://test.azuredatabricks.net",
            "new_cluster_node_type": "Standard_DS3_v2",
            "new_cluster_version": "13.3.x-scala2.12",
            "new_cluster_num_of_worker": "1",
        },
    }
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.num_workers == 1
    assert result.autoscale is None


def test_autoscale_range_parsed_correctly() -> None:
    """Test parsing of autoscale range like '1:10'."""
    spec = {
        "name": "test-cluster",
        "properties": {
            "domain": "https://test.azuredatabricks.net",
            "new_cluster_node_type": "Standard_DS3_v2",
            "new_cluster_version": "13.3.x-scala2.12",
            "new_cluster_num_of_worker": "1:10",
        },
    }
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.autoscale == {"min_workers": 1, "max_workers": 10}
    assert result.num_workers is None


def test_no_workers_specified() -> None:
    """Test cluster with no workers specified."""
    spec = {
        "name": "test-cluster",
        "properties": {
            "domain": "https://test.azuredatabricks.net",
            "new_cluster_node_type": "Standard_DS3_v2",
            "new_cluster_version": "13.3.x-scala2.12",
        },
    }
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.num_workers is None
    assert result.autoscale is None


def test_empty_properties() -> None:
    """Test cluster with empty properties."""
    spec = {"name": "test-cluster", "properties": {}}
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.node_type_id is None
    assert result.spark_version is None


def test_init_scripts_dbfs_prefix() -> None:
    """Test init script with dbfs: prefix."""
    spec = {"name": "test-cluster", "properties": {"new_cluster_init_scripts": ["dbfs:/init/script.sh"]}}
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.init_scripts is not None
    assert "dbfs" in result.init_scripts[0]


def test_init_scripts_volumes_prefix() -> None:
    """Test init script with /Volumes prefix."""
    spec = {
        "name": "test-cluster",
        "properties": {"new_cluster_init_scripts": ["/Volumes/catalog/schema/volume/script.sh"]},
    }
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.init_scripts is not None
    assert "volumes" in result.init_scripts[0]


def test_init_scripts_workspace_path() -> None:
    """Test init script with workspace path."""
    spec = {
        "name": "test-cluster",
        "properties": {"new_cluster_init_scripts": ["/Users/user@company.com/scripts/init.sh"]},
    }
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.init_scripts is not None
    assert "workspace" in result.init_scripts[0]


def test_cluster_log_conf_none() -> None:
    """Test cluster with no log destination."""
    spec = {"name": "test-cluster", "properties": {"new_cluster_node_type": "Standard_DS3_v2"}}
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.cluster_log_conf is None


def test_empty_custom_tags_gets_system_tag() -> None:
    """Test that empty custom tags still get system tag."""
    spec = {"name": "test-cluster", "properties": {"new_cluster_node_type": "Standard_DS3_v2"}}
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    assert result.custom_tags is not None
    assert "CREATED_BY_WKMIGRATE" in result.custom_tags


def test_uuid_generated_when_no_name() -> None:
    """Test that UUID is generated when no name is provided."""
    spec = {"properties": {"new_cluster_node_type": "Standard_DS3_v2"}}
    result = translate_databricks_cluster_spec(spec)

    assert isinstance(result, DatabricksClusterLinkedService)
    # UUID format check - should be a non-empty string
    assert result.service_name is not None
    assert len(result.service_name) > 0


def test_full_s3_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of S3 linked service with full configuration."""
    fixture = get_fixture(linked_service_fixtures, "s3_full")
    result = translate_s3_spec(fixture["input"])

    assert isinstance(result, S3LinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.access_key_id == fixture["expected"]["access_key_id"]
    assert result.service_url == fixture["expected"]["service_url"]


def test_minimal_s3_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of S3 linked service with minimal configuration."""
    fixture = get_fixture(linked_service_fixtures, "s3_minimal")
    result = translate_s3_spec(fixture["input"])

    assert isinstance(result, S3LinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.access_key_id is None
    assert result.service_url is None


def test_s3_null_input_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that null input returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "s3_null")
    result = translate_s3_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_full_gcs_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of GCS linked service with full configuration."""
    fixture = get_fixture(linked_service_fixtures, "gcs_full")
    result = translate_gcs_spec(fixture["input"])

    assert isinstance(result, GcsLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.access_key_id == fixture["expected"]["access_key_id"]
    assert result.service_url == fixture["expected"]["service_url"]


def test_minimal_gcs_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of GCS linked service with minimal configuration."""
    fixture = get_fixture(linked_service_fixtures, "gcs_minimal")
    result = translate_gcs_spec(fixture["input"])

    assert isinstance(result, GcsLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.access_key_id is None
    assert result.service_url is None


def test_gcs_null_input_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that null input returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "gcs_null")
    result = translate_gcs_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_full_azure_blob_configuration(linked_service_fixtures: list[dict]) -> None:
    """Test translation of Azure Blob linked service with full configuration."""
    fixture = get_fixture(linked_service_fixtures, "azure_blob_full")
    result = translate_azure_blob_spec(fixture["input"])

    assert isinstance(result, AzureBlobLinkedService)
    assert result.service_name == fixture["expected"]["service_name"]
    assert result.service_type == fixture["expected"]["service_type"]
    assert result.url == fixture["expected"]["url"]
    assert result.storage_account_name == fixture["expected"]["storage_account_name"]


def test_azure_blob_missing_connection_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that missing connection_string and service_endpoint returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "azure_blob_missing_connection")
    result = translate_azure_blob_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_azure_blob_null_input_returns_unsupported(linked_service_fixtures: list[dict]) -> None:
    """Test that null input returns UnsupportedValue."""
    fixture = get_fixture(linked_service_fixtures, "azure_blob_null")
    result = translate_azure_blob_spec(fixture["input"])

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message

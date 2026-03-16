"""Pytest fixtures for integration tests against a real Azure Data Factory.

Fixtures in this module deploy and tear down Azure resources needed for
end-to-end translation testing. They rely on environment variables that are
expected to be provided via GitHub repository secrets in CI.

Required environment variables:
    AZURE_TENANT_ID:        Azure AD tenant identifier.
    AZURE_CLIENT_ID:        Service principal application (client) ID.
    AZURE_CLIENT_SECRET:    Service principal client secret.
    AZURE_SUBSCRIPTION_ID:  Azure subscription hosting the test resources.
    AZURE_RESOURCE_GROUP:   Resource group for the integration-test factory.
    AZURE_FACTORY_NAME:     Name of the pre-deployed Azure Data Factory instance.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from dataclasses import dataclass

import pytest
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    DatasetResource,
    Factory,
    LinkedServiceResource,
    PipelineResource,
)

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    """Return the value of an environment variable or skip the test."""
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"Environment variable {name} is not set")
    return value


@dataclass(slots=True)
class AzureTestConfig:
    """Holds Azure credentials and resource identifiers for integration tests."""

    tenant_id: str
    client_id: str
    client_secret: str
    subscription_id: str
    resource_group: str
    factory_name: str


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def azure_config() -> AzureTestConfig:
    """Load Azure configuration from environment variables.

    Returns:
        An ``AzureTestConfig`` populated from environment variables.
    """
    return AzureTestConfig(
        tenant_id=_require_env("AZURE_TENANT_ID"),
        client_id=_require_env("AZURE_CLIENT_ID"),
        client_secret=_require_env("AZURE_CLIENT_SECRET"),
        subscription_id=_require_env("AZURE_SUBSCRIPTION_ID"),
        resource_group=_require_env("AZURE_RESOURCE_GROUP"),
        factory_name=_require_env("AZURE_FACTORY_NAME"),
    )


@pytest.fixture(scope="session")
def azure_credential(azure_config: AzureTestConfig) -> ClientSecretCredential:
    """Create an Azure credential from the test configuration.

    Args:
        azure_config: Azure configuration fixture.

    Returns:
        A ``ClientSecretCredential`` for authenticating to Azure.
    """
    return ClientSecretCredential(
        tenant_id=azure_config.tenant_id,
        client_id=azure_config.client_id,
        client_secret=azure_config.client_secret,
    )


@pytest.fixture(scope="session")
def adf_management_client(
    azure_config: AzureTestConfig,
    azure_credential: ClientSecretCredential,
) -> DataFactoryManagementClient:
    """Create a Data Factory management client for resource provisioning.

    Args:
        azure_config: Azure configuration fixture.
        azure_credential: Azure credential fixture.

    Returns:
        A ``DataFactoryManagementClient`` instance.
    """
    return DataFactoryManagementClient(azure_credential, azure_config.subscription_id)


@pytest.fixture(scope="session")
def adf_factory(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
) -> Generator[Factory, None, None]:
    """Ensure the test Data Factory exists and return its resource descriptor.

    The factory is created if it does not already exist. Teardown deletes
    all test resources deployed during the session.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.

    Yields:
        The ``Factory`` resource object.
    """
    factory = adf_management_client.factories.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        factory=Factory(location="eastus2"),
    )
    yield factory


# ---------------------------------------------------------------------------
# Linked-service fixtures (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sample_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy a sample Azure Blob Storage linked service into the test factory.

    The linked service uses a connection-string authentication pattern
    pointing at a non-existent storage account. This is sufficient for
    translation testing since wkmigrate only reads metadata.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    linked_service = adf_management_client.linked_services.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_blob_storage",
        linked_service=LinkedServiceResource(
            properties={
                "type": "AzureBlobStorage",
                "typeProperties": {
                    "connectionString": (
                        "DefaultEndpointsProtocol=https;"
                        "AccountName=wkmigratetest;"
                        "AccountKey=FAKE==;"
                        "EndpointSuffix=core.windows.net"
                    ),
                },
            }
        ),
    )
    yield linked_service

    # Teardown
    adf_management_client.linked_services.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_blob_storage",
    )


@pytest.fixture(scope="session")
def abfs_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy an ABFS (Azure Data Lake Storage Gen2) linked service.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    linked_service = adf_management_client.linked_services.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_abfs_storage",
        linked_service=LinkedServiceResource(
            properties={
                "type": "AzureBlobFS",
                "typeProperties": {
                    "url": (
                        "DefaultEndpointsProtocol=https;"
                        "AccountName=wkmigrateabfs;"
                        "AccountKey=FAKE==;"
                        "EndpointSuffix=core.windows.net;"
                    ),
                    "accountKey": "FAKE==",
                },
            }
        ),
    )
    yield linked_service

    adf_management_client.linked_services.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_abfs_storage",
    )


@pytest.fixture(scope="session")
def s3_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy an Amazon S3 linked service.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    linked_service = adf_management_client.linked_services.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_s3_storage",
        linked_service=LinkedServiceResource(
            properties={
                "type": "AmazonS3",
                "typeProperties": {
                    "accessKeyId": "FAKEACCESSKEYID",
                    "serviceUrl": "https://s3.amazonaws.com",
                    "secretAccessKey": {
                        "type": "SecureString",
                        "value": "FAKESECRETKEY",
                    },
                },
            }
        ),
    )
    yield linked_service

    adf_management_client.linked_services.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_s3_storage",
    )


@pytest.fixture(scope="session")
def gcs_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy a Google Cloud Storage linked service.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    linked_service = adf_management_client.linked_services.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_gcs_storage",
        linked_service=LinkedServiceResource(
            properties={
                "type": "GoogleCloudStorage",
                "typeProperties": {
                    "accessKeyId": "FAKEGCSACCESSKEY",
                    "serviceUrl": "https://storage.googleapis.com",
                    "secretAccessKey": {
                        "type": "SecureString",
                        "value": "FAKEGCSSECRET",
                    },
                },
            }
        ),
    )
    yield linked_service

    adf_management_client.linked_services.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_gcs_storage",
    )


@pytest.fixture(scope="session")
def sql_server_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy an Azure SQL Database linked service.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    linked_service = adf_management_client.linked_services.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_sql_server",
        linked_service=LinkedServiceResource(
            properties={
                "type": "AzureSqlDatabase",
                "typeProperties": {
                    "server": "wkmigratetest.database.windows.net",
                    "database": "testdb",
                    "userName": "testuser",
                    "authenticationType": "SQL",
                    "password": {
                        "type": "SecureString",
                        "value": "FAKEPASSWORD",
                    },
                },
            }
        ),
    )
    yield linked_service

    adf_management_client.linked_services.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_sql_server",
    )


@pytest.fixture(scope="session")
def databricks_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy a Databricks workspace linked service.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    linked_service = adf_management_client.linked_services.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_databricks",
        linked_service=LinkedServiceResource(
            properties={
                "type": "AzureDatabricks",
                "typeProperties": {
                    "domain": "https://adb-1234567890.1.azuredatabricks.net",
                    "accessToken": {
                        "type": "SecureString",
                        "value": "FAKETOKEN",
                    },
                    "newClusterNodeType": "Standard_DS3_v2",
                    "newClusterNumOfWorker": "2",
                    "newClusterVersion": "14.3.x-scala2.12",
                },
            }
        ),
    )
    yield linked_service

    adf_management_client.linked_services.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        linked_service_name="test_databricks",
    )


# ---------------------------------------------------------------------------
# Dataset fixtures (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sample_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    sample_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy a sample CSV dataset referencing the test linked service.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        sample_linked_service: Ensures the linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_csv_dataset",
        dataset=DatasetResource(
            properties={
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobStorageLocation",
                        "container": "test-container",
                        "fileName": "data.csv",
                    },
                    "columnDelimiter": ",",
                    "firstRowAsHeader": True,
                },
                "linkedServiceName": {
                    "referenceName": "test_blob_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    # Teardown
    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_csv_dataset",
    )


@pytest.fixture(scope="session")
def abfs_csv_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    abfs_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy an ABFS-backed CSV (DelimitedText) dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        abfs_linked_service: Ensures the ABFS linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_abfs_csv_dataset",
        dataset=DatasetResource(
            properties={
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "test-container",
                        "fileName": "data.csv",
                    },
                    "columnDelimiter": ",",
                    "firstRowAsHeader": True,
                },
                "linkedServiceName": {
                    "referenceName": "test_abfs_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_abfs_csv_dataset",
    )


@pytest.fixture(scope="session")
def abfs_parquet_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    abfs_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy an ABFS-backed Parquet dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        abfs_linked_service: Ensures the ABFS linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_abfs_parquet_dataset",
        dataset=DatasetResource(
            properties={
                "type": "Parquet",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "test-container",
                        "fileName": "data.parquet",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_abfs_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_abfs_parquet_dataset",
    )


@pytest.fixture(scope="session")
def s3_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    s3_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy an Amazon S3 dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        s3_linked_service: Ensures the S3 linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_s3_dataset",
        dataset=DatasetResource(
            properties={
                "type": "AmazonS3Dataset",
                "typeProperties": {
                    "bucketName": "test-bucket",
                    "key": "data/output.parquet",
                    "format": {
                        "type": "ParquetFormat",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_s3_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_s3_dataset",
    )


@pytest.fixture(scope="session")
def gcs_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    gcs_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy a Google Cloud Storage dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        gcs_linked_service: Ensures the GCS linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_gcs_dataset",
        dataset=DatasetResource(
            properties={
                "type": "GoogleCloudStorageDataset",
                "typeProperties": {
                    "bucketName": "test-gcs-bucket",
                    "key": "data/output.parquet",
                    "format": {
                        "type": "ParquetFormat",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_gcs_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_gcs_dataset",
    )


@pytest.fixture(scope="session")
def azure_blob_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    sample_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy an Azure Blob Storage dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        sample_linked_service: Ensures the Blob linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_azure_blob_dataset",
        dataset=DatasetResource(
            properties={
                "type": "AzureBlobStorageDataset",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobStorageLocation",
                        "container": "test-container",
                        "fileName": "blob_data.parquet",
                    },
                    "format": {
                        "type": "ParquetFormat",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_blob_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_azure_blob_dataset",
    )


@pytest.fixture(scope="session")
def sql_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    sql_server_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy an Azure SQL Table dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        sql_server_linked_service: Ensures the SQL linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_sql_dataset",
        dataset=DatasetResource(
            properties={
                "type": "AzureSqlTable",
                "typeProperties": {
                    "schema": "dbo",
                    "table": "test_table",
                },
                "linkedServiceName": {
                    "referenceName": "test_sql_server",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_sql_dataset",
    )


@pytest.fixture(scope="session")
def delta_dataset(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    databricks_linked_service: LinkedServiceResource,
) -> Generator[DatasetResource, None, None]:
    """Deploy a Databricks Delta Lake dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        databricks_linked_service: Ensures the Databricks linked service exists first.

    Yields:
        The created ``DatasetResource``.
    """
    dataset = adf_management_client.datasets.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_delta_dataset",
        dataset=DatasetResource(
            properties={
                "type": "AzureDatabricksDeltaLakeDataset",
                "typeProperties": {
                    "database": "test_db",
                    "table": "test_delta_table",
                },
                "linkedServiceName": {
                    "referenceName": "test_databricks",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    )
    yield dataset

    adf_management_client.datasets.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        dataset_name="test_delta_dataset",
    )


# ---------------------------------------------------------------------------
# Pipeline fixtures (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def sample_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[PipelineResource, None, None]:
    """Deploy a sample ADF pipeline with Databricks notebook activities.

    The pipeline contains two sequential notebook activities and a
    string parameter. This provides a realistic but minimal translation
    target.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "extract_data",
                        "type": "DatabricksNotebook",
                        "typeProperties": {
                            "notebookPath": "/Shared/extract",
                        },
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                    {
                        "name": "transform_data",
                        "type": "DatabricksNotebook",
                        "typeProperties": {
                            "notebookPath": "/Shared/transform",
                        },
                        "dependsOn": [
                            {
                                "activity": "extract_data",
                                "dependencyConditions": ["Succeeded"],
                            }
                        ],
                        "policy": {"timeout": "0.02:00:00"},
                    },
                ],
                "parameters": {
                    "env": {"type": "String", "defaultValue": "dev"},
                },
            }
        ),
    )
    yield pipeline

    # Teardown
    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_pipeline",
    )


@pytest.fixture(scope="session")
def sample_foreach_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a ForEach activity for control-flow testing.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_foreach_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "process_items",
                        "type": "ForEach",
                        "typeProperties": {
                            "isSequential": False,
                            "batchCount": 5,
                            "items": {
                                "value": "@pipeline().parameters.items",
                                "type": "Expression",
                            },
                            "activities": [
                                {
                                    "name": "process_item",
                                    "type": "DatabricksNotebook",
                                    "typeProperties": {
                                        "notebookPath": "/Shared/process",
                                    },
                                    "dependsOn": [],
                                    "policy": {"timeout": "0.01:00:00"},
                                }
                            ],
                        },
                        "dependsOn": [],
                    }
                ],
                "parameters": {
                    "items": {"type": "Array", "defaultValue": []},
                },
            }
        ),
    )
    yield pipeline

    # Teardown
    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_foreach_pipeline",
    )


@pytest.fixture(scope="session")
def sample_unsupported_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with an unsupported activity type and a secure-input policy.

    The ``AzureFunctionActivity`` type is not in the translator registry and
    will produce a placeholder ``DatabricksNotebookActivity`` with
    ``notebook_path="/UNSUPPORTED_ADF_ACTIVITY"``. The ``secure_input`` policy
    property triggers a ``NotTranslatableWarning`` during translation.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_unsupported_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "unsupported_function_call",
                        "type": "AzureFunctionActivity",
                        "typeProperties": {
                            "functionName": "MyFunction",
                            "method": "POST",
                        },
                        "dependsOn": [],
                        "policy": {
                            "timeout": "0.00:30:00",
                            "secure_input": True,
                        },
                    },
                ],
            }
        ),
    )
    yield pipeline

    # Teardown
    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_unsupported_pipeline",
    )


@pytest.fixture(scope="session")
def spark_jar_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    databricks_linked_service: LinkedServiceResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a DatabricksSparkJar activity.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        databricks_linked_service: Ensures the Databricks linked service exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_spark_jar_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "run_spark_jar",
                        "type": "DatabricksSparkJar",
                        "linkedServiceName": {
                            "referenceName": "test_databricks",
                            "type": "LinkedServiceReference",
                        },
                        "typeProperties": {
                            "mainClassName": "com.example.Main",
                            "parameters": ["--input", "/data/input"],
                        },
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_spark_jar_pipeline",
    )


@pytest.fixture(scope="session")
def spark_python_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    databricks_linked_service: LinkedServiceResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a DatabricksSparkPython activity.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        databricks_linked_service: Ensures the Databricks linked service exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_spark_python_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "run_spark_python",
                        "type": "DatabricksSparkPython",
                        "linkedServiceName": {
                            "referenceName": "test_databricks",
                            "type": "LinkedServiceReference",
                        },
                        "typeProperties": {
                            "pythonFile": "dbfs:/scripts/etl.py",
                            "parameters": ["--env", "test"],
                        },
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_spark_python_pipeline",
    )


@pytest.fixture(scope="session")
def databricks_job_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    databricks_linked_service: LinkedServiceResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a DatabricksJob activity.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        databricks_linked_service: Ensures the Databricks linked service exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_databricks_job_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "run_databricks_job",
                        "type": "DatabricksJob",
                        "linkedServiceName": {
                            "referenceName": "test_databricks",
                            "type": "LinkedServiceReference",
                        },
                        "typeProperties": {
                            "existingJobId": "12345",
                        },
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_databricks_job_pipeline",
    )


@pytest.fixture(scope="session")
def web_activity_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a WebActivity.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_web_activity_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "call_api",
                        "type": "WebActivity",
                        "typeProperties": {
                            "url": "https://httpbin.org/get",
                            "method": "GET",
                        },
                        "dependsOn": [],
                        "policy": {"timeout": "0.00:05:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_web_activity_pipeline",
    )


@pytest.fixture(scope="session")
def lookup_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    abfs_csv_dataset: DatasetResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a Lookup activity reading the ABFS CSV dataset.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        abfs_csv_dataset: Ensures the ABFS CSV dataset exists first.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_lookup_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "lookup_csv_data",
                        "type": "Lookup",
                        "typeProperties": {
                            "source": {
                                "type": "DelimitedTextSource",
                                "storeSettings": {
                                    "type": "AzureBlobFSReadSettings",
                                    "recursive": True,
                                },
                            },
                            "dataset": {
                                "referenceName": "test_abfs_csv_dataset",
                                "type": "DatasetReference",
                            },
                            "firstRowOnly": True,
                        },
                        "dependsOn": [],
                        "policy": {"timeout": "0.00:10:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_lookup_pipeline",
    )


@pytest.fixture(scope="session")
def copy_abfs_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    abfs_csv_dataset: DatasetResource,
    abfs_parquet_dataset: DatasetResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a Copy activity using ABFS (CSV to Parquet).

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        abfs_csv_dataset: Ensures the ABFS CSV dataset exists.
        abfs_parquet_dataset: Ensures the ABFS Parquet dataset exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_abfs_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "copy_abfs_csv_to_parquet",
                        "type": "Copy",
                        "typeProperties": {
                            "source": {
                                "type": "DelimitedTextSource",
                                "storeSettings": {
                                    "type": "AzureBlobFSReadSettings",
                                    "recursive": True,
                                },
                            },
                            "sink": {
                                "type": "ParquetSink",
                                "storeSettings": {
                                    "type": "AzureBlobFSWriteSettings",
                                },
                            },
                        },
                        "inputs": [
                            {
                                "referenceName": "test_abfs_csv_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "outputs": [
                            {
                                "referenceName": "test_abfs_parquet_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_abfs_pipeline",
    )


@pytest.fixture(scope="session")
def copy_s3_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    s3_dataset: DatasetResource,
    abfs_parquet_dataset: DatasetResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a Copy activity reading from S3.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        s3_dataset: Ensures the S3 dataset exists.
        abfs_parquet_dataset: Ensures the ABFS Parquet dataset exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_s3_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "copy_s3_to_abfs",
                        "type": "Copy",
                        "typeProperties": {
                            "source": {
                                "type": "ParquetSource",
                                "storeSettings": {
                                    "type": "AmazonS3ReadSettings",
                                    "recursive": True,
                                },
                            },
                            "sink": {
                                "type": "ParquetSink",
                                "storeSettings": {
                                    "type": "AzureBlobFSWriteSettings",
                                },
                            },
                        },
                        "inputs": [
                            {
                                "referenceName": "test_s3_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "outputs": [
                            {
                                "referenceName": "test_abfs_parquet_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_s3_pipeline",
    )


@pytest.fixture(scope="session")
def copy_gcs_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    gcs_dataset: DatasetResource,
    abfs_parquet_dataset: DatasetResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a Copy activity reading from GCS.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        gcs_dataset: Ensures the GCS dataset exists.
        abfs_parquet_dataset: Ensures the ABFS Parquet dataset exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_gcs_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "copy_gcs_to_abfs",
                        "type": "Copy",
                        "typeProperties": {
                            "source": {
                                "type": "ParquetSource",
                                "storeSettings": {
                                    "type": "GoogleCloudStorageReadSettings",
                                    "recursive": True,
                                },
                            },
                            "sink": {
                                "type": "ParquetSink",
                                "storeSettings": {
                                    "type": "AzureBlobFSWriteSettings",
                                },
                            },
                        },
                        "inputs": [
                            {
                                "referenceName": "test_gcs_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "outputs": [
                            {
                                "referenceName": "test_abfs_parquet_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_gcs_pipeline",
    )


@pytest.fixture(scope="session")
def copy_azure_blob_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    azure_blob_dataset: DatasetResource,
    abfs_parquet_dataset: DatasetResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a Copy activity reading from Azure Blob Storage.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        azure_blob_dataset: Ensures the Azure Blob dataset exists.
        abfs_parquet_dataset: Ensures the ABFS Parquet dataset exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_azure_blob_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "copy_blob_to_abfs",
                        "type": "Copy",
                        "typeProperties": {
                            "source": {
                                "type": "ParquetSource",
                                "storeSettings": {
                                    "type": "AzureBlobStorageReadSettings",
                                    "recursive": True,
                                },
                            },
                            "sink": {
                                "type": "ParquetSink",
                                "storeSettings": {
                                    "type": "AzureBlobFSWriteSettings",
                                },
                            },
                        },
                        "inputs": [
                            {
                                "referenceName": "test_azure_blob_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "outputs": [
                            {
                                "referenceName": "test_abfs_parquet_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_azure_blob_pipeline",
    )


@pytest.fixture(scope="session")
def copy_sql_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    sql_dataset: DatasetResource,
    abfs_parquet_dataset: DatasetResource,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a Copy activity reading from Azure SQL.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        sql_dataset: Ensures the SQL dataset exists.
        abfs_parquet_dataset: Ensures the ABFS Parquet dataset exists.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_sql_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "copy_sql_to_abfs",
                        "type": "Copy",
                        "typeProperties": {
                            "source": {
                                "type": "AzureSqlSource",
                                "sqlReaderQuery": "SELECT * FROM dbo.test_table",
                            },
                            "sink": {
                                "type": "ParquetSink",
                                "storeSettings": {
                                    "type": "AzureBlobFSWriteSettings",
                                },
                            },
                        },
                        "inputs": [
                            {
                                "referenceName": "test_sql_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "outputs": [
                            {
                                "referenceName": "test_abfs_parquet_dataset",
                                "type": "DatasetReference",
                            }
                        ],
                        "dependsOn": [],
                        "policy": {"timeout": "0.01:00:00"},
                    },
                ],
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_copy_sql_pipeline",
    )


@pytest.fixture(scope="session")
def if_condition_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with an IfCondition activity.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_if_condition_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "check_env",
                        "type": "IfCondition",
                        "typeProperties": {
                            "expression": {
                                "value": "@equals(pipeline().parameters.env, 'prod')",
                                "type": "Expression",
                            },
                            "ifTrueActivities": [
                                {
                                    "name": "prod_notebook",
                                    "type": "DatabricksNotebook",
                                    "typeProperties": {
                                        "notebookPath": "/Shared/prod_etl",
                                    },
                                    "dependsOn": [],
                                    "policy": {"timeout": "0.01:00:00"},
                                }
                            ],
                            "ifFalseActivities": [
                                {
                                    "name": "dev_notebook",
                                    "type": "DatabricksNotebook",
                                    "typeProperties": {
                                        "notebookPath": "/Shared/dev_etl",
                                    },
                                    "dependsOn": [],
                                    "policy": {"timeout": "0.01:00:00"},
                                }
                            ],
                        },
                        "dependsOn": [],
                    }
                ],
                "parameters": {
                    "env": {"type": "String", "defaultValue": "dev"},
                },
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_if_condition_pipeline",
    )


@pytest.fixture(scope="session")
def set_variable_pipeline(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[PipelineResource, None, None]:
    """Deploy a pipeline with a SetVariable activity.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``PipelineResource``.
    """
    pipeline = adf_management_client.pipelines.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_set_variable_pipeline",
        pipeline=PipelineResource(
            properties={
                "activities": [
                    {
                        "name": "set_output_path",
                        "type": "SetVariable",
                        "typeProperties": {
                            "variableName": "output_path",
                            "value": "/data/output",
                        },
                        "dependsOn": [],
                    }
                ],
                "variables": {
                    "output_path": {"type": "String", "defaultValue": ""},
                },
            }
        ),
    )
    yield pipeline

    adf_management_client.pipelines.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        pipeline_name="integration_test_set_variable_pipeline",
    )


# ---------------------------------------------------------------------------
# High-level store fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def factory_client(azure_config: AzureTestConfig) -> FactoryClient:
    """Create a ``FactoryClient`` connected to the test Azure Data Factory.

    Args:
        azure_config: Azure configuration fixture.

    Returns:
        A ``FactoryClient`` instance pointing at the test factory.
    """
    return FactoryClient(
        tenant_id=azure_config.tenant_id,
        client_id=azure_config.client_id,
        client_secret=azure_config.client_secret,
        subscription_id=azure_config.subscription_id,
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
    )


@pytest.fixture()
def factory_store(azure_config: AzureTestConfig) -> FactoryDefinitionStore:
    """Create a ``FactoryDefinitionStore`` connected to the test factory.

    Args:
        azure_config: Azure configuration fixture.

    Returns:
        A ``FactoryDefinitionStore`` instance.
    """
    return FactoryDefinitionStore(
        tenant_id=azure_config.tenant_id,
        client_id=azure_config.client_id,
        client_secret=azure_config.client_secret,
        subscription_id=azure_config.subscription_id,
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
    )

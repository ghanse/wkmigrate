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
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import pytest
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    ActivityPolicy,
    AmazonS3ReadSettings,
    AzureBlobFSReadSettings,
    AzureBlobFSWriteSettings,
    AzureBlobStorageReadSettings,
    AzureFunctionActivity,
    AzureSqlSource,
    CopyActivity,
    DatabricksNotebookActivity,
    DatabricksSparkJarActivity,
    DatabricksJobActivity,
    DatabricksSparkPythonActivity,
    DatasetReference,
    DatasetResource,
    DelimitedTextSource,
    Expression,
    Factory,
    ForEachActivity,
    GoogleCloudStorageReadSettings,
    IfConditionActivity,
    LinkedServiceReference,
    LinkedServiceResource,
    LookupActivity,
    ParquetSink,
    ParquetSource,
    PipelineResource,
    SetVariableActivity,
    WebActivity,
)

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

_T = TypeVar("_T", LinkedServiceResource, DatasetResource, PipelineResource)


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


# Name-keyword and resource-keyword mappings for the three ADF management
# sub-clients.  Each tuple is (name_kwarg, resource_kwarg) passed to both
# ``create_or_update`` and ``delete``.
_KWARG_MAP: dict[str, tuple[str, str]] = {
    "LinkedServicesOperations": ("linked_service_name", "linked_service"),
    "DatasetsOperations": ("dataset_name", "dataset"),
    "PipelinesOperations": ("pipeline_name", "pipeline"),
}


@contextmanager
def _deploy_adf_resource(
    mgmt_ops: Any,
    azure_config: AzureTestConfig,
    resource_name: str,
    resource: _T,
) -> Generator[_T, None, None]:
    """Create an ADF resource, yield it, then delete it on teardown.

    This eliminates the repetitive create/yield/delete boilerplate that every
    linked-service, dataset, and pipeline fixture would otherwise duplicate.

    Args:
        mgmt_ops: One of ``adf_management_client.linked_services``,
            ``.datasets``, or ``.pipelines``.
        azure_config: Azure configuration with resource group / factory name.
        resource_name: The name to assign to the resource inside ADF.
        resource: The SDK resource object to deploy.

    Yields:
        The created resource as returned by the Azure SDK.
    """
    ops_type = type(mgmt_ops).__name__
    name_kwarg, resource_kwarg = _KWARG_MAP[ops_type]

    result = mgmt_ops.create_or_update(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        **{name_kwarg: resource_name},
        **{resource_kwarg: resource},
    )
    yield result

    mgmt_ops.delete(
        resource_group_name=azure_config.resource_group,
        factory_name=azure_config.factory_name,
        **{name_kwarg: resource_name},
    )


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

    The factory is assumed to be pre-deployed and shared across test runs.
    Teardown is intentionally omitted — we do not delete the factory because
    it is a long-lived resource managed outside of CI. Individual resource
    fixtures (linked services, datasets, pipelines) clean up after themselves.

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


@pytest.fixture(scope="session")
def sample_linked_service(
    azure_config: AzureTestConfig,
    adf_management_client: DataFactoryManagementClient,
    adf_factory: Factory,
) -> Generator[LinkedServiceResource, None, None]:
    """Deploy a sample Azure Blob Storage linked service into the test factory.

    Args:
        azure_config: Azure configuration fixture.
        adf_management_client: Data Factory management client fixture.
        adf_factory: Ensures the factory exists before provisioning.

    Yields:
        The created ``LinkedServiceResource``.
    """
    with _deploy_adf_resource(
        adf_management_client.linked_services,
        azure_config,
        "test_blob_storage",
        LinkedServiceResource(
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
    ) as ls:
        yield ls


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
    with _deploy_adf_resource(
        adf_management_client.linked_services,
        azure_config,
        "test_abfs_storage",
        LinkedServiceResource(
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
    ) as ls:
        yield ls


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
    with _deploy_adf_resource(
        adf_management_client.linked_services,
        azure_config,
        "test_s3_storage",
        LinkedServiceResource(
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
    ) as ls:
        yield ls


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
    with _deploy_adf_resource(
        adf_management_client.linked_services,
        azure_config,
        "test_gcs_storage",
        LinkedServiceResource(
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
    ) as ls:
        yield ls


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
    with _deploy_adf_resource(
        adf_management_client.linked_services,
        azure_config,
        "test_sql_server",
        LinkedServiceResource(
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
    ) as ls:
        yield ls


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
    with _deploy_adf_resource(
        adf_management_client.linked_services,
        azure_config,
        "test_databricks",
        LinkedServiceResource(
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
    ) as ls:
        yield ls


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_csv_dataset",
        DatasetResource(
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
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_abfs_csv_dataset",
        DatasetResource(
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
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_abfs_parquet_dataset",
        DatasetResource(
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
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_s3_dataset",
        DatasetResource(
            properties={
                "type": "Parquet",
                "typeProperties": {
                    "location": {
                        "type": "AmazonS3Location",
                        "bucketName": "test-bucket",
                        "fileName": "output.parquet",
                        "folderPath": "data",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_s3_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_gcs_dataset",
        DatasetResource(
            properties={
                "type": "Parquet",
                "typeProperties": {
                    "location": {
                        "type": "GoogleCloudStorageLocation",
                        "bucketName": "test-gcs-bucket",
                        "fileName": "output.parquet",
                        "folderPath": "data",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_gcs_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_azure_blob_dataset",
        DatasetResource(
            properties={
                "type": "Parquet",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobStorageLocation",
                        "container": "test-container",
                        "fileName": "blob_data.parquet",
                    },
                },
                "linkedServiceName": {
                    "referenceName": "test_blob_storage",
                    "type": "LinkedServiceReference",
                },
            }
        ),
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.datasets,
        azure_config,
        "test_sql_dataset",
        DatasetResource(
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
    ) as ds:
        yield ds


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_pipeline",
        PipelineResource(
            activities=[
                DatabricksNotebookActivity(
                    name="extract_data",
                    notebook_path="/Shared/extract",
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
                DatabricksNotebookActivity(
                    name="transform_data",
                    notebook_path="/Shared/transform",
                    depends_on=[
                        ActivityDependency(
                            activity="extract_data",
                            dependency_conditions=["Succeeded"],
                        ),
                    ],
                    policy=ActivityPolicy(timeout="0.02:00:00"),
                ),
            ],
            parameters={
                "env": {"type": "String", "defaultValue": "dev"},
            },
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_foreach_pipeline",
        PipelineResource(
            activities=[
                ForEachActivity(
                    name="process_items",
                    is_sequential=False,
                    batch_count=5,
                    items=Expression(
                        type="Expression",
                        value="@array(['item1','item2','item3'])",
                    ),
                    activities=[
                        DatabricksNotebookActivity(
                            name="process_item",
                            notebook_path="/Shared/process",
                            depends_on=[],
                            policy=ActivityPolicy(timeout="0.01:00:00"),
                        ),
                    ],
                    depends_on=[],
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_unsupported_pipeline",
        PipelineResource(
            activities=[
                AzureFunctionActivity(
                    name="unsupported_function_call",
                    method="POST",
                    function_name="MyFunction",
                    depends_on=[],
                    policy=ActivityPolicy(
                        timeout="0.00:30:00",
                        secure_input=True,
                    ),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_spark_jar_pipeline",
        PipelineResource(
            activities=[
                DatabricksSparkJarActivity(
                    name="run_spark_jar",
                    main_class_name="com.example.Main",
                    parameters=["--input", "/data/input"],
                    linked_service_name=LinkedServiceReference(
                        type="LinkedServiceReference",
                        reference_name="test_databricks",
                    ),
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_spark_python_pipeline",
        PipelineResource(
            activities=[
                DatabricksSparkPythonActivity(
                    name="run_spark_python",
                    python_file="dbfs:/scripts/etl.py",
                    parameters=["--env", "test"],
                    linked_service_name=LinkedServiceReference(
                        type="LinkedServiceReference",
                        reference_name="test_databricks",
                    ),
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_databricks_job_pipeline",
        PipelineResource(
            activities=[
                DatabricksJobActivity(
                    name="run_databricks_job",
                    job_id=12345,
                    linked_service_name=LinkedServiceReference(
                        type="LinkedServiceReference",
                        reference_name="test_databricks",
                    ),
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                    depends_on=[],
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_web_activity_pipeline",
        PipelineResource(
            activities=[
                WebActivity(
                    name="call_api",
                    method="GET",
                    url="https://httpbin.org/get",
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.00:05:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_lookup_pipeline",
        PipelineResource(
            activities=[
                LookupActivity(
                    name="lookup_csv_data",
                    source=DelimitedTextSource(
                        store_settings=AzureBlobFSReadSettings(recursive=True),
                    ),
                    dataset=DatasetReference(
                        type="DatasetReference",
                        reference_name="test_abfs_csv_dataset",
                    ),
                    first_row_only=True,
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.00:10:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_copy_abfs_pipeline",
        PipelineResource(
            activities=[
                CopyActivity(
                    name="copy_abfs_csv_to_parquet",
                    source=DelimitedTextSource(
                        store_settings=AzureBlobFSReadSettings(recursive=True),
                    ),
                    sink=ParquetSink(
                        store_settings=AzureBlobFSWriteSettings(),
                    ),
                    inputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_abfs_csv_dataset",
                        ),
                    ],
                    outputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_abfs_parquet_dataset",
                        ),
                    ],
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_copy_s3_pipeline",
        PipelineResource(
            activities=[
                CopyActivity(
                    name="copy_s3_to_abfs",
                    source=ParquetSource(
                        store_settings=AmazonS3ReadSettings(recursive=True),
                    ),
                    sink=ParquetSink(
                        store_settings=AzureBlobFSWriteSettings(),
                    ),
                    inputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_s3_dataset",
                        ),
                    ],
                    outputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_abfs_parquet_dataset",
                        ),
                    ],
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_copy_gcs_pipeline",
        PipelineResource(
            activities=[
                CopyActivity(
                    name="copy_gcs_to_abfs",
                    source=ParquetSource(
                        store_settings=GoogleCloudStorageReadSettings(recursive=True),
                    ),
                    sink=ParquetSink(
                        store_settings=AzureBlobFSWriteSettings(),
                    ),
                    inputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_gcs_dataset",
                        ),
                    ],
                    outputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_abfs_parquet_dataset",
                        ),
                    ],
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_copy_azure_blob_pipeline",
        PipelineResource(
            activities=[
                CopyActivity(
                    name="copy_blob_to_abfs",
                    source=ParquetSource(
                        store_settings=AzureBlobStorageReadSettings(recursive=True),
                    ),
                    sink=ParquetSink(
                        store_settings=AzureBlobFSWriteSettings(),
                    ),
                    inputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_azure_blob_dataset",
                        ),
                    ],
                    outputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_abfs_parquet_dataset",
                        ),
                    ],
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_copy_sql_pipeline",
        PipelineResource(
            activities=[
                CopyActivity(
                    name="copy_sql_to_abfs",
                    source=AzureSqlSource(
                        sql_reader_query="SELECT * FROM dbo.test_table",
                    ),
                    sink=ParquetSink(
                        store_settings=AzureBlobFSWriteSettings(),
                    ),
                    inputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_sql_dataset",
                        ),
                    ],
                    outputs=[
                        DatasetReference(
                            type="DatasetReference",
                            reference_name="test_abfs_parquet_dataset",
                        ),
                    ],
                    depends_on=[],
                    policy=ActivityPolicy(timeout="0.01:00:00"),
                ),
            ],
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_if_condition_pipeline",
        PipelineResource(
            activities=[
                IfConditionActivity(
                    name="check_env",
                    expression=Expression(
                        type="Expression",
                        value="@equals(pipeline().parameters.env, 'prod')",
                    ),
                    if_true_activities=[
                        DatabricksNotebookActivity(
                            name="prod_notebook",
                            notebook_path="/Shared/prod_etl",
                            depends_on=[],
                            policy=ActivityPolicy(timeout="0.01:00:00"),
                        ),
                    ],
                    if_false_activities=[
                        DatabricksNotebookActivity(
                            name="dev_notebook",
                            notebook_path="/Shared/dev_etl",
                            depends_on=[],
                            policy=ActivityPolicy(timeout="0.01:00:00"),
                        ),
                    ],
                    depends_on=[],
                ),
            ],
            parameters={
                "env": {"type": "String", "defaultValue": "dev"},
            },
        ),
    ) as pl:
        yield pl


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
    with _deploy_adf_resource(
        adf_management_client.pipelines,
        azure_config,
        "integration_test_set_variable_pipeline",
        PipelineResource(
            activities=[
                SetVariableActivity(
                    name="set_output_path",
                    variable_name="output_path",
                    value="/data/output",
                    depends_on=[],
                ),
            ],
            variables={
                "output_path": {"type": "String", "defaultValue": ""},
            },
        ),
    ) as pl:
        yield pl


@pytest.fixture(scope="session")
def factory_client(azure_config: AzureTestConfig) -> FactoryClient:
    """Create a ``FactoryClient`` connected to the test Azure Data Factory.

    Session-scoped because the client is stateless — reusing a single instance
    avoids redundant Azure AD credential round-trips.

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


@pytest.fixture(scope="session")
def factory_store(azure_config: AzureTestConfig) -> FactoryDefinitionStore:
    """Create a ``FactoryDefinitionStore`` connected to the test factory.

    Session-scoped because the store is stateless — reusing a single instance
    avoids redundant Azure AD credential round-trips.

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

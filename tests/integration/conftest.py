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
# Test-resource provisioning (session-scoped)
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

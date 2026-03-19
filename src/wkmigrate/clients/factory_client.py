"""This module defines a client for interacting with Azure Data Factory.

Clients in this module interact with Azure Data Factory to retrieve pipeline, dataset,
linked service, and trigger definitions. Clients validate required fields, authenticate
using the provided credentials, and make API calls to the Data Factory resource using
the Azure Data Factory management client.
"""

from dataclasses import dataclass, field

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


@dataclass(slots=True)
class FactoryClient:
    """
    Data Factory management client for retrieving pipelines, datasets, linked services, and triggers.

    Attributes:
        tenant_id: Azure Active Directory tenant identifier used for authentication.
        client_id: Application (client) ID of the service principal.
        client_secret: Client secret associated with the service principal.
        subscription_id: Azure subscription identifier that hosts the Data Factory instance.
        resource_group_name: Resource group containing the Data Factory.
        factory_name: Name of the Azure Data Factory instance.
        management_client: ``DataFactoryManagementClient`` used to make API calls. Automatically created using the provided credentials.
    """

    __test__ = False

    tenant_id: str
    client_id: str
    client_secret: str
    subscription_id: str
    resource_group_name: str
    factory_name: str
    management_client: DataFactoryManagementClient | None = field(init=False)

    def __post_init__(self) -> None:
        """Sets up the Data Factory management client for the provided credentials."""
        if self.tenant_id is None:
            raise ValueError("A tenant_id must be provided when creating a FactoryDefinitionStore")
        if self.client_id is None:
            raise ValueError("A client_id must be provided when creating a FactoryDefinitionStore")
        if self.client_secret is None:
            raise ValueError("A client_secret must be provided when creating a FactoryDefinitionStore")
        credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        self.management_client = DataFactoryManagementClient(credential, self.subscription_id)

    def list_pipelines(self) -> list[str]:
        """
        Lists the names of all pipelines available in the Data Factory.

        Returns:
            Pipeline names as a ``list[str]``.
        """
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        pipelines = self.management_client.pipelines.list_by_factory(
            resource_group_name=self.resource_group_name, factory_name=self.factory_name
        )
        return [pipeline.name for pipeline in pipelines if pipeline.name is not None]  # type: ignore[misc]

    def get_pipeline(self, pipeline_name: str) -> dict:
        """
        Gets a pipeline definition with the specified name.

        Args:
            pipeline_name: Name of the Data Factory pipeline.

        Returns:
            Pipeline definition as a ``dict``.
        """
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        pipeline = self.management_client.pipelines.get(self.resource_group_name, self.factory_name, pipeline_name)
        if pipeline is None:
            raise ValueError(f'No pipeline found with name "{pipeline_name}"')
        return dict(pipeline.as_dict())

    def get_linked_service(self, linked_service_name: str) -> dict:
        """
        Gets a linked-service definition with the specified name.

        Args:
            linked_service_name: Name of the linked service in Data Factory.

        Returns:
            Linked-service definition as a ``dict``.
        """
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        linked_service = self.management_client.linked_services.get(
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
            linked_service_name=linked_service_name,
        )
        if linked_service is None:
            raise ValueError(f'No linked service found with name "{linked_service_name}"')
        return dict(linked_service.as_dict())

    def get_trigger(self, pipeline_name: str) -> dict | None:
        """
        Gets the trigger associated with a pipeline.

        Args:
            pipeline_name: Name of the Data Factory pipeline.

        Returns:
            Trigger definition as a ``dict``, or ``None`` if the pipeline has no trigger.
        """
        triggers = self._list_triggers()
        for trigger in triggers:
            properties = trigger.get("properties")
            if properties is None:
                continue
            pipelines = properties.get("pipelines")
            if pipelines is None:
                continue
            pipeline_references = [
                pipeline.get("pipeline_reference")
                for pipeline in pipelines
                if pipeline.get("pipeline_reference") is not None
            ]
            pipeline_names = [
                pipeline_reference.get("reference_name")
                for pipeline_reference in pipeline_references
                if pipeline_reference.get("reference_name") is not None
                and pipeline_reference.get("type") == "PipelineReference"
            ]
            if pipeline_name in pipeline_names:
                return trigger
        return None

    def _list_triggers(self) -> list[dict]:
        """
        Lists triggers available in the source Data Factory.

        Returns:
            List of trigger definitions as ``list[dict]``.
        """
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        triggers = self.management_client.triggers.list_by_factory(
            resource_group_name=self.resource_group_name, factory_name=self.factory_name
        )
        if triggers is None:
            raise ValueError(f'No triggers found for factory "{self.factory_name}"')
        return [dict(trigger.as_dict()) for trigger in triggers]

    def get_dataset(self, dataset_name: str) -> dict:
        """
        Gets the dataset definition for a specified dataset name.

        Args:
            dataset_name: Dataset name from the Data Factory.

        Returns:
            Dataset definition as a ``dict``.
        """
        datasets = self._list_datasets()
        for dataset in datasets:
            if dataset.get("name") != dataset_name:
                continue
            properties = dataset.get("properties")
            if properties is None:
                return dataset
            linked_service = properties.get("linked_service_name")
            if linked_service is None:
                return dataset
            linked_service_name = linked_service.get("reference_name")
            linked_service_definition = self.get_linked_service(linked_service_name)
            dataset["linked_service_definition"] = linked_service_definition
            return dataset
        raise ValueError(f'No dataset found for factory with name "{dataset_name}"')

    def _list_datasets(self) -> list[dict]:
        """
        Lists dataset definitions available in the source Data Factory.

        Returns:
            List of dataset definitions as ``list[dict]``.
        """
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        datasets = self.management_client.datasets.list_by_factory(
            resource_group_name=self.resource_group_name, factory_name=self.factory_name
        )
        if datasets is None:
            raise ValueError(f'No datasets found for factory "{self.factory_name}"')
        return [dict(dataset.as_dict()) for dataset in datasets]

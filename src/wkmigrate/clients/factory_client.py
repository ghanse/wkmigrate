"""Defines the ``FactoryClient`` class."""

from dataclasses import dataclass, field

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


@dataclass
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

    def get_pipeline(self, pipeline_name: str) -> dict:
        """
        Gets a pipeline definition with the specified name.

        Args:
            pipeline_name: Name of the Data Factory pipeline.

        Returns:
            Pipeline definition as a ``dict``.
        """
        # Get the pipelines:
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        pipeline = self.management_client.pipelines.get(self.resource_group_name, self.factory_name, pipeline_name)
        # If no pipeline was found:
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
        # Get the linked service:
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        linked_service = self.management_client.linked_services.get(
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
            linked_service_name=linked_service_name,
        )
        # If no linked service was found:
        if linked_service is None:
            raise ValueError(f'No linked service found with name "{linked_service_name}"')
        return dict(linked_service.as_dict())

    def get_trigger(self, pipeline_name: str) -> dict:
        """
        Gets the trigger associated with a pipeline.

        Args:
            pipeline_name: Name of the Data Factory pipeline.

        Returns:
            Trigger definition as a ``dict``.
        """
        # List the triggers:
        triggers = self._list_triggers()
        for trigger in triggers:
            # Get the trigger properties:
            properties = trigger.get("properties")
            if properties is None:
                continue
            # Get the associated pipeline definitions:
            pipelines = properties.get("pipelines")
            if pipelines is None:
                continue
            # Get the pipeline references:
            pipeline_references = [
                pipeline.get("pipeline_reference")
                for pipeline in pipelines
                if pipeline.get("pipeline_reference") is not None
            ]
            # Get the pipeline names:
            pipeline_names = [
                pipeline_reference.get("reference_name")
                for pipeline_reference in pipeline_references
                if pipeline_reference.get("reference_name") is not None
                and pipeline_reference.get("type") == "PipelineReference"
            ]
            # Get the trigger by pipeline name:
            if pipeline_name in pipeline_names:
                return trigger
        # If no trigger was found:
        raise ValueError(f'No trigger found for pipeline with name "{pipeline_name}"')

    def _list_triggers(self) -> list[dict]:
        """
        Lists triggers available in the source Data Factory.

        Returns:
            List of trigger definitions as ``list[dict]``.
        """
        # List the triggers:
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        triggers = self.management_client.triggers.list_by_factory(
            resource_group_name=self.resource_group_name, factory_name=self.factory_name
        )
        # If no triggers were found:
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
        # List the datasets:
        datasets = self._list_datasets()
        for dataset in datasets:
            # Check the dataset name:
            if dataset.get("name") != dataset_name:
                continue
            # Get the dataset properties:
            properties = dataset.get("properties")
            if properties is None:
                return dataset
            # Get the associated linked service:
            linked_service = properties.get("linked_service_name")
            if linked_service is None:
                return dataset
            # Get the linked service reference name:
            linked_service_name = linked_service.get("reference_name")
            # Get the linked service definition:
            linked_service_definition = self.get_linked_service(linked_service_name)
            # Append the linked service definition to the dataset object:
            dataset["linked_service_definition"] = linked_service_definition
            return dataset
        # If no datasets were found:
        raise ValueError(f'No dataset found for factory with name "{dataset_name}"')

    def _list_datasets(self) -> list[dict]:
        """
        Lists dataset definitions available in the source Data Factory.

        Returns:
            List of dataset definitions as ``list[dict]``.
        """
        # List the datasets:
        if self.management_client is None:
            raise ValueError("management_client is not initialized")
        datasets = self.management_client.datasets.list_by_factory(
            resource_group_name=self.resource_group_name, factory_name=self.factory_name
        )
        # If no datasets were found:
        if datasets is None:
            raise ValueError(f'No datasets found for factory "{self.factory_name}"')
        return [dict(dataset.as_dict()) for dataset in datasets]

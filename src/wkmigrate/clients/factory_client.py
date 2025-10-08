"""Defines the ``FactoryClient`` and ``TestFactoryClient`` classes."""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

import wkmigrate


class FactoryClient(ABC):
    """A client implementing methods for getting data pipeline, linked service,
    dataset, and pipeline trigger definitions.
    """

    __test__ = False

    @abstractmethod
    def get_pipeline(self, pipeline_name: str) -> dict:
        pass

    @abstractmethod
    def get_trigger(self, pipeline_name: str) -> dict:
        pass

    @abstractmethod
    def get_dataset(self, dataset_name: str) -> dict:
        pass

    @abstractmethod
    def get_linked_service(self, linked_service_name: str) -> dict:
        pass


@dataclass
class FactoryManagementClient(FactoryClient):
    """A Data Factory management client implementing methods for getting data pipeline,
    linked service, dataset, and pipeline trigger definitions.
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
        """Gets a pipeline definition with the specified name.
        :parameter pipeline_name: Name of the Data Factory pipeline as a ``str``
        :return: Data Factory pipeline definition as a ``dict``
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
        """Gets a linked service with the specified name from a Data Factory.
        :parameter linked_service_name: Name of the linked service in Data Factory as a ``str``
        :return: Linked service definition as a ``dict``
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
        """Gets a single trigger for a Data Factory pipeline.
        :parameter pipeline_name: Name of the Data Factory pipeline as a ``str``
        :return: Triggers in the source Data Factory as a ``list[dict]``
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
        """Lists triggers in the source Data Factory.
        :return: Triggers in the source Data Factory as a ``list[dict]``
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
        """Gets a single dataset from a source Data Factory.
        :parameter dataset_name: Dataset name as a ``str``
        :return: Dataset definition as a ``dict``
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
        """Lists datasets in the source Data Factory.
        :return: Datasets in the source Data Factory as a ``list[dict]``
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


@dataclass
class FactoryTestClient(FactoryClient):
    """A mock client implementing methods for getting data pipeline, linked service,
    dataset, and pipeline trigger definitions.
    """

    test_json_path: str = wkmigrate.JSON_PATH

    def get_pipeline(self, pipeline_name: str) -> dict:
        """Gets a pipeline definition with the specified name.
        :parameter pipeline_name: Name of the Data Factory pipeline as a ``str``
        :return: Data Factory pipeline definition as a ``dict``
        """
        # Open the test pipelines file:
        with open(f"{self.test_json_path}/test_pipelines.json", "rb") as file:
            # Load the data from JSON:
            pipelines = json.load(file)
        # Get the pipeline by name:
        for pipeline in pipelines:
            if pipeline.get("name") == pipeline_name:
                return pipeline
        # If no pipeline was found:
        raise ValueError(f'No pipeline found with name "{pipeline_name}"')

    def get_trigger(self, pipeline_name: str) -> dict:
        """Gets a single trigger for a Data Factory pipeline.
        :parameter pipeline_name: Name of the Data Factory pipeline as a ``str``
        :return: Triggers in the source Data Factory as a ``list[dict]``
        """
        # Open the test triggers file:
        with open(f"{self.test_json_path}/test_triggers.json", "rb") as file:
            # Load the data from JSON:
            triggers = json.load(file)
        # Get the pipeline by name:
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
                if (
                    pipeline_reference.get("reference_name") is not None
                    and pipeline_reference.get("type") == "PipelineReference"
                )
            ]
            # Get the trigger by pipeline name:
            if pipeline_name in pipeline_names:
                return trigger
        # If no trigger was found:
        raise ValueError(f'No trigger found for pipeline with name "{pipeline_name}"')

    def get_dataset(self, dataset_name: str) -> dict:
        """Gets a single dataset from a source Data Factory.
        :parameter dataset_name: Dataset name as a ``str``
        :return: Dataset definition as a ``dict``
        """
        # Open the test datasets file:
        with open(f"{self.test_json_path}/test_datasets.json", "rb") as file:
            # Load the data from JSON:
            datasets = json.load(file)
        for dataset in datasets:
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

    def get_linked_service(self, linked_service_name: str) -> dict:
        """Gets a linked service with the specified name from a Data Factory.
        :parameter linked_service_name: Name of the linked service in Data Factory as a ``str``
        :return: Linked service definition as a ``dict``
        """
        # Open the test linked_services file:
        with open(f"{self.test_json_path}/test_linked_services.json", "rb") as file:
            # Load the data from JSON:
            linked_services = json.load(file)
        # Get the linked_service by name:
        for linked_service in linked_services:
            if linked_service.get("name") == linked_service_name:
                return linked_service
        # If no linked service was found:
        raise ValueError(f'No linked service found with name "{linked_service_name}"')

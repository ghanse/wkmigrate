import json
import os

from dataclasses import dataclass
from wkmigrate.clients.factory_client import FactoryClient


# Path to test JSON files
JSON_PATH = os.path.join(os.path.dirname(__file__), "resources", "json")

# Path to test YAML files
YAML_PATH = os.path.join(os.path.dirname(__file__), "resources", "yaml")


@dataclass
class FactoryTestClient(FactoryClient):
    """A mock client implementing methods for getting data pipeline, linked service,
    dataset, and pipeline trigger definitions.
    """

    test_json_path: str = JSON_PATH

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

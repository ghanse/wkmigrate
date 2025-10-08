"""This module defines the ``FactoryDefinitionStore`` class."""

import warnings
from dataclasses import dataclass, field
from collections.abc import Callable
from wkmigrate.clients.factory_client import (
    FactoryClient,
    FactoryManagementClient,
    FactoryTestClient,
)
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.pipeline_translators.pipeline_translator import translate_pipeline


@dataclass
class FactoryDefinitionStore(DefinitionStore):
    """This class is used to list and describe objects in an Azure Data Factory instance."""

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    subscription_id: str | None = None
    resource_group_name: str | None = None
    factory_name: str | None = None
    factory_client: FactoryClient | None = field(init=False)
    _appenders: list[Callable[[dict], dict]] | None = field(init=False)
    _use_test_client: bool | None = False

    def __post_init__(self) -> None:
        self._appenders = [self._append_datasets, self._append_linked_service]
        if self._use_test_client:
            self.factory_client = FactoryTestClient()
            return
        if self.tenant_id is None:
            raise ValueError("A tenant_id must be provided when creating a FactoryDefinitionStore")
        if self.client_id is None:
            raise ValueError("A client_id must be provided when creating a FactoryDefinitionStore")
        if self.client_secret is None:
            raise ValueError("A client_secret must be provided when creating a FactoryDefinitionStore")
        if self.subscription_id is None:
            raise ValueError("subscription_id cannot be None")
        if self.resource_group_name is None:
            raise ValueError("resource_group_name cannot be None")
        if self.factory_name is None:
            raise ValueError("factory_name cannot be None")

        self.factory_client = FactoryManagementClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            subscription_id=self.subscription_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )

    def load(self, pipeline_name: str) -> dict:
        """Gets a dictionary representation of a Data Factory pipeline.
        :parameter pipeline_name: Name of the Data Factory pipeline as an ``str``
        :return: Data Factory pipeline as a ``dict``
        """
        if self.factory_client is None:
            raise ValueError("factory_client is not initialized")
        pipeline = self.factory_client.get_pipeline(pipeline_name)
        pipeline["trigger"] = self.factory_client.get_trigger(pipeline_name)
        activities = pipeline.get("activities")
        if activities is not None:
            pipeline["activities"] = [self._append_objects(activity) for activity in activities]
        else:
            pipeline["activities"] = []
        return translate_pipeline(pipeline)

    def dump(self, pipeline_definition: dict) -> None:
        """NOTE: Loading data pipeline definitions to Data Factory is not currently supported."""
        raise NotImplementedError(f"Dump of pipeline {pipeline_definition} to FactoryDefinitionStore not supported.")

    def _append_objects(self, activity: dict) -> dict:
        """Appends datasets, linked services, etc. associated with a pipeline activity.
        :parameter activity: Data Factory activity definition as a ``dict``
        :return: Data Factory activity definition with parsed objects as a ``dict``
        """
        if self._appenders is None:
            return activity
        for appender in self._appenders:
            activity = appender(activity)
        return activity

    def _append_datasets(self, activity: dict) -> dict:
        """Gets the dataset definitions and associated linked services for the
        specified pipeline activity.
        :parameter activity: Data Factory activity definition as a ``dict``
        :return: Data Factory activity definition with parsed datasets and linked services as a ``dict``
        """
        if "inputs" in activity:
            datasets = activity.get("inputs")
            if datasets is not None:
                dataset_names = [dataset.get("reference_name") for dataset in datasets]
                if self.factory_client is None:
                    raise ValueError("factory_client is not initialized")
                activity["input_dataset_definitions"] = [
                    self.factory_client.get_dataset(dataset_name) for dataset_name in dataset_names
                ]
        if "outputs" in activity:
            datasets = activity.get("outputs")
            if datasets is not None:
                dataset_names = [dataset.get("reference_name") for dataset in datasets]
                if self.factory_client is None:
                    raise ValueError("factory_client is not initialized")
                activity["output_dataset_definitions"] = [
                    self.factory_client.get_dataset(dataset_name) for dataset_name in dataset_names
                ]
        return activity

    def _append_linked_service(self, activity: dict) -> dict:
        """Gets the Databricks linked service for the specified pipeline activity
        :parameter activity: Data Factory activity definition as a ``dict``
        :return: Data Factory activity definition with parsed linked service as a ``dict``
        """
        if "linked_service_name" in activity:
            # Get the linked service reference name:
            linked_service_reference = activity.get("linked_service_name")
            if linked_service_reference is not None:
                linked_service_name = linked_service_reference.get("reference_name")
                if self.factory_client is None:
                    raise ValueError("factory_client is not initialized")
                # Get the linked service details from data factory:
                activity["linked_service_definition"] = self.factory_client.get_linked_service(linked_service_name)
        # Check the nested "if false" activities:
        if "if_false_activities" in activity:
            if_false_activities = activity.get("if_false_activities")
            if if_false_activities is not None:
                activity["if_false_activities"] = [
                    self._append_linked_service(if_false_activity) for if_false_activity in if_false_activities
                ]
        # Check the nested "if true" activities:
        if "if_true_activities" in activity:
            if_true_activities = activity.get("if_true_activities")
            if if_true_activities is not None:
                activity["if_true_activities"] = [
                    self._append_linked_service(if_true_activity) for if_true_activity in if_true_activities
                ]
        # Check the nested "for each" activities:
        if "activities" in activity:
            activities = activity.get("activities")
            if activities is not None:
                activity["activities"] = [
                    self._append_linked_service(for_each_activity) for for_each_activity in activities
                ]
        return activity

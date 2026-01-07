"""This module defines the ``FactoryDefinitionStore`` class."""

from dataclasses import asdict, dataclass, field
from collections.abc import Callable
from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.pipeline_translators.pipeline_translator import translate_pipeline


@dataclass
class FactoryDefinitionStore(DefinitionStore):
    """
    Definition store implementation backed by an Azure Data Factory instance.

    Attributes:
        tenant_id: Azure AD tenant identifier.
        client_id: Service principal application (client) ID.
        client_secret: Secret used to authenticate the client.
        subscription_id: Azure subscription identifier.
        resource_group_name: Resource group name for the factory.
        factory_name: Name of the Azure Data Factory instance.
        factory_client: Concrete ``FactoryClient`` used to load pipelines and child resources. Automatically created using the provided credentials.
    """

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    subscription_id: str | None = None
    resource_group_name: str | None = None
    factory_name: str | None = None
    factory_client: FactoryClient | None = field(init=False)
    _appenders: list[Callable[[dict], dict]] | None = field(init=False)

    def __post_init__(self) -> None:
        """
        Validates configuration and initializes the Factory client.

        Raises:
            ValueError: If the tenant ID is not provided.
            ValueError: If the client ID is not provided.
            ValueError: If the client secret is not provided.
            ValueError: If the subscription ID is not provided.
            ValueError: If the resource group name is not provided.
            ValueError: If the factory name is not provided.
        """
        self._appenders = [self._append_datasets, self._append_linked_service]
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

        self.factory_client = FactoryClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            subscription_id=self.subscription_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )

    def load(self, pipeline_name: str) -> dict:
        """
        Returns a dictionary representation of a Data Factory pipeline.

        Args:
            pipeline_name: Name of the pipeline to load as a ``str``.

        Returns:
            Pipeline definition decorated with linked resources as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
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
        return asdict(translate_pipeline(pipeline))

    def dump(self, pipeline_definition: dict) -> None:
        """
        Note:
            Saving pipeline definitions to Azure Data Factory is not currently supported.

        Args:
            pipeline_definition: Pipeline definition to dump as a ``dict``.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(f"Dump of pipeline {pipeline_definition} to FactoryDefinitionStore not supported.")

    def _append_objects(self, activity: dict) -> dict:
        """
        Attaches datasets and linked services to an activity definition.

        Args:
            activity: Activity payload emitted by the factory service as a ``dict``.

        Returns:
            Activity definition with curated child objects as a ``dict``.
        """
        if self._appenders is None:
            return activity
        for appender in self._appenders:
            activity = appender(activity)
        return activity

    def _append_datasets(self, activity: dict) -> dict:
        """
        Populates referenced datasets for the provided activity.

        Args:
            activity: Activity definition containing dataset references as a ``dict``.

        Returns:
            Activity definition enriched with dataset metadata as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
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
        """
        Populates linked-service metadata for Databricks activities.

        Args:
            activity: Activity definition containing linked-service references as a ``dict``.

        Returns:
            Activity definition enriched with linked-service payloads as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        # Get the linked service reference name:
        linked_service_reference = activity.get("linked_service_name")
        if linked_service_reference is not None:
            linked_service_name = linked_service_reference.get("reference_name")
            if self.factory_client is None:
                raise ValueError("factory_client is not initialized")
            # Get the linked service details from data factory:
            linked_service = self.factory_client.get_linked_service(linked_service_name)
            if linked_service["type"] == "AzureDatabricks":
                activity["linked_service_definition"] = self.factory_client.get_linked_service(linked_service_name)

        # Check the nested "if false" activities:
        if_false_activities = activity.get("if_false_activities")
        if if_false_activities is not None:
            activity["if_false_activities"] = [
                self._append_linked_service(if_false_activity) for if_false_activity in if_false_activities
            ]

        # Check the nested "if true" activities:
        if_true_activities = activity.get("if_true_activities")
        if if_true_activities is not None:
            activity["if_true_activities"] = [
                self._append_linked_service(if_true_activity) for if_true_activity in if_true_activities
            ]

        # Check the nested "for each" activities:
        activities = activity.get("activities")
        if activities is not None:
            activity["activities"] = [self._append_linked_service(activity) for activity in activities]
        return activity

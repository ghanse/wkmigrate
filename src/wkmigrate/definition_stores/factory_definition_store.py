"""This module defines a `FactoryDefinitionStore` class used to load pipeline definitions from Azure Data Factory.

``FactoryDefinitionStore`` connects to an ADF instance, loads pipeline JSON via
the ADF management client, and returns a translated internal representation with
embedded linked services and datasets. It is typically used as the source store
when migrating from ADF to Databricks Workflows.

Example:
    ```python
    from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

    store = FactoryDefinitionStore(
        tenant_id=\"TENANT\",
        client_id=\"CLIENT_ID\",
        client_secret=\"SECRET\",
        subscription_id=\"SUBSCRIPTION\",
        resource_group_name=\"RESOURCE_GROUP\",
        factory_name=\"ADF_NAME\",
    )
    pipeline_dict = store.load(\"my_pipeline\")
    ```
"""

import logging
import os

from dataclasses import dataclass, field
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline

logger = logging.getLogger(__name__)


@dataclass(slots=True)
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
    _factory_client: FactoryClient | None = field(init=False)
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

        self._factory_client = FactoryClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            subscription_id=self.subscription_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )

    def list_pipelines(self) -> list[str]:
        """
        Returns the names of all pipelines available in the Data Factory.

        Returns:
            Pipeline names as a ``list[str]``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self._factory_client is None:
            raise ValueError("Factory client is not initialized")
        return self._factory_client.list_pipelines()

    def load_all(self, pipeline_names: list[str] | None = None) -> list[Pipeline]:
        """
        Loads and translates multiple ADF pipelines.

        When ``pipeline_names`` is ``None`` all pipelines in the factory are
        loaded. Individual pipeline failures are logged and skipped so that
        one broken pipeline does not prevent the rest from being translated.

        Args:
            pipeline_names: Optional list of pipeline names to translate. When
                ``None``, every pipeline in the factory is included.

        Returns:
            Translated ``Pipeline`` objects as a ``list[Pipeline]``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self._factory_client is None:
            raise ValueError("Factory client is not initialized")
        if pipeline_names is None:
            pipeline_names = self.list_pipelines()
        results: list[Pipeline] = []
        with ThreadPoolExecutor(max_workers=self._get_worker_count()) as executor:
            futures = {executor.submit(self.load, name): name for name in pipeline_names}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.warning(f"Could not load pipeline '{futures[future]}'", exc_info=e)
        return results

    def load(self, pipeline_name: str) -> Pipeline:
        """
        Returns an internal ``Pipeline`` representation of a Data Factory pipeline.

        Args:
            pipeline_name: Name of the pipeline to load as a ``str``.

        Returns:
            Pipeline definition decorated with linked resources as a ``Pipeline`` dataclass.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self._factory_client is None:
            raise ValueError("Factory client is not initialized")
        pipeline = self._factory_client.get_pipeline(pipeline_name)
        pipeline["trigger"] = self._factory_client.get_trigger(pipeline_name)
        activities = pipeline.get("activities")
        if activities is not None:
            pipeline["activities"] = [self._append_objects(activity) for activity in activities]
        else:
            pipeline["activities"] = []
        return translate_pipeline(pipeline)

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
                if self._factory_client is None:
                    raise ValueError("Factory client is not initialized")
                activity["input_dataset_definitions"] = [
                    self._factory_client.get_dataset(dataset_name) for dataset_name in dataset_names
                ]
        if "outputs" in activity:
            datasets = activity.get("outputs")
            if datasets is not None:
                dataset_names = [dataset.get("reference_name") for dataset in datasets]
                if self._factory_client is None:
                    raise ValueError("Factory client is not initialized")
                activity["output_dataset_definitions"] = [
                    self._factory_client.get_dataset(dataset_name) for dataset_name in dataset_names
                ]
        if "dataset" in activity:
            dataset_ref = activity.get("dataset")
            if dataset_ref is not None:
                dataset_name = dataset_ref.get("reference_name")
                if self._factory_client is None:
                    raise ValueError("Factory client is not initialized")
                activity["input_dataset_definitions"] = [self._factory_client.get_dataset(dataset_name)]
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
        linked_service_reference = activity.get("linked_service_name")
        if linked_service_reference is not None:
            linked_service_name = linked_service_reference.get("reference_name")
            if self._factory_client is None:
                raise ValueError("Factory client is not initialized")
            activity["linked_service_definition"] = self._factory_client.get_linked_service(linked_service_name)

        if_false_activities = activity.get("if_false_activities")
        if if_false_activities is not None:
            activity["if_false_activities"] = [
                self._append_linked_service(if_false_activity) for if_false_activity in if_false_activities
            ]

        if_true_activities = activity.get("if_true_activities")
        if if_true_activities is not None:
            activity["if_true_activities"] = [
                self._append_linked_service(if_true_activity) for if_true_activity in if_true_activities
            ]

        activities = activity.get("activities")
        if activities is not None:
            activity["activities"] = [self._append_linked_service(activity) for activity in activities]
        return activity

    @staticmethod
    def _get_worker_count() -> int:
        """
        Returns the number of threadpool workers to use.

        Returns:
            Number of threadpool workers as an ``int``.
        """
        cpu_count = os.cpu_count()
        return cpu_count * 2 if cpu_count is not None else 1

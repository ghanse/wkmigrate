"""Definition store backed by an Azure Data Factory instance.

``FactoryDefinitionStore`` connects to an ADF instance via the management API,
loads pipeline JSON, and returns a translated internal representation with
embedded linked services and datasets.

Example:
    ```python
    from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

    store = FactoryDefinitionStore(
        tenant_id="TENANT",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION",
        resource_group_name="RESOURCE_GROUP",
        factory_name="ADF_NAME",
    )
    pipeline = store.load("my_pipeline")
    ```
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.pipeline_adapter import PipelineAdapter
from wkmigrate.enums.source_property_case import SourcePropertyCase
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FactoryDefinitionStore(DefinitionStore):
    """
    Definition store backed by an Azure Data Factory instance (management API).

    Attributes:
        tenant_id: Azure AD tenant identifier.
        client_id: Service principal application (client) ID.
        client_secret: Secret used to authenticate the client.
        subscription_id: Azure subscription identifier.
        resource_group_name: Resource group name for the factory.
        factory_name: Name of the Azure Data Factory instance.
        source_property_case: ``"snake"`` when the API returns snake_case (default);
            ``"camel"`` when the source uses camelCase.
    """

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    subscription_id: str | None = None
    resource_group_name: str | None = None
    factory_name: str | None = None
    source_property_case: SourcePropertyCase = SourcePropertyCase.SNAKE

    _client: FactoryClient | None = field(init=False)
    _adapter: PipelineAdapter | None = field(init=False)

    def __post_init__(self) -> None:
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

        self._client = FactoryClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            subscription_id=self.subscription_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )
        self._adapter = PipelineAdapter(
            get_dataset=self._client.get_dataset,
            get_linked_service=self._client.get_linked_service,
            source_property_case=self.source_property_case,
        )

    def list_pipelines(self) -> list[str]:
        """Return the names of all pipelines in the Data Factory.

        Returns:
            Pipeline names as a ``list[str]``.
        """
        if self._client is None:
            raise ValueError("Factory client is not initialized")
        return self._client.list_pipelines()

    def load(self, pipeline_name: str) -> Pipeline:
        """Load, enrich, and translate a single ADF pipeline by name.

        Args:
            pipeline_name: Name of the pipeline to load.

        Returns:
            Translated ``Pipeline`` IR.
        """
        if self._client is None:
            raise ValueError("Factory client is not initialized")

        if self._adapter is None:
            raise ValueError("Adapter is not initialized")

        pipeline = dict(self._client.get_pipeline(pipeline_name))
        normalized = self._adapter.normalize_casing(pipeline)
        if normalized is not None:
            pipeline = normalized
        trigger = self._client.get_trigger(pipeline_name)

        enriched = self._adapter.adapt(pipeline, trigger)
        return translate_pipeline(enriched)

    def load_all(self, pipeline_names: list[str] | None = None) -> list[Pipeline]:
        """Load and translate multiple pipelines. Failures are logged and skipped.

        Args:
            pipeline_names: Names to translate. When ``None``, all pipelines are loaded.

        Returns:
            Translated ``Pipeline`` objects.
        """
        if pipeline_names is None:
            pipeline_names = self.list_pipelines()
        results: list[Pipeline] = []
        with ThreadPoolExecutor(max_workers=_get_worker_count()) as executor:
            futures = {executor.submit(self.load, name): name for name in pipeline_names}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    logger.warning(f"Could not load pipeline '{futures[future]}'", exc_info=exc)
        return results


def _get_worker_count() -> int:
    """Return the number of threadpool workers to use."""
    cpu_count = os.cpu_count()
    return cpu_count * 2 if cpu_count is not None else 1

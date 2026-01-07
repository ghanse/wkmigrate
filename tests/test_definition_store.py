import pytest

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore


class TestDefinitionStoreContracts:
    """Unit tests for ``FactoryDefinitionStore`` and ``WorkspaceDefinitionStore`` contracts."""

    def test_factory_definition_store_requires_mandatory_fields(self) -> None:
        """FactoryDefinitionStore should validate required configuration fields."""
        with pytest.raises(ValueError):
            FactoryDefinitionStore(  # type: ignore[call-arg]
                tenant_id=None,
                client_id=None,
                client_secret=None,
                subscription_id=None,
                resource_group_name=None,
                factory_name=None,
            )

    def test_workspace_definition_store_requires_auth_and_host(self) -> None:
        """WorkspaceDefinitionStore should validate authentication type and host name."""
        with pytest.raises(ValueError):
            WorkspaceDefinitionStore(  # type: ignore[call-arg]
                authentication_type="invalid",
                host_name=None,
            )

    def test_factory_definition_store_uses_definition_store_interface(
        self,
        mock_factory_client,
    ) -> None:
        """FactoryDefinitionStore should behave as a DefinitionStore when wired with a mock client."""
        assert mock_factory_client is not None

        store = FactoryDefinitionStore(
            tenant_id="TENANT_ID",
            client_id="CLIENT_ID",
            client_secret="SECRET",
            subscription_id="SUBSCRIPTION_ID",
            resource_group_name="RESOURCE_GROUP",
            factory_name="FACTORY_NAME",
        )

        assert isinstance(store, DefinitionStore)
        pipeline = store.load("TEST_PIPELINE_NAME")
        assert isinstance(pipeline, dict)

    def test_workspace_definition_store_uses_definition_store_interface(
        self,
        mock_workspace_client,
    ) -> None:
        """WorkspaceDefinitionStore should behave as a DefinitionStore when wired with a mock workspace client."""
        assert mock_workspace_client is not None

        store = WorkspaceDefinitionStore(
            authentication_type="pat",
            host_name="https://example.com",
            pat="DUMMY_TOKEN",
        )

        assert isinstance(store, DefinitionStore)
        # The mocked jobs API starts empty; loading a non-existent workflow should raise a ValueError.
        with pytest.raises(ValueError):
            store.load("WORKFLOW")

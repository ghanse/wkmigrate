from contextlib import nullcontext as does_not_raise
import pytest

from wkmigrate.definition_stores.definition_store_builder import build_definition_store
from wkmigrate.definition_stores.definition_store import DefinitionStore


DEFINITION_STORE_CASES = [
    # FactoryDefinitionStore: missing options
    ("factory_definition_store", None, pytest.raises(ValueError)),
    # FactoryDefinitionStore: minimal valid options
    (
        "factory_definition_store",
        {
            "tenant_id": "TENANT_ID",
            "client_id": "CLIENT_ID",
            "client_secret": "SECRET",
            "subscription_id": "SUBSCRIPTION_ID",
            "resource_group_name": "RESOURCE_GROUP",
            "factory_name": "FACTORY_NAME",
        },
        does_not_raise(),
    ),
    # WorkspaceDefinitionStore: missing options
    ("workspace_definition_store", None, pytest.raises(ValueError)),
    # WorkspaceDefinitionStore: minimal valid options
    (
        "workspace_definition_store",
        {
            "authentication_type": "pat",
            "host_name": "https://example.com",
            "pat": "DUMMY_TOKEN",
        },
        does_not_raise(),
    ),
    # Edge cases: unknown / empty type
    ("invalid_definition_store", {}, pytest.raises(ValueError)),
    ("", None, pytest.raises(ValueError)),
]


class TestDefinitionStoreBuilder:
    """Unit tests for the build_definition_store method."""

    @pytest.mark.parametrize(
        "definition_store_type, definition_store_options, expected_result",
        DEFINITION_STORE_CASES,
    )
    def test_definition_store_builder_returns_object(
        self,
        definition_store_type: str,
        definition_store_options: dict,
        expected_result: Exception,
        mock_factory_client,
    ) -> None:
        """Tests that the definition store builder returns a non-null object."""
        assert mock_factory_client is not None
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options,
            )
            assert store is not None, 'Method build_definition_store returned "None"'

    @pytest.mark.parametrize(
        "definition_store_type, definition_store_options, expected_result",
        DEFINITION_STORE_CASES,
    )
    def test_definition_store_builder_returns_valid_type(
        self,
        definition_store_type: str,
        definition_store_options: dict,
        expected_result: callable,
        mock_factory_client,
    ) -> None:
        """Tests that the definition store builder returns a DefinitionStore object."""
        assert mock_factory_client is not None
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options,
            )
            assert isinstance(
                store, DefinitionStore
            ), 'Method build_definition_store did not return a "DefinitionStore" object'

import pytest
import wkmigrate
from contextlib import nullcontext as does_not_raise
from wkmigrate.definition_stores.definition_store_builder import build_definition_store
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.enums.json_source_type import JSONSourceType


class TestDefinitionStoreBuilder:
    """Unit tests for the build_definition_store method."""

    @pytest.mark.parametrize(
        "definition_store_type, definition_store_options, expected_result",
        [
            ("factory_definition_store", {"_use_test_client": True}, does_not_raise()),
            (
                "factory_definition_store",
                {"_use_test_client": False},
                pytest.raises(ValueError),
            ),
            ("factory_definition_store", None, pytest.raises(ValueError)),
            (
                "factory_definition_store",
                {"tenant_id": "TENANT_ID"},
                pytest.raises(ValueError),
            ),
            (
                "factory_definition_store",
                {"tenant_id": "TENANT_ID", "client_id": "CLIENT_ID"},
                pytest.raises(ValueError),
            ),
            (
                "factory_definition_store",
                {
                    "tenant_id": "TENANT_ID",
                    "client_id": "CLIENT_ID",
                    "client_secret": "SECRET",
                },
                pytest.raises(ValueError),
            ),
            (
                "workspace_definition_store",
                {"_use_test_client": True},
                does_not_raise(),
            ),
            (
                "workspace_definition_store",
                {"_use_test_client": False},
                pytest.raises(ValueError),
            ),
            ("workspace_definition_store", None, pytest.raises(ValueError)),
            (
                "workspace_definition_store",
                {"host_name": "test_host_name"},
                pytest.raises(ValueError),
            ),
            (
                "workspace_definition_store",
                {"authentication_type": "pat"},
                pytest.raises(ValueError),
            ),
            (
                "workspace_definition_store",
                {"authentication_type": None},
                pytest.raises(ValueError),
            ),
            (
                "json_definition_store",
                {"json_file_path": f"{wkmigrate.JSON_PATH}/test_pipelines.json"},
                does_not_raise(),
            ),
            (
                "json_definition_store",
                {
                    "json_file_path": f"{wkmigrate.JSON_PATH}/test_workflows.json",
                    "json_source_type": JSONSourceType.DATABRICKS_WORKFLOW,
                },
                does_not_raise(),
            ),
            (
                "json_definition_store",
                {"json_file_path": f"{wkmigrate.JSON_PATH}/INVALID_PATH"},
                pytest.raises(ValueError),
            ),
            ("invalid_definition_store", {}, pytest.raises(ValueError)),
            ("", None, pytest.raises(ValueError)),
        ],
    )
    def test_definition_store_builder_returns_object(
        self,
        definition_store_type: str,
        definition_store_options: dict,
        expected_result: Exception,
    ) -> None:
        """Tests that the definition store builder returns a non-null object."""
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options,
            )
            assert store is not None, 'Method build_definition_store returned "None"'

    @pytest.mark.parametrize(
        "definition_store_type, definition_store_options, expected_result",
        [
            ("factory_definition_store", {"_use_test_client": True}, does_not_raise()),
            (
                "factory_definition_store",
                {"_use_test_client": False},
                pytest.raises(ValueError),
            ),
            ("factory_definition_store", None, pytest.raises(ValueError)),
            (
                "factory_definition_store",
                {"tenant_id": "TENANT_ID"},
                pytest.raises(ValueError),
            ),
            (
                "factory_definition_store",
                {"tenant_id": "TENANT_ID", "client_id": "CLIENT_ID"},
                pytest.raises(ValueError),
            ),
            (
                "factory_definition_store",
                {
                    "tenant_id": "TENANT_ID",
                    "client_id": "CLIENT_ID",
                    "client_secret": "SECRET",
                },
                pytest.raises(ValueError),
            ),
            (
                "workspace_definition_store",
                {"_use_test_client": True},
                does_not_raise(),
            ),
            (
                "workspace_definition_store",
                {"_use_test_client": False},
                pytest.raises(ValueError),
            ),
            ("workspace_definition_store", None, pytest.raises(ValueError)),
            (
                "workspace_definition_store",
                {"host_name": "test_host_name"},
                pytest.raises(ValueError),
            ),
            (
                "workspace_definition_store",
                {"authentication_type": "pat"},
                pytest.raises(ValueError),
            ),
            (
                "workspace_definition_store",
                {"authentication_type": None},
                pytest.raises(ValueError),
            ),
            (
                "json_definition_store",
                {"json_file_path": f"{wkmigrate.JSON_PATH}/test_pipelines.json"},
                does_not_raise(),
            ),
            (
                "json_definition_store",
                {
                    "json_file_path": f"{wkmigrate.JSON_PATH}/test_workflows.json",
                    "json_source_type": JSONSourceType.DATABRICKS_WORKFLOW,
                },
                does_not_raise(),
            ),
            (
                "json_definition_store",
                {"json_file_path": f"{wkmigrate.JSON_PATH}/INVALID_PATH.json"},
                pytest.raises(ValueError),
            ),
            ("invalid_definition_store", {}, pytest.raises(ValueError)),
            ("", None, pytest.raises(ValueError)),
        ],
    )
    def test_definition_store_builder_returns_valid_type(
        self,
        definition_store_type: str,
        definition_store_options: dict,
        expected_result: callable,
    ) -> None:
        """Tests that the definition store builder returns a DefinitionStore object."""
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options,
            )
            assert isinstance(
                store, DefinitionStore
            ), 'Method build_definition_store did not return a "DefinitionStore" object'

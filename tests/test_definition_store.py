import pytest
import wkmigrate
from contextlib import nullcontext as does_not_raise
from typing import Any
from wkmigrate.definition_stores.definition_store_builder import build_definition_store
from wkmigrate.enums.json_source_type import JSONSourceType


class TestDefinitionStore:
    """ Unit tests for the ``DefinitionStore`` class and subclasses."""

    @pytest.mark.parametrize('definition_store_type, definition_store_options, expected_result', [
        # FactoryDefinitionStore tests:
            ('factory_definition_store', {'_use_test_client': True}, does_not_raise()),
            ('factory_definition_store', None, pytest.raises(ValueError)),
            ('factory_definition_store', {'_use_test_client': False}, pytest.raises(ValueError)),
        # WorkspaceDefinitionStore tests:
            ('workspace_definition_store', {'_use_test_client': True}, does_not_raise()),
            ('workspace_definition_store', {'_use_test_client': False}, pytest.raises(ValueError)),
            ('workspace_definition_store', None, pytest.raises(ValueError)),
        # JSONDefinitionStore tests:
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_pipelines.json'}, does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_workflows.json',
                                       'json_source_type': JSONSourceType.DATABRICKS_WORKFLOW}, does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/INVALID_PATH.json'},
                pytest.raises(ValueError)),
        # YAMLDefinitionStore tests:
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/test_workflows.yaml'}, does_not_raise()),
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/INVALID_PATH.json'},
                pytest.raises(ValueError)),
        # Edge case tests:
            ('invalid_definition_store', {}, pytest.raises(ValueError)),
            ('', None, pytest.raises(ValueError))
        ])
    def test_definition_store_has_load(
            self,
            definition_store_type: str,
            definition_store_options: dict,
            expected_result: callable
    ) -> None:
        """ Tests that the ``DefinitionStore`` object implements the ``load`` method."""
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options
            )
            assert hasattr(store, 'load'), 'DefinitionStore object does not implement the "load" method'

    @pytest.mark.parametrize('definition_store_type, definition_store_options, pipeline_to_load, expected_result', [
        # FactoryDefinitionStore tests:
            ('factory_definition_store', {'_use_test_client': True}, 'TEST_PIPELINE_NAME', does_not_raise()),
            ('factory_definition_store', {'_use_test_client': True}, 'test_adf_pipeline_2', does_not_raise()),
            ('factory_definition_store', {'_use_test_client': True}, 'test_pipeline_no_triggers',
                pytest.raises(ValueError)),
            ('factory_definition_store', {'_use_test_client': True}, 'test_pipeline_invalid_linked_service',
                pytest.raises(ValueError)),
            ('factory_definition_store', {'_use_test_client': True}, 'INVALID_PIPELINE_NAME',
                pytest.raises(ValueError)),
            ('factory_definition_store', {'_use_test_client': False}, 'TEST_PIPELINE_NAME', pytest.raises(ValueError)),
            ('factory_definition_store', None, 'TEST_PIPELINE_NAME', pytest.raises(ValueError)),
        # WorkspaceDefinitionStore tests:
            ('workspace_definition_store', {'_use_test_client': True}, 'TEST_PIPELINE_NAME', does_not_raise()),
            ('workspace_definition_store', {'_use_test_client': True}, 'INVALID_PIPELINE_NAME',
                pytest.raises(ValueError)),
            ('workspace_definition_store', {'_use_test_client': False}, 'TEST_PIPELINE_NAME',
                pytest.raises(ValueError)),
            ('workspace_definition_store', None, 'TEST_PIPELINE_NAME', pytest.raises(ValueError)),
        # JSONDefinitionStore tests:
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_pipelines.json'}, 'test_adf_pipeline_2',
                does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_pipelines.json'}, 'INVALID_PIPELINE_NAME',
                pytest.raises(ValueError)),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/INVALID_PATH.json'},
                'INVALID_PIPELINE_NAME', pytest.raises(ValueError)),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_workflows.json',
                'json_source_type': JSONSourceType.DATABRICKS_WORKFLOW}, 'TEST_PIPELINE_NAME', does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_workflows.json',
                'json_source_type': JSONSourceType.DATABRICKS_WORKFLOW},
                'INVALID_PIPELINE_NAME', pytest.raises(ValueError)),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/INVALID_PATH.json'},
                'TEST_PIPELINE_NAME', pytest.raises(ValueError)),
        # YAMLDefinitionStore tests:
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/test_workflows.yaml'}, 'test_adf_pipeline', does_not_raise()),
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/test_workflows.yaml'}, 'INVALID_WORKFLOW_NAME',
                pytest.raises(ValueError)),
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/INVALID_PATH.json'}, 'test_adf_pipeline',
                pytest.raises(ValueError)),
        # Edge case tests:
            ('invalid_definition_store', {}, 'TEST_PIPELINE_NAME', pytest.raises(ValueError)),
            ('', None, 'TEST_PIPELINE_NAME', pytest.raises(ValueError))
        ])
    def test_definition_store_load_returns_valid_object(
            self,
            definition_store_type: str,
            definition_store_options: dict,
            pipeline_to_load: Any,
            expected_result: callable
    ) -> None:
        """ Tests that the DefinitionStore's ``load`` method returns a ``dict``."""
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options
            )
            pipeline = store.load(pipeline_to_load)
            assert isinstance(pipeline, dict), 'DefinitionStore "load" method did not return a "dict"'

    @pytest.mark.parametrize('definition_store_type, definition_store_options, expected_result', [
        # FactoryDefinitionStore tests:
            ('factory_definition_store', {'_use_test_client': True}, does_not_raise()),
            ('factory_definition_store', {'_use_test_client': False}, pytest.raises(ValueError)),
            ('factory_definition_store', None, pytest.raises(ValueError)),
        # WorkspaceDefinitionStore tests:
            ('workspace_definition_store', {'_use_test_client': True}, does_not_raise()),
            ('workspace_definition_store', {'_use_test_client': False}, pytest.raises(ValueError)),
            ('workspace_definition_store', None, pytest.raises(ValueError)),
        # JSONDefinitionStore tests:
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_pipelines.json'}, does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_workflows.json',
                                       'json_source_type': JSONSourceType.DATABRICKS_WORKFLOW}, does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/INVALID_PATH.json'},
                pytest.raises(ValueError)),
        # YAMLDefinitionStore tests:
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/test_workflows.yaml'}, does_not_raise()),
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/INVALID_PATH.json'},
                pytest.raises(ValueError)),
        # Edge case tests:
            ('invalid_definition_store', {}, pytest.raises(ValueError)),
            ('', None, pytest.raises(ValueError))
        ])
    def test_definition_store_has_dump(
            self,
            definition_store_type: str,
            definition_store_options: dict,
            expected_result: callable
    ) -> None:
        """ Tests that the definition store builder returns a ``DefinitionStore`` object that implements the
            ``dump`` method."""
        with expected_result:
            store = build_definition_store(definition_store_type=definition_store_type,
                                           options=definition_store_options)
            assert hasattr(store, 'dump'), 'DefinitionStore object does not implement the "dump" method'

    @pytest.mark.parametrize('definition_store_type, definition_store_options, pipeline_to_dump, expected_result', [
        # FactoryDefinitionStore tests:
            ('factory_definition_store', {'_use_test_client': True}, {}, pytest.warns(UserWarning)),
            ('factory_definition_store', None, 'test_pipeline', pytest.raises(ValueError)),
        # WorkspaceDefinitionStore tests:
            ('workspace_definition_store', {'_use_test_client': True}, {}, does_not_raise()),
            ('workspace_definition_store', None, 'test_pipeline', pytest.raises(ValueError)),
        # JSONDefinitionStore tests:
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/test_output.json',
                                       'json_source_type': JSONSourceType.DATABRICKS_WORKFLOW}, {'name': 'TEST_PIPELINE'}, does_not_raise()),
            ('json_definition_store', {'json_file_path': f'{wkmigrate.JSON_PATH}/INVALID_PATH.json'}, {'name': 'TEST_PIPELINE'},
                pytest.raises(ValueError)),
        # YAMLDefinitionStore tests:
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/test_output.yaml'}, {'name': 'TEST_WORKFLOW'}, does_not_raise()),
            ('yaml_definition_store', {'yaml_file_path': f'{wkmigrate.YAML_PATH}/INVALID_PATH.json'},
                {'name': 'TEST_WORKFLOW'}, pytest.raises(ValueError)),
        # Edge case tests:
            ('invalid_definition_store', {}, 'test_pipeline', pytest.raises(ValueError)),
            ('', None, 'test_pipeline', pytest.raises(ValueError))
        ])
    def test_definition_store_dump(
            self,
            definition_store_type: str,
            definition_store_options: dict,
            pipeline_to_dump: Any,
            expected_result: callable
    ) -> None:
        """ Tests that the DefinitionStore's ``dump`` method succeeds."""
        with expected_result:
            store = build_definition_store(
                definition_store_type=definition_store_type,
                options=definition_store_options
            )
            store.dump(pipeline_to_dump)
            assert True, 'DefinitionStore "dump" method failed'

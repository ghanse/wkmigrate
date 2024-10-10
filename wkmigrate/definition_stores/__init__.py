from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.json_definition_store import JSONDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.definition_stores.yaml_definition_store import YAMLDefinitionStore

types = {
    'factory_definition_store': FactoryDefinitionStore,
    'json_definition_store': JSONDefinitionStore,
    'workspace_definition_store': WorkspaceDefinitionStore,
    'yaml_definition_store': YAMLDefinitionStore
}

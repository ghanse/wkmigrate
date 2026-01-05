from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore

# Create the factory store and get the ADF pipeline:
factory_options = {
    'tenant_id': "<YOUR AZURE TENANT ID>",
    'client_id': "<YOUR AZURE SERVICE PRINCIPAL CLIENT ID>",  # NOTE: Should have `data-factory-contributor` access assigned to your ADF resource
    'client_secret': "<YOUR AZURE SERVICE PRINCIPAL CLIENT SECRET>",
    'subscription_id': "<YOUR AZURE SUBSCRIPTION ID>",  # NOTE: Should contain your ADF resource
    'resource_group_name': "<YOUR AZURE RESOURCE GROUP NAME>",  # NOTE: Should contain your ADF resource
    'factory_name': "<YOUR ADF RESOURCE NAME>",
}
factory_definition_store = FactoryDefinitionStore(**factory_options)
pipeline = factory_definition_store.load('<YOUR ADF PIPELINE NAME>')

# Print the ADF pipeline represented as a dictionary:
print(pipeline)

# Create the workspace definition store:
workspace_options = {
    "host_name": "<YOUR DATABRICKS WORKSPACE URL>",
    "pat": "<YOUR DATABRICKS PERSONAL ACCESS TOKEN (PAT)>",
    "authentication_type": "pat",
}
workspace_definition_store = WorkspaceDefinitionStore(**workspace_options)

# Dump the translated pipeline to the local filesystem:
workspace_definition_store.to_local_files(pipeline, local_directory="<YOUR LOCAL DIRECTORY>")

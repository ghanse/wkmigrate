from wkmigrate.definition_stores.definition_store_builder import build_definition_store

# Create the factory store and get the ADF pipeline:
factory_options = {
    'tenant_id': "<Azure Tenant ID>",
    'client_id': "<Azure Service Principal Client ID>",
    'client_secret': "<Azure Service Principal Client Secret>",
    'subscription_id': "<Azure Subscription ID>",
    'resource_group_name': "<Azure Data Factory Resource Group Name>",
    'factory_name': "<Azure Data Factory Resource Name>",
}
factory_definition_store = build_definition_store('factory_definition_store', factory_options)
pipeline = factory_definition_store.load('pipeline1')

# Print the ADF pipeline represented as a dictionary:
print(pipeline)


workspace_options = {
    "host_name": "<Databricks Host URL>",
    "pat": "<Databricks Personal Access Token (PAT)>",
    "authentication_type": "pat",
}
workspace_definition_store = build_definition_store("workspace_definition_store", workspace_options)
workspace_definition_store.dump(pipeline)

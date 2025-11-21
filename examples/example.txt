from wkmigrate.definition_stores.definition_store_builder import build_definition_store

# Create the factory store and get the ADF pipeline:
factory_options = {
    'tenant_id': "<YOUR AZURE TENANT ID>",
    'client_id': "<YOUR AZURE SERVICE PRINCIPAL CLIENT ID>",  # NOTE: Should have `data-factory-contributor` access assigned to your ADF resource
    'client_secret': "<YOUR AZURE SERVICE PRINCIPAL CLIENT SECRET>",
    'subscription_id': "<YOUR AZURE SUBSCRIPTION ID>",  # NOTE: Should contain your ADF resource
    'resource_group_name': "<YOUR AZURE RESOURCE GROUP NAME>",  # NOTE: Should contain your ADF resource
    'factory_name': "<YOUR ADF RESOURCE NAME>",
}
factory_definition_store = build_definition_store('factory_definition_store', factory_options)
pipeline = factory_definition_store.load('<YOUR ADF PIPELINE NAME>')

# Print the ADF pipeline represented as a dictionary:
print(pipeline)


workspace_options = {
    "host_name": "<YOUR DATABRICKS WORKSPACE URL>",
    "pat": "<YOUR DATABRICKS PERSONAL ACCESS TOKEN (PAT)>",
    "authentication_type": "pat",
}
workspace_definition_store = build_definition_store("workspace_definition_store", workspace_options)
workspace_definition_store.dump(pipeline)

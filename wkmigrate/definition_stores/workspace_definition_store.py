""" This module defines the ``DatabricksWorkspaceDefinitionStore`` class."""
from dataclasses import dataclass, field
from typing import Optional
from wkmigrate.clients.workspace_client import DatabricksWorkspaceClient, \
    WorkspaceManagementClient, WorkspaceTestClient
from wkmigrate.definition_stores.definition_store import DefinitionStore


@dataclass
class WorkspaceDefinitionStore(DefinitionStore):
    """This class is used to list, describe, and update objects in a Databricks workspace."""
    authentication_type: Optional[str] = None
    host_name: Optional[str] = None
    pat: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    resource_id: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    workspace_client: Optional[DatabricksWorkspaceClient] = field(init=False)
    _use_test_client: Optional[bool] = False
    _valid_authentication_types = ['pat', 'basic', 'azure-client-secret']

    def __post_init__(self) -> None:
        """ Sets up the workspace client for the provided authentication credentials."""
        if self._use_test_client:
            self.workspace_client = WorkspaceTestClient()
            return
        if self.authentication_type not in self._valid_authentication_types:
            raise ValueError(
                'Invalid value for "self.authentication_type"; Must be "pat", "basic", or "azure-client-secret"')
        if self.host_name is None:
            raise ValueError(
                '"host_name" must be provided when creating a WorkspaceDefinitionStore'
            )
        self.workspace_client = WorkspaceManagementClient(
            self.authentication_type,
            self.host_name,
            self.pat,
            self.username,
            self.password,
            self.resource_id,
            self.tenant_id,
            self.client_id,
            self.client_secret
        )

    def load(self, job_name: str) -> dict:
        """ Gets a dictionary representation of a Databricks workflow from the Databricks workspace.
            :parameter job_name: Job name for the specified workflow
            :return: Workflow definition as a ``dict``
        """
        return self.workspace_client.get_workflow(job_name=job_name)

    def dump(self, job_settings: dict) -> int:
        """ Creates workflow in the Databricks workspace with the specified definition.
            :parameter job_settings: Workflow definition as a ``dict``
            :return: ``None``
        """
        job_definition = {'settings': job_settings}
        return self.workspace_client.create_workflow(job_definition=job_definition)

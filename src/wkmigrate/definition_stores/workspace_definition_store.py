"""This module defines the ``DatabricksWorkspaceDefinitionStore`` class."""

from dataclasses import dataclass, field
from wkmigrate.clients.workspace_client import (
    DatabricksWorkspaceClient,
    WorkspaceManagementClient,
    WorkspaceTestClient,
)
from wkmigrate.definition_stores.definition_store import DefinitionStore


@dataclass
class WorkspaceDefinitionStore(DefinitionStore):
    """Lists, describes, and updates objects in a Databricks workspace."""

    authentication_type: str | None = None
    host_name: str | None = None
    pat: str | None = None
    username: str | None = None
    password: str | None = None
    resource_id: str | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    files_to_delta_sinks: bool | None = True
    workspace_client: DatabricksWorkspaceClient | None = field(init=False)
    _use_test_client: bool | None = False
    _valid_authentication_types = ["pat", "basic", "azure-client-secret"]

    def __post_init__(self) -> None:
        if self._use_test_client:
            self.workspace_client = WorkspaceTestClient()
            return
        if self.authentication_type not in self._valid_authentication_types:
            raise ValueError(
                'Invalid value for "self.authentication_type"; Must be "pat", "basic", or "azure-client-secret"'
            )
        if self.host_name is None:
            raise ValueError('"host_name" must be provided when creating a WorkspaceDefinitionStore')
        self.workspace_client = WorkspaceManagementClient(
            self.authentication_type,
            self.host_name,
            self.pat,
            self.username,
            self.password,
            self.resource_id,
            self.tenant_id,
            self.client_id,
            self.client_secret,
        )

    def load(self, job_name: str) -> dict:
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        job = self.workspace_client.get_workflow(job_name=job_name)
        return job.as_dict()

    def dump(self, job_settings: dict) -> int:
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        job_definition = {"settings": job_settings}
        job_id = self.workspace_client.create_workflow(job_definition=job_definition)
        if job_id is None:
            raise ValueError("Failed to create workflow")
        return job_id

    def to_local_files(self, job_settings: dict, local_directory: str) -> None:
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        job_definition = {"settings": job_settings}
        self.workspace_client.create_workflow(
            job_definition=job_definition,
            create_in_workspace=False,
            local_directory=local_directory,
        )

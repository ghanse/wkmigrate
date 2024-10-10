""" Defines ``WorkspaceClient`` classes."""
import base64
import json
import warnings
import wkmigrate
from abc import ABC
from dataclasses import dataclass, field
from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob, CronSchedule, Job, Task
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language


class DatabricksWorkspaceClient(ABC):
    """ A client implementing methods for getting data pipeline, linked service,
        dataset, and pipeline trigger definitions.
    """
    __test__ = False

    def get_workflow(self, job_name: str) -> dict:
        pass

    def create_workflow(self, job_definition: dict) -> int | None:
        pass


@dataclass
class WorkspaceManagementClient(DatabricksWorkspaceClient):
    """A client used to list, describe, and update objects in a Databricks workspace."""
    __test__ = False

    authentication_type: str
    host_name: str
    pat: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    resource_id: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    workspace_client: Optional[WorkspaceClient] = field(init=False)

    def __post_init__(self) -> None:
        """ Sets up the workspace client for the provided authentication credentials."""
        if self.authentication_type == 'pat':
            self.workspace_client = self._login_with_pat()
            return
        if self.authentication_type == 'basic':
            self.workspace_client = self._login_with_basic_auth()
            return
        if self.authentication_type == 'azure-client-secret':
            self.workspace_client = self._login_with_azure_client_secret()
            return
        raise ValueError(
            'Got an invalid value for "self.authentication_type", must be "pat", "basic", or "azure-client-secret"')

    def _login_with_pat(self) -> WorkspaceClient:
        """ Creates a ``WorkspaceClient`` with PAT authentication.
            :return: A ``WorkspaceClient`` from the Databricks SDK
        """
        if self.pat is None:
            raise ValueError('No value provided for "pat" with access token authentication')
        return WorkspaceClient(
            auth_type=self.authentication_type,
            host=self.host_name,
            token=self.pat
        )

    def _login_with_basic_auth(self) -> WorkspaceClient:
        """ Creates a ``WorkspaceClient`` with basic authentication.
            :return: A ``WorkspaceClient`` from the Databricks SDK
        """
        if self.username is None:
            raise ValueError('No value provided for "username" with basic authentication')
        if self.password is None:
            raise ValueError('No value provided for "password" with basic authentication')
        return WorkspaceClient(
            auth_type=self.authentication_type,
            host=self.host_name,
            username=self.username,
            password=self.password
        )

    def _login_with_azure_client_secret(self) -> WorkspaceClient:
        """ Creates a ``WorkspaceClient`` with Azure client secret authentication.
            :return: A ``WorkspaceClient`` from the Databricks SDK
        """
        if self.resource_id is None:
            raise ValueError('No value provided for "resource_id" with Azure client secret authentication')
        if self.tenant_id is None:
            raise ValueError('No value provided for "tenant_id" with Azure client secret authentication')
        if self.client_id is None:
            raise ValueError('No value provided for "client_id" with Azure client secret authentication')
        if self.client_secret is None:
            raise ValueError('No value provided for "client_secret" with Azure client secret authentication')
        return WorkspaceClient(
            auth_type=self.authentication_type,
            host=self.host_name,
            azure_workspace_resource_id=self.resource_id,
            azure_tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )

    def get_workflow(self, job_id: Optional[int] = None, job_name: Optional[str] = None) -> Job:
        """ Gets a workflow with the specified ID or name as a ``Job`` object.
            :parameter job_id: Job ID for the specified workflow
            :parameter job_name: Job name for the specified workflow
            :return: Workflow definition as a ``Job``
        """
        # Check the input parameters:
        if job_id is None and job_name is None:
            raise ValueError('Must provide a value for "job_id" or "job_name".')
        # If a workflow ID is specified, get the workflow by ID:
        if job_id is not None:
            return self.workspace_client.jobs.get(job_id=job_id)
        # Otherwise, list the workflows by name:
        workflows = self._list_workflows_by_name(job_name=job_name)
        # If more than 1 workflow exists with the specified name:
        if len(workflows) > 1:
            raise ValueError(f'Duplicate workflows found in the target workspace with name "{job_name}"')
        return self.workspace_client.jobs.get(job_id=workflows[0].job_id)

    def _list_workflows_by_name(self, job_name: str) -> list[BaseJob]:
        """ Gets workflows with the specified name as ``BaseJob`` objects.
            :return: Workflow definitions as a ``list[BaseJob]``
        """
        # List the workflows:
        workflows = list(self.workspace_client.jobs.list(name=job_name))
        # If no workflows were found:
        if workflows is None or len(workflows) == 0:
            raise ValueError(f'No workflows found in the target workspace with name "{job_name}"')
        return workflows

    def create_workflow(self, job_definition: dict) -> int:
        """ Creates a workflow with the specified definition as a ``dict``.
            :parameter job_definition: Workflow definition settings
            :return: Created Job ID as an ``int``
        """
        job_settings = job_definition.get('settings')
        if job_settings is None:
            raise ValueError('Invalid "job_definition" object.')
        job_name = job_settings.get('name', None)
        if job_name is None:
            raise ValueError('No value provided for "name"')
        else:
            access_control_list = job_settings.get('access_control_list', None)
            is_continuous = job_settings.get('is_continuous', None)
            deployment = job_settings.get('deployment', None)
            edit_mode = job_settings.get('edit_mode', None)
            email_notifications = job_settings.get('email_notifications', None)
            git_source = job_settings.get('git_source', None)
            health_rules = job_settings.get('health_rules', None)
            max_concurrent_runs = job_settings.get('max_concurrent_runs', None)
            notification_settings = job_settings.get('notification_settings', None)
            parameter_definitions = job_settings.get('parameter_definitions', None)
            queue = job_settings.get('queue', None)
            run_as_principal = job_settings.get('run_as_principal', None)
            webhook_notifications = job_settings.get('webhook_notifications', None)
            response = (
                self.workspace_client.jobs.create(
                    name=job_settings.get('name'),
                    description=job_settings.get('description'),
                    schedule=WorkspaceManagementClient._get_schedule(job_settings.get('schedule')),
                    tags=job_settings.get('tags'),
                    tasks=[self._create_task(task) for task in job_settings.get('tasks')],
                    timeout_seconds=job_settings.get('timeout_seconds')
                )
            )
            return response.job_id

    def _create_task(self, task: dict) -> Task:
        """ Creates a Databricks workflow ``Task`` object from the task definition.
            :parameter task: Workflow task definition as a ``dict``
            :return: Workflow ``Task`` object
        """
        if 'type' not in task:
            raise ValueError('Task has no "type"')
        if task.get('type') == 'DatabricksNotebook':
            self._create_notebook_task_dependencies(task)
        if task.get('type') == 'Copy':
            notebook_path = self._create_copy_task_dependencies(task)
            task.pop('copy_data_task')
            task['notebook_task'] = {'notebook_path': notebook_path}
        if task.get('type') == 'ForEach':
            return self._create_for_each_task(task)
        return Task.from_dict(task)

    def _create_copy_task_dependencies(self, task: dict) -> str:
        copy_data_task = task.get('copy_data_task')
        source_definition = copy_data_task.get('source_dataset_definition')[0]
        sink_definition = copy_data_task.get('sink_dataset_definition')[0]
        column_mapping = copy_data_task.get('column_mapping')
        notebook_path = self._create_copy_data_notebook(source_definition, sink_definition, column_mapping)
        return notebook_path

    def _create_copy_data_notebook(self, source_definition: dict, sink_definition: dict,
                                   column_mapping: dict) -> str:
        """ Creates a notebook in the target workspace to copy data between specified data source and sink with
            the given column mapping.
            :parameter source_definition: Source dataset definition as a ``dict``
            :parameter sink_definition: Sink dataset definition as a ``dict``
            :parameter column_mapping: Column-level mapping as a ``dict``
            :return: Databricks notebook path in the target workspace as a ``str``
        """
        script_lines = [
            '# Databricks notebook source',
            'import pyspark.sql.types as T',
            'import pyspark.sql.functions as F',
            'data_source_options = {}'
        ]
        # Get the source properties:
        source_dataset_name = source_definition.get('name')
        source_service = source_definition.get('linked_service_definition')
        source_service_name = source_service.get('name')
        source_properties = source_definition.get('properties')
        source_schema = source_properties.get('schema_type_properties_schema')
        source_table = source_properties.get('table')
        # Append code blocks to get source dataset options from the Databricks secret scope:
        option_mappings = {'server': 'host', 'database': 'database', 'user_name': 'user_name', 'credential': 'password'}
        script_lines.extend([
            f'''data_source_options["{option_key}"] = dbutils.secrets.get(
                    scope="wkmigrate_credentials_scope", 
                    key="{source_service_name}_{property_key}"
                )
            '''
            for property_key, option_key in option_mappings.items()
        ])
        # Append a code block to read the source as a DataFrame:
        script_lines.append(
            f'''{source_dataset_name}_df = ( 
                    spark.read.format("sqlserver")
                        .options(**data_source_options)
                        .option("dbtable", "{source_schema}.{source_table}")
                        .load()
                    )
            '''
        )
        # Get the sink properties:
        sink_dataset_name = sink_definition.get('name')
        sink_properties = sink_definition.get('properties')
        sink_database = sink_properties.get('database')
        sink_table = sink_properties.get('table')
        # Append code blocks to create a new DataFrame with mapped column names and data types:
        mapping_expressions = [
            f'"cast({mapping["source_column_name"]} as {mapping["sink_column_type"]}) as {mapping["sink_column_name"]}"'
            for mapping in column_mapping
        ]
        script_lines.append(
            f'''{sink_dataset_name}_df = {source_dataset_name}_df.selectExpr({
                ", \n\t".join(mapping_expressions)
                })    
            '''
        )
        # Append a code block to write the DataFrame to Delta:
        script_lines.append(
            f'''({sink_dataset_name}_df.write.format("delta")
                        .mode("overwrite")
                        .saveAsTable("hive_metastore.{sink_database}.{sink_table}")
                )
            '''
        )
        # Create and upload the script as a Python notebook:
        notebook_str = '\n'.join(script_lines)
        notebook_path = f'/wkmigrate/copy_data_notebooks/copy_{source_dataset_name}'
        self.workspace_client.workspace.mkdirs('/wkmigrate/copy_data_notebooks')
        self.workspace_client.workspace.import_(
            content=base64.b64encode(notebook_str.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
            path=notebook_path
        )
        return notebook_path

    def _create_data_source_secrets(self, source_service: dict) -> list[str]:
        secret_keys = []
        source_service_name = source_service.get('name')
        properties = source_service.get('properties')
        scopes = [scope.name for scope in self.workspace_client.secrets.list_scopes()]
        if 'wkmigrate_credentials_scope' not in scopes:
            self.workspace_client.secrets.create_scope(scope='wkmigrate_credentials_scope')
        for property_key in ['database', 'server', 'user_name']:
            secrets = [
                secret.key for secret in self.workspace_client.secrets.list_secrets('wkmigrate_credentials_scope')
            ]
            if f'{source_service_name}_{property_key}' in secrets:
                continue
            self.workspace_client.secrets.put_secret(
                scope='wkmigrate_credentials_scope',
                key=f'{source_service_name}_{property_key}',
                string_value=properties.get(property_key)
            )
            secret_keys.append(f'{source_service_name}_{property_key}')
        db_pwd = input(
            f'Enter database password for user {properties.get('user_name')} on host {properties.get('server')}'
        )
        self.workspace_client.secrets.put_secret(
            scope='wkmigrate_credentials_scope',
            key=f'{source_service_name}_credential',
            string_value=db_pwd
        )
        secret_keys.append(f'{source_service_name}_credential')
        return secret_keys

    def _create_notebook_task_dependencies(self, task: dict) -> str:
        notebook_task = task.get('notebook_task')
        notebook_path = f'/Workspace{notebook_task.get('notebook_path')}'
        cluster_definition = task.get('new_cluster')
        host_name = cluster_definition.pop('host_name')
        try:
            self.workspace_client.workspace.get_status(path=notebook_path)
            return notebook_path
        except Exception:
            pat = input('Notebook not found in target workspace for notebook task. Enter source workspace PAT')
            self._upload_notebook(host_name, pat, notebook_path)
            return notebook_path

    def _upload_notebook(self, host_name, pat, notebook_path) -> None:
        source_client = WorkspaceManagementClient(host_name=host_name, pat=pat, authentication_type='pat')
        target_folder = '/'.join(notebook_path.split('/')[:-1])
        self.workspace_client.workspace.mkdirs(target_folder)
        language = source_client.workspace_client.workspace.get_status(path=notebook_path).language
        with source_client.workspace_client.workspace.download(path=notebook_path, format=ExportFormat.SOURCE) as file:
            self.workspace_client.workspace.upload(
                path=notebook_path,
                content=file,
                format=ImportFormat.SOURCE,
                language=language
            )

    def _create_for_each_task(self, task: dict) -> Task:
        """ Creates a Databricks workflow ``Task`` object from a foreach task definition.
            :parameter task: Workflow for each task definition as a ``dict``
            :return: Workflow ``Task`` object
        """
        if 'for_each_task' not in task:
            raise ValueError('No "for_each_task" value for for each task type')
        for_each_task = task.get('for_each_task')
        inner_tasks = for_each_task.get('task')
        if len(inner_tasks) == 1:
            task['for_each_task']['task'] = inner_tasks[0]
            return self._create_task(task)
        task_key = f'{task.get('task_key')}_inner_tasks'
        job_id = self.create_workflow({
            'settings': {
                'name': task_key,
                'tasks': inner_tasks,
                'tags': {'CREATED_BY_WKMIGRATE': ''}
            }
        })
        task['for_each_task']['task'] = {'task_key': task_key, 'run_job_task': {'job_id': job_id}}
        return Task.from_dict(task)

    def _create_copy_task(self, task: dict) -> Task:
        """ Creates a Databricks workflow ``Task`` object from a copy data task definition.
            :parameter task: Workflow copy data task definition as a ``dict``
            :return: Workflow ``Task`` object (Notebook task)
        """
        # TODO: AUTHOR THE NOTEBOOK
        if 'notebook_task' not in task:
            raise ValueError('No "notebook_task" value for copy data task type')
        notebook_task = task.get('notebook_task')
        # TODO: CREATE THE NOTEBOOK TASK
        pass

    def _create_copy_notebook(self, task: dict) -> int:
        """ Creates a Databricks notebook defining a copy data task.
            :parameter task: Workflow copy data task definition as a ``dict``
            :return: Notebook ID as an ``int``
        """
        pass

    @staticmethod
    def _get_schedule(schedule: Optional[dict]) -> Optional[CronSchedule]:
        if schedule is None:
            return None
        return CronSchedule.from_dict(schedule)


@dataclass
class WorkspaceTestClient(DatabricksWorkspaceClient):
    """ A mock client implementing methods to list, describe, and update objects in a Databricks workspace."""
    test_json_path: str = wkmigrate.JSON_PATH

    def get_workflow(self, job_name: str) -> dict:
        """ Gets a workflow with the specified ID or name as a ``Job`` object.
            :parameter job_name: Job name for the specified workflow
            :return: Workflow definition as a ``Job``
        """
        # Open the test workflows file:
        with open(f'{self.test_json_path}/test_workflows.json', 'r') as file:
            # Load the data from JSON:
            workflows = json.load(file)
        if job_name is not None:
            workflows = [workflow for workflow in workflows if workflow['settings']['name'] == job_name]
            if len(workflows) == 1:
                return workflows[0]
            # If no workflow was found:
            raise ValueError(f'No workflow found with job name {job_name}."')

    def create_workflow(self, job_definition: dict) -> None:
        pass

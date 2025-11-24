"""Defines ``WorkspaceClient`` classes."""

import base64
import json
import os
import warnings
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field

import autopep8  # type: ignore
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob, CronSchedule, Job, Task
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

import wkmigrate
from wkmigrate.datasets import options, secrets
from wkmigrate.datasets.data_type_mapping import parse_spark_data_type


class DatabricksWorkspaceClient(ABC):
    """A client implementing methods for getting data pipeline, linked service,
    dataset, and pipeline trigger definitions.
    """

    __test__ = False

    @abstractmethod
    def get_workflow(self, job_id: int | None = None, job_name: str | None = None) -> Job:
        pass

    @abstractmethod
    def create_workflow(
        self,
        job_definition: dict,
        translation_options: dict | None = None,
        create_in_workspace: bool = True,
        local_directory: str | None = None,
    ) -> int | dict | None:
        pass


@dataclass
class WorkspaceManagementClient(DatabricksWorkspaceClient):
    """A client used to list, describe, and update objects in a Databricks workspace."""

    __test__ = False

    authentication_type: str
    host_name: str
    pat: str | None = None
    username: str | None = None
    password: str | None = None
    resource_id: str | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    workspace_client: WorkspaceClient | None = field(init=False)
    _local_artifacts_collector: "LocalArtifactsCollector | None" = field(init=False, default=None)

    def __post_init__(self) -> None:
        """Sets up the workspace client for the provided authentication credentials."""
        if self.authentication_type == "pat":
            self.workspace_client = self._login_with_pat()
            return
        if self.authentication_type == "basic":
            self.workspace_client = self._login_with_basic_auth()
            return
        if self.authentication_type == "azure-client-secret":
            self.workspace_client = self._login_with_azure_client_secret()
            return
        raise ValueError(
            'Got an invalid value for "self.authentication_type", must be "pat", "basic", or "azure-client-secret"'
        )

    def _login_with_pat(self) -> WorkspaceClient:
        """Creates a ``WorkspaceClient`` with PAT authentication.
        :return: A ``WorkspaceClient`` from the Databricks SDK
        """
        if self.pat is None:
            raise ValueError('No value provided for "pat" with access token authentication')
        return WorkspaceClient(auth_type=self.authentication_type, host=self.host_name, token=self.pat)

    def _login_with_basic_auth(self) -> WorkspaceClient:
        """Creates a ``WorkspaceClient`` with basic authentication.
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
            password=self.password,
        )

    def _login_with_azure_client_secret(self) -> WorkspaceClient:
        """Creates a ``WorkspaceClient`` with Azure client secret authentication.
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
            client_secret=self.client_secret,
        )

    def get_workflow(self, job_id: int | None = None, job_name: str | None = None) -> Job:
        """Gets a workflow with the specified ID or name as a ``Job`` object.
        :parameter job_id: Job ID for the specified workflow
        :parameter job_name: Job name for the specified workflow
        :return: Workflow definition as a ``Job``
        """
        # Check the input parameters:
        if job_id is None and job_name is None:
            raise ValueError('Must provide a value for "job_id" or "job_name".')
        # If a workflow ID is specified, get the workflow by ID:
        if job_id is not None:
            if self.workspace_client is None:
                raise ValueError("workspace_client is not initialized")
            return self.workspace_client.jobs.get(job_id=job_id)
        # Otherwise, list the workflows by name:
        if job_name is None:
            raise ValueError("job_name cannot be None when job_id is not provided")
        workflows = self._list_workflows_by_name(job_name=job_name)
        # If more than 1 workflow exists with the specified name:
        if len(workflows) > 1:
            raise ValueError(f'Duplicate workflows found in the target workspace with name "{job_name}"')
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        job_id = workflows[0].job_id
        if job_id is None:
            raise ValueError("Job ID cannot be None")
        return self.workspace_client.jobs.get(job_id=job_id)

    def _list_workflows_by_name(self, job_name: str) -> list[BaseJob]:
        """Gets workflows with the specified name as ``BaseJob`` objects.
        :return: Workflow definitions as a ``list[BaseJob]``
        """
        # List the workflows:
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        workflows = list(self.workspace_client.jobs.list(name=job_name))
        # If no workflows were found:
        if workflows is None or len(workflows) == 0:
            raise ValueError(f'No workflows found in the target workspace with name "{job_name}"')
        return workflows

    def create_workflow(
        self,
        job_definition: dict,
        translation_options: dict | None = None,
        create_in_workspace: bool = True,
        local_directory: str | None = None,
    ) -> int | dict | None:
        """Creates a workflow with the specified definition and options.
        :parameter job_definition: Workflow definition settings as a ``dict``
        :parameter translation_options: Workflow translation options as a ``dict``
        :parameter create_in_workspace: Whether to create the workflow in Databricks or export locally
        :parameter local_directory: Optional output directory when exporting locally
        :return: Created Job ID as an ``int``
        """
        job_settings = job_definition.get("settings")
        if job_settings is None:
            raise ValueError('Invalid "job_definition" object.')
        job_name = job_settings.get("name", None)
        if job_name is None:
            raise ValueError('No value provided for "name"')

        # access_control_list = job_settings.get("access_control_list", None)
        # is_continuous = job_settings.get("is_continuous", None)
        # deployment = job_settings.get("deployment", None)
        # edit_mode = job_settings.get("edit_mode", None)
        # email_notifications = job_settings.get("email_notifications", None)
        # git_source = job_settings.get("git_source", None)
        # health_rules = job_settings.get("health_rules", None)
        # max_concurrent_runs = job_settings.get("max_concurrent_runs", None)
        # notification_settings = job_settings.get("notification_settings", None)
        # parameter_definitions = job_settings.get("parameter_definitions", None)
        # queue = job_settings.get("queue", None)
        # run_as_principal = job_settings.get("run_as_principal", None)
        # webhook_notifications = job_settings.get("webhook_notifications", None)
        if not create_in_workspace:
            collector = LocalArtifactsCollector(local_directory)
            return self._create_workflow_locally(job_settings, translation_options, collector)

        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        tasks = job_settings.get("tasks")
        if tasks is None:
            raise ValueError('No "tasks" provided in job_settings')
        response = self.workspace_client.jobs.create(
            name=job_settings.get("name"),
            description=job_settings.get("description"),
            schedule=WorkspaceManagementClient._get_schedule(job_settings.get("schedule")),
            tags=job_settings.get("tags"),
            tasks=[self._create_task(task, translation_options) for task in tasks],
            timeout_seconds=job_settings.get("timeout_seconds"),
        )
        job_id = response.job_id
        if job_id is None:
            raise ValueError("Created job ID cannot be None")
        return job_id

    def _create_workflow_locally(
        self,
        job_settings: dict,
        translation_options: dict | None,
        collector: "LocalArtifactsCollector",
    ) -> dict:
        tasks = job_settings.get("tasks")
        if tasks is None:
            raise ValueError('No "tasks" provided in job_settings')
        original_collector = self._local_artifacts_collector
        self._local_artifacts_collector = collector
        try:
            for task in tasks:
                self._create_task(task, translation_options)
            collector.record_workflow(job_settings)
            if collector.output_dir is not None:
                collector.write_to_directory()
            return collector.build_result()
        finally:
            self._local_artifacts_collector = original_collector

    def _create_task(self, task: dict, translation_options: dict | None) -> Task:
        """Creates a Databricks workflow ``Task`` object from the task definition.
        :parameter task: Workflow task definition as a ``dict``
        :parameter translation_options: Workflow translation options as a ``dict``
        :return: Workflow ``Task`` object
        """
        if "type" not in task:
            raise ValueError('Task has no "type"')
        if task.get("type") == "DatabricksNotebook":
            self._create_notebook_task_dependencies(task)
        if task.get("type") == "Copy":
            copy_data_task = task.get("copy_data_task")
            sink_dataset = copy_data_task.get("sink_dataset")
            files_to_delta_sinks = sink_dataset.get("type") == "delta"
            #files_to_delta_sinks = True
            if translation_options is not None:
                files_to_delta_sinks = translation_options.get("files_to_delta_sinks", True)
            dependency_object = self._create_copy_task_dependencies(
                task,
                files_to_delta_sinks
            )
            task.pop("copy_data_task")
            if not files_to_delta_sinks:
                task["notebook_task"] = {"notebook_path": dependency_object}
            else:
                task["pipeline_task"] = {"pipeline_id": dependency_object}
        if task.get("type") == "ForEach":
            return self._create_for_each_task(task)
        return Task.from_dict(task)

    def _create_copy_task_dependencies(
        self,
        task: dict,
        files_to_delta_sinks: bool
    ) -> str:
        """Creates a Databricks notebook to copy data.
        :parameter task: Workflow task definition as a ``dict``
        :parameter files_to_delta_sinks: Whether to create Lakeflow Declarative Pipelines to copy data files to Delta table sinks
        :return: Databricks notebook path as a ``str``
        """
        copy_data_task = task.get("copy_data_task")
        if copy_data_task is None:
            raise ValueError('No "copy_data_task" found in task')
        source_dataset = copy_data_task.get("source_dataset")
        source_properties = copy_data_task.get("source_properties")
        if source_dataset is None or source_properties is None:
            raise ValueError('Missing "source_dataset" or "source_properties"')
        source_definition = {
            **source_dataset,
            **source_properties,
        }
        sink_dataset = copy_data_task.get("sink_dataset")
        sink_properties = copy_data_task.get("sink_properties")
        if sink_dataset is None or sink_properties is None:
            raise ValueError('Missing "sink_dataset" or "sink_properties"')
        sink_definition = {
            **sink_dataset,
            **sink_properties,
        }
        self._create_data_source_secrets(source_definition)
        self._create_data_source_secrets(sink_definition)
        column_mapping = copy_data_task.get("column_mapping")
        notebook_path = self._create_copy_data_notebook(
            source_definition, sink_definition, column_mapping, files_to_delta_sinks
        )
        if not files_to_delta_sinks:
            return notebook_path
        # Create a DLT pipeline from the notebook path:
        if self._local_artifacts_collector is not None:
            return self._local_artifacts_collector.add_pipeline_reference(task.get("task_key"), notebook_path)
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        pipeline_response = self.workspace_client.pipelines.create(
            allow_duplicate_names=True,
            catalog="wkmigrate",
            channel="CURRENT",
            continuous=False,
            development=False,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=notebook_path))],
            name=f'{task.get("task_key")}_pipeline',
            photon=True,
            serverless=True,
            target="wkmigrate",
        )
        pipeline_id = pipeline_response.pipeline_id
        if pipeline_id is None:
            raise ValueError("Created pipeline ID cannot be None")
        return pipeline_id

    def _create_copy_data_notebook(
        self,
        source_definition: dict,
        sink_definition: dict,
        column_mapping: dict,
        files_to_delta_sinks: bool,
    ) -> str:
        """Creates a notebook in the target workspace to copy data between specified data source and sink with
        the given column mapping.
        :parameter source_definition: Source dataset definition as a ``dict``
        :parameter sink_definition: Sink dataset definition as a ``dict``
        :parameter column_mapping: Column-level mapping as a ``dict``
        :parameter files_to_delta_sinks: A ``bool`` indicating whether file sinks should be
                                            converted to Delta tables
        :return: Databricks notebook path in the target workspace as a ``str``
        """
        script_lines = [
            "# Databricks notebook source",
            "import pyspark.sql.types as T",
            "import pyspark.sql.functions as F",
            "",
            "# Set the source options:",
        ]
        # Append code blocks to set source dataset options from the Databricks secret scope:
        script_lines.extend(WorkspaceManagementClient._get_option_expressions(source_definition))
        if not files_to_delta_sinks:
            # Append code blocks to set source dataset options from the Databricks secret scope:
            script_lines.append("# Set the target options:")
            script_lines.extend(WorkspaceManagementClient._get_option_expressions(sink_definition))
            # Append a code block to read the source as a DataFrame:
            script_lines.append("# Read from the source:")
            script_lines.append(WorkspaceManagementClient._get_read_expression(source_definition))
            # Append code blocks to create a new DataFrame with mapped column names and data types:
            script_lines.append("# Map the source columns to the target columns:")
            script_lines.append(
                WorkspaceManagementClient._get_mapping(source_definition, sink_definition, column_mapping, True)
            )
            # Append a code block to write the DataFrame to Delta:
            script_lines.append("# Write to the target:")
            script_lines.append(WorkspaceManagementClient._get_write_expression(sink_definition))
        else:
            # Append a code block to define the Delta table in DLT:
            script_lines.append("# Load the data with DLT as a materialized view:")
            script_lines.append(
                WorkspaceManagementClient._get_dlt_definition(source_definition, sink_definition, column_mapping)
            )
        # Create and upload the script as a Python notebook:
        source_dataset_name = source_definition.get("dataset_name")
        sink_dataset_name = sink_definition.get("dataset_name")
        notebook_str = autopep8.fix_code("\n".join(script_lines))
        notebook_path = f"/wkmigrate/copy_data_notebooks/copy_{source_dataset_name}_to_{sink_dataset_name}"
        if self._local_artifacts_collector is not None:
            self._local_artifacts_collector.add_notebook(
                databricks_path=notebook_path,
                content=notebook_str,
                language="python",
            )
            return notebook_path
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        self.workspace_client.workspace.mkdirs("/wkmigrate/copy_data_notebooks")
        self.workspace_client.workspace.import_(
            content=base64.b64encode(notebook_str.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
            path=notebook_path,
        )
        return notebook_path

    @staticmethod
    def _get_dlt_definition(source_dataset: dict, sink_dataset: dict, column_mapping: dict) -> str:
        """Creates a DLT table definition for a give source, sink, and column mapping.
        :parameter source_dataset: Source dataset properties as a ``dict``
        :parameter sink_dataset: Sink dataset properties as a ``dict``
        :parameter column_mapping: Column mapping as a ``dict``
        :return: A DLT table definition in Python as a ``str``
        """
        source_name = source_dataset.get("dataset_name")
        sink_name = sink_dataset.get("dataset_name")
        return f"""@dlt.table(
                        name="{sink_name}",
                        comment="Data copied from {source_name}; Previously targeted {sink_name}."
                        tbl_properties={{'delta.createdBy.wkmigrate': 'true'}}
                    )
                    def {sink_name}:
                        {WorkspaceManagementClient._get_read_expression(source_dataset)}
                        {WorkspaceManagementClient._get_mapping(source_dataset, sink_dataset, column_mapping, True)}
                        return {sink_name}_df
                """

    @staticmethod
    def _get_mapping(source_dataset: dict, sink_dataset: dict, column_mapping: dict, cast_column_types: bool) -> str:
        """Creates a PySpark expression mapping columns from a source DataFrame to a sink DataFrame.
        :parameter source_dataset: Source dataset properties as a ``dict``
        :parameter sink_dataset: Sink dataset properties as a ``dict``
        :parameter column_mapping: Column mapping as a ``dict``
        :parameter cast_column_types: Whether to cast column data types
        :return: PySpark expression creating a new DataFrame with the mapped columns
        """
        source_name = source_dataset.get("dataset_name")
        sink_name = sink_dataset.get("dataset_name")
        mapping_expressions = [
            (
                f'"cast({mapping["source_column_name"]} as {parse_spark_data_type(mapping["sink_column_type"], sink_dataset["type"])}) as {mapping["sink_column_name"]}"'
                if cast_column_types else f'"{mapping["source_column_name"]} as {mapping["sink_column_name"]}"'
            )
            for mapping in column_mapping
        ]
        newline_characters = ", \n\t"
        return f"{sink_name}_df = {source_name}_df.selectExpr(\n\t{newline_characters.join(mapping_expressions)}\n)"

    @staticmethod
    def _get_write_expression(sink_definition: dict) -> str:
        """Creates a PySpark expression writing to a specified data sink.
        :parameter sink_definition: Sink dataset properties as a ``dict``
        :return: PySpark expression writing a DataFrame to the sink
        """
        sink_name = sink_definition.get("dataset_name")
        sink_type = sink_definition.get("type")
        if sink_type == "avro":
            container_name = sink_definition.get("container")
            storage_account_name = sink_definition.get("storage_account_name")
            folder_path = sink_definition.get("folder_path")
            return rf"""{sink_name}_df.write.format("avro")  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
        if sink_type == "csv":
            container_name = sink_definition.get("container")
            storage_account_name = sink_definition.get("storage_account_name")
            folder_path = sink_definition.get("folder_path")
            return rf"""{sink_name}_df.write.format("csv")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
        if sink_type == "delta":
            database_name = sink_definition.get("database_name")
            table_name = sink_definition.get("table_name")
            return rf"""{sink_name}_df.write.format("delta")  \
                        .mode("overwrite")  \
                        .saveAsTable("hive_metastore.{database_name}.{table_name}")
                    """
        if sink_type == "json":
            container_name = sink_definition.get("container")
            storage_account_name = sink_definition.get("storage_account_name")
            folder_path = sink_definition.get("folder_path")
            return rf"""{sink_name}_df.write.format("json")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
        if sink_type == "orc":
            container_name = sink_definition.get("container")
            storage_account_name = sink_definition.get("storage_account_name")
            folder_path = sink_definition.get("folder_path")
            return rf"""{sink_name}_df.write.format("orc")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
        if sink_type == "parquet":
            container_name = sink_definition.get("container")
            storage_account_name = sink_definition.get("storage_account_name")
            folder_path = sink_definition.get("folder_path")
            return rf"""{sink_name}_df.write.format("parquet")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
        if sink_type == "sqlserver":
            return rf"""{sink_name}_df.write.format("jdbc")  \
                        .options(**{sink_name}_options)  \
                        .save()
                    """  # TODO: SET UP A WRITE TO SQL SERVER
        raise ValueError(f'Writing data to "{sink_type}" not supported')

    @staticmethod
    def _get_read_expression(source_definition: dict) -> str:
        """Creates a PySpark expression reading from a specified data source.
        :parameter source_definition: Source dataset properties as a ``dict``
        :return: PySpark expression reading from the source to a DataFrame
        """
        source_name = source_definition.get("dataset_name")
        source_type = source_definition.get("type")
        if source_type == "avro":
            container_name = source_definition.get("container")
            storage_account_name = source_definition.get("storage_account_name")
            folder_path = source_definition.get("folder_path")
            return f"""{source_name}_df = ( 
                        spark.read.format("avro")
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    )
                    """
        if source_type == "csv":
            container_name = source_definition.get("container")
            storage_account_name = source_definition.get("storage_account_name")
            folder_path = source_definition.get("folder_path")
            return f"""{source_name}_df = ( 
                        spark.read.format("csv")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
        if source_type == "delta":
            database_name = source_definition.get("database_name")
            table_name = source_definition.get("table_name")
            return f'{source_name}_df = spark.read.table("hive_metastore.{database_name}.{table_name}'
        if source_type == "json":
            container_name = source_definition.get("container")
            storage_account_name = source_definition.get("storage_account_name")
            folder_path = source_definition.get("folder_path")
            return f"""{source_name}_df = ( 
                        spark.read.format("json")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
        if source_type == "orc":
            container_name = source_definition.get("container")
            storage_account_name = source_definition.get("storage_account_name")
            folder_path = source_definition.get("folder_path")
            return f"""{source_name}_df = ( 
                        spark.read.format("orc")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
        if source_type == "parquet":
            container_name = source_definition.get("container")
            storage_account_name = source_definition.get("storage_account_name")
            folder_path = source_definition.get("folder_path")
            return f"""{source_name}_df = ( 
                        spark.read.format("parquet")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
        if source_type == "sqlserver":
            schema_name = source_definition.get("schema_name")
            table_name = source_definition.get("table_name")
            return f"""{source_name}_df = ( 
                    spark.read.format("sqlserver")
                        .options(**{source_name}_options)
                        .option("dbtable", "{schema_name}.{table_name}")
                        .load()
                    )
                    """
        raise ValueError(f'Reading data from "{source_type}" not supported')

    @staticmethod
    def _get_option_expressions(dataset_definition: dict) -> list[str]:
        """Creates a PySpark expression defining DataFrameReader or DataFrameWriter options.
        :parameter dataset_definition: Dataset properties as a ``dict``
        :return: PySpark expression writing a DataFrame to the sink
        """
        dataset_name = dataset_definition.get("dataset_name")
        service_name = dataset_definition.get("service_name")
        dataset_type = dataset_definition.get("type")
        if dataset_type == "avro":
            config_lines = [
                f"""spark.conf.set(
                    "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                    dbutils.secrets.get(
                        scope="wkmigrate_credentials_scope", 
                        key="{service_name}_storage_account_key"
                    )
                )""",
                f"""spark.conf.set(
                    "spark.sql.files.maxRecordsPerFile",
                    "{dataset_definition.get('records_per_file')}"
                )""",
            ]
            return [f"{dataset_name}_options = {{}}", *config_lines]
        if dataset_type == "csv":
            config_lines = [
                rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
                for option in options[dataset_type]
                if dataset_definition.get(option)
            ]
            if "records_per_file" in dataset_definition:
                records_per_file = dataset_definition.get("records_per_file")
                config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
            config_lines.append(
                f"""spark.conf.set(
                    "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                        dbutils.secrets.get(
                            scope="wkmigrate_credentials_scope", 
                            key="{service_name}_storage_account_key"
                    )
                )
                """
            )
            return [f"{dataset_name}_options = {{}}", *config_lines]
        if dataset_type == "json":
            config_lines = [
                rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
                for option in options[dataset_type]
                if dataset_definition.get(option)
            ]
            if "records_per_file" in dataset_definition:
                records_per_file = dataset_definition.get("records_per_file")
                config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
            config_lines.append(
                f"""spark.conf.set(
                    "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                        dbutils.secrets.get(
                            scope="wkmigrate_credentials_scope", 
                            key="{service_name}_storage_account_key"
                    )
                )
                """
            )
            return [f"{dataset_name}_options = {{}}", *config_lines]
        if dataset_type == "orc":
            config_lines = [
                rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
                for option in options[dataset_type]
                if dataset_definition.get(option)
            ]
            if "records_per_file" in dataset_definition:
                records_per_file = dataset_definition.get("records_per_file")
                config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
            config_lines.append(
                f"""spark.conf.set(
                    "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                        dbutils.secrets.get(
                            scope="wkmigrate_credentials_scope", 
                            key="{service_name}_storage_account_key"
                    )
                )
                """
            )
            return [f"{dataset_name}_options = {{}}", *config_lines]
        if dataset_type == "parquet":
            config_lines = [
                rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
                for option in options[dataset_type]
                if dataset_definition.get(option)
            ]
            if "records_per_file" in dataset_definition:
                records_per_file = dataset_definition.get("records_per_file")
                config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
            config_lines.append(
                f"""spark.conf.set(
                    "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                        dbutils.secrets.get(
                            scope="wkmigrate_credentials_scope", 
                            key="{service_name}_storage_account_key"
                    )
                )
                """
            )
            return [f"{dataset_name}_options = {{}}", *config_lines]
        if dataset_type == "sqlserver":
            secrets_lines = [
                f"""{dataset_name}_options["{secret}"] = dbutils.secrets.get(
                    scope="wkmigrate_credentials_scope", 
                    key="{service_name}_{secret}"
                )
                """
                for secret in secrets[dataset_type]
            ]
            options_lines = [
                f"""{dataset_name}_options["{option}"] = '{dataset_definition.get(option)}'"""
                for option in options[dataset_type]
            ]
            return [f"{dataset_name}_options = {{}}", *secrets_lines, *options_lines]
        return []

    def _create_data_source_secrets(self, source_definition: dict) -> list[str]:
        """Creates Databricks secrets for credentials and connection strings in a given data source definition.
        :parameter source_definition: Data source definition as a ``dict``
        :return: Created secret keys as a ``list[str]``
        """
        secret_keys = []
        source_service_name = source_definition.get("service_name")
        source_service_type = source_definition.get("type")
        collector = self._local_artifacts_collector
        if collector is None:
            if self.workspace_client is None:
                raise ValueError("workspace_client is not initialized")
            scopes = [scope.name for scope in self.workspace_client.secrets.list_scopes()]
            if "wkmigrate_credentials_scope" not in scopes:
                self.workspace_client.secrets.create_scope(scope="wkmigrate_credentials_scope")
        if source_service_type is None:
            raise ValueError("Source service type cannot be None")
        for secret in secrets[source_service_type]:
            secret_value = source_definition.get(secret)
            if secret_value is None:
                if collector is not None:
                    collector.add_secret(
                        scope="wkmigrate_credentials_scope",
                        key=f"{source_service_name}_{secret}",
                        service_name=source_service_name,
                        service_type=source_service_type,
                        provided_value=None,
                        user_input_required=True,
                    )
                    continue
                secret_value = input(f"Enter {secret} for dataset {source_service_name}")
            if collector is not None:
                collector.add_secret(
                    scope="wkmigrate_credentials_scope",
                    key=f"{source_service_name}_{secret}",
                    service_name=source_service_name,
                    service_type=source_service_type,
                    provided_value=secret_value,
                    user_input_required=False,
                )
                secret_keys.append(f"{source_service_name}_{secret}")
                continue
            if self.workspace_client is None:
                raise ValueError("workspace_client is not initialized")
            self.workspace_client.secrets.put_secret(
                scope="wkmigrate_credentials_scope",
                key=f"{source_service_name}_{secret}",
                string_value=secret_value,
            )
            secret_keys.append(f"{source_service_name}_{secret}")
        return secret_keys

    def _create_notebook_task_dependencies(self, task: dict) -> str:
        """Creates a Databricks notebook if it does not exist in the target workspace.
        :parameter task: Task definition as a ``dict``
        :return: Notebook path in the target workspace as a ``str``
        """
        notebook_task = task.get("notebook_task")
        if notebook_task is None:
            raise ValueError('No "notebook_task" found in task')
        notebook_path_value = notebook_task.get('notebook_path')
        if notebook_path_value is None:
            raise ValueError('No "notebook_path" found in notebook_task')
        notebook_path = f"/Workspace{notebook_path_value}"
        cluster_definition = task.get("new_cluster")
        host_name = self.host_name
        if cluster_definition is None:
            warnings.warn('No "new_cluster" found in task, using serverless compute')
        else:
            host_name = cluster_definition.pop("host_name")
        if task.get("unsupported"):
            return notebook_path
        try:
            if self.workspace_client is None:
                raise ValueError("workspace_client is not initialized")
            self.workspace_client.workspace.get_status(path=notebook_path)
            return notebook_path
        except Exception:
            pat = input("Notebook not found in target workspace for notebook task. Enter source workspace PAT")
            self._upload_notebook(host_name, pat, notebook_path)
            return notebook_path

    def _upload_notebook(self, host_name: str, pat: str, notebook_path: str) -> None:
        """Uploads a Databricks notebook to the target workspace.
        :parameter host_name: Workspace host as a ``str``
        :parameter pat: Workspace PAT as a ``str``
        :parameter notebook_path: Target notebook path as a ``str``
        """
        source_client = WorkspaceManagementClient(host_name=host_name, pat=pat, authentication_type="pat")
        target_folder = "/".join(notebook_path.split("/")[:-1])
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        self.workspace_client.workspace.mkdirs(target_folder)
        if source_client.workspace_client is None:
            raise ValueError("source workspace_client is not initialized")
        language = source_client.workspace_client.workspace.get_status(path=notebook_path).language
        with source_client.workspace_client.workspace.download(path=notebook_path, format=ExportFormat.SOURCE) as file:
            self.workspace_client.workspace.upload(
                path=notebook_path,
                content=file,
                format=ImportFormat.SOURCE,
                language=language,
            )

    def _create_for_each_task(self, task: dict) -> Task:
        """Creates a Databricks workflow ``Task`` object from a foreach task definition.
        :parameter task: Workflow for each task definition as a ``dict``
        :return: Workflow ``Task`` object
        """
        if "for_each_task" not in task:
            raise ValueError('No "for_each_task" value for for each task type')
        for_each_task = task.get("for_each_task")
        if for_each_task is None:
            raise ValueError('"for_each_task" cannot be None')
        inner_tasks = for_each_task.get("task")
        if len(inner_tasks) == 1:
            task["for_each_task"]["task"] = inner_tasks[0]
        else:
            task_key = f"{task.get('task_key')}_inner_tasks"
            job_id = self.create_workflow(
                {
                    "settings": {
                        "name": task_key,
                        "tasks": inner_tasks,
                        "tags": {"CREATED_BY_WKMIGRATE": ""},
                    }
                }
            )
            task["for_each_task"]["task"] = {
                "task_key": task_key,
                "run_job_task": {"job_id": job_id},
            }
        return Task.from_dict(task)

    @staticmethod
    def _get_schedule(schedule: dict | None) -> CronSchedule | None:
        if schedule is None:
            return None
        return CronSchedule.from_dict(schedule)


@dataclass
class WorkspaceTestClient(DatabricksWorkspaceClient):
    """A mock client implementing methods to list, describe, and update objects in a Databricks workspace."""

    test_json_path: str = wkmigrate.JSON_PATH

    def get_workflow(self, job_id: int | None = None, job_name: str | None = None) -> Job:
        """Gets a workflow with the specified ID or name as a ``Job`` object.
        :parameter job_id: Job ID for the specified workflow
        :parameter job_name: Job name for the specified workflow
        :return: Workflow definition as a ``Job``
        """
        # Check the input parameters:
        if job_id is None and job_name is None:
            raise ValueError('Must provide a value for "job_id" or "job_name".')

        # Open the test workflows file:
        with open(f"{self.test_json_path}/test_workflows.json", "r", encoding="utf-8") as file:
            # Load the data from JSON:
            workflows = json.load(file)

        # If a workflow ID is specified, get the workflow by ID:
        if job_id is not None:
            workflows = [workflow for workflow in workflows if workflow.get("job_id") == job_id]
            if len(workflows) == 1:
                return Job.from_dict(workflows[0])
            raise ValueError(f'No workflow found with job ID {job_id}.')

        # Otherwise, list the workflows by name:
        if job_name is not None:
            workflows = [workflow for workflow in workflows if workflow["settings"]["name"] == job_name]
            if len(workflows) == 1:
                return Job.from_dict(workflows[0])
            # If no workflow was found:
            raise ValueError(f'No workflow found with job name {job_name}.')

        # This should never be reached due to the initial check, but mypy needs it
        raise ValueError('Must provide a value for "job_id" or "job_name".')

    def create_workflow(self, job_definition: dict, translation_options: dict | None = None) -> None:
        pass


class LocalArtifactsCollector:
    """Collects workflow artifacts when running WorkspaceManagementClient offline."""

    def __init__(self, output_dir: str | None):
        self.output_dir = output_dir
        self.workflows: list[dict] = []
        self.notebooks: list[dict] = []
        self.secrets: list[dict] = []
        self.unsupported: list[dict] = []
        self._not_translatable: list[dict] = []

    def record_workflow(self, job_settings: dict) -> None:
        workflow_copy = {"settings": deepcopy(job_settings)}
        self.workflows.append(workflow_copy)
        not_translatable = workflow_copy["settings"].get("not_translatable") or []
        self._not_translatable.extend(not_translatable)

    def add_notebook(self, databricks_path: str, content: str, language: str) -> None:
        file_name = databricks_path.split("/")[-1] or "notebook"
        notebook_entry = {
            "name": file_name,
            "path": databricks_path,
            "language": language,
            "content": content,
        }
        if self.output_dir is not None:
            notebooks_dir = os.path.join(self.output_dir, "notebooks")
            os.makedirs(notebooks_dir, exist_ok=True)
            file_name_with_ext = file_name if file_name.endswith(".py") else f"{file_name}.py"
            local_path = os.path.join(notebooks_dir, file_name_with_ext)
            with open(local_path, "w", encoding="utf-8") as notebook_file:
                notebook_file.write(content)
            notebook_entry["local_path"] = local_path
        self.notebooks.append(notebook_entry)

    def add_pipeline_reference(self, task_key: str | None, notebook_path: str) -> str:
        pipeline_id = f"{(task_key or 'pipeline')}_pipeline"
        return pipeline_id

    def add_secret(
        self,
        scope: str,
        key: str,
        service_name: str | None,
        service_type: str | None,
        provided_value: str | None,
        user_input_required: bool,
    ) -> None:
        self.secrets.append(
            {
                "scope": scope,
                "key": key,
                "linked_service_name": service_name,
                "linked_service_type": service_type,
                "provided_value": provided_value,
                "user_input_required": user_input_required,
            }
        )

    def build_result(self) -> dict:
        return {
            "workflows": self.workflows,
            "notebooks": self.notebooks,
            "secrets": self.secrets,
            "unsupported": self.get_unsupported_with_warnings(),
            "not_translatable": self._not_translatable,
        }

    def write_to_directory(self) -> None:
        if self.output_dir is None:
            return
        os.makedirs(self.output_dir, exist_ok=True)
        self._write_workflows()
        self._write_secrets()
        self._write_unsupported()

    def get_unsupported_with_warnings(self) -> list[dict]:
        unsupported = list(self.unsupported)
        for warning in self._not_translatable:
            activity_name = warning.get("activity_name") or warning.get("property", "pipeline")
            activity_type = warning.get("activity_type") or "not_translatable"
            metadata = {
                key: value
                for key, value in warning.items()
                if key not in {"activity_name", "activity_type"}
            }
            unsupported.append(
                {
                    "activity_name": activity_name,
                    "activity_type": activity_type,
                    "reason": warning.get("message", "Property could not be translated"),
                    "metadata": metadata,
                }
            )
        return unsupported

    def _write_workflows(self) -> None:
        workflows_dir = os.path.join(self.output_dir, "workflows")
        os.makedirs(workflows_dir, exist_ok=True)
        for index, workflow in enumerate(self.workflows):
            workflow_name = workflow.get("settings", {}).get("name") or f"workflow_{index}"
            file_path = os.path.join(workflows_dir, f"{workflow_name}.json")
            with open(file_path, "w", encoding="utf-8") as workflow_file:
                json.dump(workflow, workflow_file, indent=2, ensure_ascii=False)

    def _write_secrets(self) -> None:
        secrets_file = os.path.join(self.output_dir, "secrets.json")
        with open(secrets_file, "w", encoding="utf-8") as secrets_handle:
            json.dump(self.secrets, secrets_handle, indent=2, ensure_ascii=False)

    def _write_unsupported(self) -> None:
        unsupported_file = os.path.join(self.output_dir, "unsupported.json")
        with open(unsupported_file, "w", encoding="utf-8") as unsupported_handle:
            json.dump(self.get_unsupported_with_warnings(), unsupported_handle, indent=2, ensure_ascii=False)

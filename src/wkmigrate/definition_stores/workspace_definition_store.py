"""This module defines the `WorkspaceDefinitionStore` class used to load and persist pipeline definitions in a Databricks workspace.

``WorkspaceDefinitionStore`` materializes translated pipelines into Databricks
Lakeflow Jobs, generates notebooks and Spark Declarative Pipelines for copying
data, and can list or update workspace assets. It is commonly used as the sink
when migrating from ADF definitions to Databricks.

Example:
    ```python
    from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore

    store = WorkspaceDefinitionStore(authentication_type=\"pat\", host_name=\"https://adb-123.azuredatabricks.net\", pat=\"TOKEN\")
    store.to_local_files(translated_pipeline_ir, local_directory=\"/path/to/local/directory\")
    store.to_job(translated_pipeline_ir)
    ```
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from typing import Any
import warnings
from collections.abc import Callable, Iterable
import dataclasses
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobParameterDefinition,
    NotebookTask,
    PipelineTask,
    Task,
)
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from typing_extensions import deprecated

from wkmigrate.datasets import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.models.workflows.artifacts import PreparedWorkflow
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction
from wkmigrate.preparers.preparer import prepare_workflow

logger = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True)
class WorkspaceDefinitionStore(DefinitionStore):
    """
    Definition store implementation that lists, describes, and updates objects in a Databricks workspace.

    Attributes:
        authentication_type: Authentication mode. Can be "pat", "basic", or "azure-client-secret".
        host_name: Workspace hostname for Databricks.
        pat: Personal access token used for "pat" authentication.
        username: Username used for "basic" authentication.
        password: Password used for "basic" authentication.
        resource_id: Azure resource ID for workspace-scoped authentication flows.
        tenant_id: Azure AD tenant identifier used for client-secret authentication.
        client_id: Application (client) ID used for client-secret authentication.
        client_secret: Secret associated with the client ID for client-secret authentication.
        options: Dictionary of options that customize workflow generation and deployment behaviour.
        workspace_client: Databricks workspace client used to interact with the Databricks workspace. Automatically created using the provided credentials.
    """

    authentication_type: str | None = None
    host_name: str | None = None
    pat: str | None = None
    username: str | None = None
    password: str | None = None
    resource_id: str | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    options: dict[str, Any] = dataclasses.field(default_factory=dict)
    workspace_client: WorkspaceClient | None = dataclasses.field(init=False, default=None)
    _valid_authentication_types = ["pat", "basic", "azure-client-secret"]
    _valid_option_keys = frozenset(
        {
            "root_path",
            "files_to_delta_sinks",
            "compute_type",
            "catalog",
            "schema",
            "workspace_url",
            "credentials_scope",
        }
    )
    _valid_compute_types = frozenset({"serverless", "classic"})

    def __post_init__(self) -> None:
        """
        Validates credentials, option keys, and initializes the Databricks workspace client.

        Raises:
            ValueError: If the authentication type is invalid or the host name is not provided.
            ValueError: If any option key is not a recognised key.
            ValueError: If the ``compute_type`` option value is not a supported compute type.
        """
        if self.authentication_type not in self._valid_authentication_types:
            raise ValueError(
                'Invalid value for "authentication_type"; must be "pat", "basic", or "azure-client-secret"'
            )
        if self.host_name is None:
            raise ValueError("'host_name' must be provided when creating a WorkspaceDefinitionStore")
        self._validate_option_keys(self.options.keys())
        self._validate_compute_type_value(self.options.get('compute_type'))
        self.workspace_client = self._login_workspace_client()

    def set_option(self, key: str, value: Any) -> None:
        """
        Sets the value of a single option.

        Args:
            key: Option name. Must be one of the recognised option keys.
            value: New value for the option.

        Raises:
            ValueError: If *key* is not a recognised option.
            ValueError: If *key* is ``compute_type`` and *value* is not a supported compute type.
        """
        self._validate_option_keys([key])
        if key == 'compute_type':
            self._validate_compute_type_value(value)
        self.options[key] = value

    def set_options(self, options: dict[str, Any]) -> None:
        """
        Replaces all options with the provided dictionary.

        Args:
            options: Dictionary of option key-value pairs. All keys must be recognised.

        Raises:
            ValueError: If any key is not a recognised option.
            ValueError: If the ``compute_type`` value is not a supported compute type.
        """
        self._validate_option_keys(options.keys())
        self._validate_compute_type_value(options.get('compute_type'))
        self.options = dict(options)

    def to_jobs(self, pipeline_definitions: list[Pipeline]) -> list[int]:
        """
        Uploads artifacts and creates a Databricks job for each pipeline.

        Args:
            pipeline_definitions: List of ``Pipeline`` dataclasses to deploy.

        Returns:
            List of job identifiers registered in the workspace.
        """
        job_ids: list[int] = []
        for pipeline_definition in pipeline_definitions:
            try:
                job_id = self.to_job(pipeline_definition)
            except Exception:
                logger.warning(  # pylint: disable=logging-too-many-args
                    "Failed to create job for pipeline '%s', skipping",
                    pipeline_definition.name,
                    exc_info=True,
                )
                continue
            if job_id is not None:
                job_ids.append(job_id)
        return job_ids

    def to_job(self, pipeline_definition: Pipeline) -> int | None:
        """
        Uploads artifacts and creates a Databricks job.

        Args:
            pipeline_definition: ``Pipeline`` dataclass.

        Returns:
            Optional job identifier registered in the workspace.

        Raises:
            ValueError: If the job cannot be created.
        """
        prepared = self._prepare_workflow(pipeline_definition)
        client = self._get_workspace_client()
        self._upload_notebooks(client, prepared.all_notebooks)
        self._materialize_secrets(client, prepared.all_secrets)
        self._materialize_pipelines(client, prepared.all_pipelines)
        self._ensure_notebook_dependencies(client, prepared.tasks)
        inner_job_ids = self._create_inner_jobs(client, prepared.inner_workflows)
        if inner_job_ids:
            self._assign_inner_job_ids(prepared.tasks, inner_job_ids)
        job_payload = self._build_job_payload_for_api(prepared)
        response = client.jobs.create(**job_payload)
        job_id = response.job_id
        if job_id is None:
            raise ValueError("Failed to create workflow")
        return job_id

    def to_asset_bundles(
        self,
        pipeline_definitions: list[Pipeline],
        bundle_directory: str,
        download_notebooks: bool = True,
    ) -> None:
        """
        Creates a Databricks asset bundle for each pipeline inside a shared parent directory.

        Each pipeline is written to a subdirectory named after the pipeline.

        Args:
            pipeline_definitions: List of ``Pipeline`` dataclasses to export.
            bundle_directory: Parent directory for all generated bundles.
            download_notebooks: If True, downloads referenced notebooks from the workspace.
        """
        bundle_dir = os.path.abspath(bundle_directory)
        seen_names: set[str] = set()
        for pipeline_definition in pipeline_definitions:
            safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", pipeline_definition.name)
            if not safe_name:
                raise ValueError(f"Pipeline name {pipeline_definition.name!r} is empty after sanitization")
            if safe_name in seen_names:
                suffix = 1
                while f"{safe_name}_{suffix}" in seen_names:
                    suffix += 1
                safe_name = f"{safe_name}_{suffix}"
                logger.warning(  # pylint: disable=logging-too-many-args
                    "Sanitized pipeline name collides with a previous pipeline; renaming to '%s'",
                    safe_name,
                )
            seen_names.add(safe_name)
            sub_directory = os.path.join(bundle_dir, safe_name)
            try:
                self.to_asset_bundle(pipeline_definition, sub_directory, download_notebooks=download_notebooks)
            except Exception:
                logger.warning(  # pylint: disable=logging-too-many-args
                    "Failed to create asset bundle for pipeline '%s', skipping",
                    pipeline_definition.name,
                    exc_info=True,
                )

    def to_asset_bundle(
        self,
        pipeline_definition: Pipeline | dict,
        bundle_directory: str,
        download_notebooks: bool = True,
    ) -> None:
        """
        Creates a Databricks asset bundle containing the workflow definition, notebooks, secrets, and unsupported nodes.

        When ``download_notebooks`` is True, workspace notebook paths are extracted
        using the original (pre-rewrite) paths so that downloads succeed.  The
        ``root_path`` rewrite is applied after the download-path mapping.

        Args:
            pipeline_definition: Prepared pipeline as a ``Pipeline`` or raw dictionary payload.
            bundle_directory: Destination directory for the bundle artifacts.
            download_notebooks: If True, downloads referenced notebooks from the workspace to the bundle.
        """
        pipeline_ir = (
            pipeline_definition if isinstance(pipeline_definition, Pipeline) else Pipeline(**pipeline_definition)
        )
        if download_notebooks:
            prepared = self._prepare_workflow(pipeline_ir, defer_root_path=True)
            self._write_asset_bundle(prepared, bundle_directory, download_notebooks=True)
        else:
            prepared = self._prepare_workflow(pipeline_ir)
            self._write_asset_bundle(prepared, bundle_directory, download_notebooks=False)

    @deprecated("Use 'to_job' as of wkmigrate 0.0.3")
    def dump(self, pipeline_definition: Pipeline) -> int | None:
        """
        This method is deprecated. Use ``to_job`` instead. Uploads artifacts and creates a Databricks job.

        Args:
            pipeline_definition: Serialized ``Pipeline`` dataclass payload as a ``dict``.

        Returns:
            Optional job identifier registered in the workspace.

        Raises:
            ValueError: If the job cannot be created.
        """
        return self.to_job(pipeline_definition)

    @deprecated("Use 'to_asset_bundle' as of wkmigrate 0.0.3")
    def to_local_files(self, pipeline_definition: Pipeline, local_directory: str) -> None:
        """
        Creates a Databricks asset bundle containing the workflow definition, notebooks, secrets, and unsupported nodes.

        Args:
            pipeline_definition: Prepared pipeline as a ``Pipeline``.
            local_directory: Destination directory for generated artifacts.
        """
        self.to_asset_bundle(pipeline_definition, local_directory, download_notebooks=True)

    def _effective_files_to_delta_sinks(self) -> bool | None:
        """Returns the files_to_delta_sinks value, preferring options over the field."""
        return self.options.get('files_to_delta_sinks')

    def _effective_root_path(self) -> str | None:
        """Returns the root_path option, or None if not set."""
        return self.options.get('root_path')

    def _effective_compute_type(self) -> str | None:
        """Returns the compute_type option, or None if not set."""
        return self.options.get('compute_type')

    def _effective_catalog(self) -> str | None:
        """Returns the catalog option, or None if not set."""
        return self.options.get('catalog')

    def _effective_schema(self) -> str | None:
        """Returns the schema option, or None if not set."""
        return self.options.get('schema')

    def _effective_workspace_url(self) -> str | None:
        """Returns the workspace_url option, or None if not set."""
        return self.options.get('workspace_url')

    def _validate_option_keys(self, keys: Iterable[str]) -> None:
        """
        Validates that all provided keys are recognised option keys.

        Args:
            keys: Option key names to validate.

        Raises:
            ValueError: If any key is not a recognised option.
        """
        invalid_keys = set(keys) - self._valid_option_keys
        if invalid_keys:
            raise ValueError(f'Invalid option key(s): {", ".join(sorted(invalid_keys))}')

    def _validate_compute_type_value(self, compute_type: Any) -> None:
        """
        Validates that a ``compute_type`` value is one of the supported types.

        Args:
            compute_type: Value to validate, or ``None`` to skip validation.

        Raises:
            ValueError: If *compute_type* is not ``None`` and not in the supported set.
        """
        if compute_type is not None and compute_type not in self._valid_compute_types:
            raise ValueError(
                f'Invalid compute_type "{compute_type}"; must be one of: '
                f'{", ".join(sorted(self._valid_compute_types))}'
            )

    def _prepare_workflow(self, pipeline_definition: Pipeline, *, defer_root_path: bool = False) -> PreparedWorkflow:
        """
        Translates the pipeline and collects artifacts via ``prepare_workflow``, then applies options.

        Args:
            pipeline_definition: Pipeline to translate as a ``Pipeline``.
            defer_root_path: When True, skip the root_path rewrite so the caller
                can apply it after download-path extraction.

        Returns:
            PreparedWorkflow with configured options applied.
        """
        prepared = prepare_workflow(
            pipeline=pipeline_definition,
            files_to_delta_sinks=self._effective_files_to_delta_sinks(),
        )
        return self._apply_options(prepared, defer_root_path=defer_root_path)

    def _apply_options(self, prepared: PreparedWorkflow, *, defer_root_path: bool = False) -> PreparedWorkflow:
        """
        Returns a new ``PreparedWorkflow`` with all configured options applied.

        Applies ``root_path`` (rewrites notebook and pipeline instruction paths),
        ``compute_type`` (switches tasks to serverless or classic compute),
        ``catalog`` / ``schema`` (rewrites DLT pipeline targets), and
        ``workspace_url`` (overrides Databricks linked-service hosts).

        Args:
            prepared: Source PreparedWorkflow.
            defer_root_path: When True, skip the root_path rewrite so it can be
                applied later (e.g. after download-path extraction).

        Returns:
            A new PreparedWorkflow reflecting the current options.
        """
        root_path = self._effective_root_path()
        compute_type = self._effective_compute_type()
        catalog = self._effective_catalog()
        schema = self._effective_schema()
        workspace_url = self._effective_workspace_url()

        tasks = [dict(t) for t in prepared.tasks]
        activities = list(prepared.activities)

        if compute_type is not None:
            tasks = self._apply_compute_type_override(tasks, compute_type)

        if workspace_url is not None:
            tasks = self._apply_workspace_url_override(tasks, workspace_url)

        if root_path is not None and not defer_root_path:
            tasks = self._apply_root_path_override(tasks, root_path)
            activities = self._apply_root_path_to_activities(activities, root_path)

        if catalog is not None or schema is not None:
            activities = self._apply_catalog_schema_to_activities(activities, catalog, schema)

        result = dataclasses.replace(prepared, activities=activities)
        for activity, task in zip(result.activities, tasks):
            activity.task = task
        return result

    @classmethod
    def _apply_root_path_to_activities(
        cls,
        activities: list[PreparedActivity],
        root_path: str,
    ) -> list[PreparedActivity]:
        """Returns new activity list with root_path applied to notebook and pipeline file_paths.

        Creates copies via ``dataclasses.replace`` so the originals are not mutated.
        Recurses into inner workflows.  Bundle-relative paths (starting with
        ``./``) are left unchanged.
        """
        new_activities: list[PreparedActivity] = []
        for activity in activities:
            replacements: dict[str, Any] = {}
            if activity.notebooks:
                replacements['notebooks'] = [
                    (
                        dataclasses.replace(
                            nb,
                            file_path=f'{root_path.rstrip("/")}/{nb.file_path.lstrip("/")}',
                        )
                        if not (nb.file_path.startswith('./') or cls._has_root_prefix(nb.file_path, root_path))
                        else nb
                    )
                    for nb in activity.notebooks
                ]
            if activity.pipelines:
                replacements['pipelines'] = [
                    (
                        dataclasses.replace(
                            pi,
                            file_path=f'{root_path.rstrip("/")}/{pi.file_path.lstrip("/")}',
                        )
                        if not (pi.file_path.startswith('./') or cls._has_root_prefix(pi.file_path, root_path))
                        else pi
                    )
                    for pi in activity.pipelines
                ]
            if activity.inner_workflow:
                inner_activities = cls._apply_root_path_to_activities(
                    list(activity.inner_workflow.activities), root_path
                )
                replacements['inner_workflow'] = dataclasses.replace(
                    activity.inner_workflow, activities=inner_activities
                )
            new_activities.append(dataclasses.replace(activity, **replacements))
        return new_activities

    @staticmethod
    def _apply_catalog_schema_to_activities(
        activities: list[PreparedActivity],
        catalog: str | None,
        schema: str | None,
    ) -> list[PreparedActivity]:
        """Returns new activity list with catalog/schema applied to pipeline instructions.

        Walks into inner workflows recursively so that pipelines from ForEach
        activities are correctly updated.
        """
        new_activities: list[PreparedActivity] = []
        for activity in activities:
            replacements: dict[str, Any] = {}
            if activity.pipelines:
                replacements['pipelines'] = [
                    dataclasses.replace(
                        pi,
                        catalog=catalog if catalog is not None else pi.catalog,
                        target=schema if schema is not None else pi.target,
                    )
                    for pi in activity.pipelines
                ]
            if activity.inner_workflow:
                inner_activities = WorkspaceDefinitionStore._apply_catalog_schema_to_activities(
                    list(activity.inner_workflow.activities), catalog, schema
                )
                replacements['inner_workflow'] = dataclasses.replace(
                    activity.inner_workflow, activities=inner_activities
                )
            new_activities.append(dataclasses.replace(activity, **replacements))
        return new_activities

    @staticmethod
    def _has_root_prefix(path: str, root_path: str) -> bool:
        """
        Checks whether *path* already starts with the *root_path* directory prefix.

        Uses a normalised form ``root_path + '/'`` so that ``/migrated`` does not
        falsely match ``/migrated_old/notebook``.

        Args:
            path: File path to test.
            root_path: Expected root directory prefix.

        Returns:
            True when *path* is already rooted under *root_path*.
        """
        prefix = root_path.rstrip('/') + '/'
        return path.startswith(prefix) or path.rstrip('/') == root_path.rstrip('/')

    @staticmethod
    def _apply_task_override(
        tasks: list[dict[str, Any]],
        rewrite_fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Applies *rewrite_fn* to each task dict, recursing into ``for_each_task``.

        This is the shared recursion skeleton used by the individual override
        helpers so the ``for_each_task`` traversal logic is defined once.

        Args:
            tasks: Job task dicts to process.
            rewrite_fn: Function that receives a shallow-copied task dict and
                returns the modified task dict.

        Returns:
            New list of task dicts with *rewrite_fn* applied.
        """
        result: list[dict[str, Any]] = []
        for task in tasks:
            task = rewrite_fn(dict(task))
            for_each_task = task.get('for_each_task')
            if for_each_task:
                for_each_task = dict(for_each_task)
                nested = for_each_task.get('task')
                if isinstance(nested, dict):
                    for_each_task['task'] = WorkspaceDefinitionStore._apply_task_override([nested], rewrite_fn)[0]
                elif isinstance(nested, list):
                    for_each_task['task'] = WorkspaceDefinitionStore._apply_task_override(nested, rewrite_fn)
                task['for_each_task'] = for_each_task
            result.append(task)
        return result

    @staticmethod
    def _apply_root_path_override(tasks: list[dict[str, Any]], root_path: str) -> list[dict[str, Any]]:
        """Returns a new tasks list with notebook paths rooted under *root_path*.

        Args:
            tasks: Job task dicts to rewrite.
            root_path: New root directory for notebook paths.

        Returns:
            New list of task dicts with rewritten paths.
        """
        prefix = root_path.rstrip('/') + '/'

        def _rewrite(task: dict[str, Any]) -> dict[str, Any]:
            notebook_task = task.get('notebook_task')
            if notebook_task:
                notebook_task = dict(notebook_task)
                original = notebook_task.get('notebook_path') or ''
                if (
                    not original.startswith('./')
                    and not original.startswith(prefix)
                    and original.rstrip('/') != root_path.rstrip('/')
                ):
                    notebook_task['notebook_path'] = f'{root_path.rstrip("/")}/{original.lstrip("/")}'
                task['notebook_task'] = notebook_task
            return task

        return WorkspaceDefinitionStore._apply_task_override(tasks, _rewrite)

    @staticmethod
    def _apply_compute_type_override(tasks: list[dict[str, Any]], compute_type: str) -> list[dict[str, Any]]:
        """Returns a new tasks list switched to the requested compute type.

        When *compute_type* is ``"serverless"``, existing ``new_cluster`` definitions
        are removed so the task runs on serverless compute.

        Args:
            tasks: Job task dicts to process.
            compute_type: Desired compute type (``"serverless"`` or ``"classic"``).

        Returns:
            New list of task dicts with compute settings applied.
        """

        def _rewrite(task: dict[str, Any]) -> dict[str, Any]:
            if compute_type == 'serverless':
                task.pop('new_cluster', None)
                task.pop('existing_cluster_id', None)
            return task

        return WorkspaceDefinitionStore._apply_task_override(tasks, _rewrite)

    @staticmethod
    def _apply_workspace_url_override(tasks: list[dict[str, Any]], workspace_url: str) -> list[dict[str, Any]]:
        """Returns a new tasks list with Databricks linked-service hosts set to *workspace_url*.

        Args:
            tasks: Job task dicts to process.
            workspace_url: Databricks workspace URL to set on every linked service.

        Returns:
            New list of task dicts with workspace URLs applied.
        """

        def _rewrite(task: dict[str, Any]) -> dict[str, Any]:
            new_cluster = task.get('new_cluster')
            if isinstance(new_cluster, dict) and 'host_name' in new_cluster:
                new_cluster = dict(new_cluster)
                new_cluster['host_name'] = workspace_url
                task['new_cluster'] = new_cluster
            return task

        return WorkspaceDefinitionStore._apply_task_override(tasks, _rewrite)

    @staticmethod
    def _upload_notebooks(client: WorkspaceClient, notebooks: Iterable[NotebookArtifact]) -> None:
        """
        Uploads generated notebooks to the workspace.

        Args:
            client: Authenticated workspace client.
            notebooks: Notebook artifacts to upload.
        """
        for notebook in notebooks:
            folder = "/".join(notebook.file_path.split("/")[:-1])
            client.workspace.mkdirs(folder)
            client.workspace.import_(
                content=base64.b64encode(notebook.content.encode()).decode(),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON if notebook.language == "python" else Language.SCALA,
                overwrite=True,
                path=notebook.file_path,
            )

    @staticmethod
    def _materialize_pipelines(
        client: WorkspaceClient,
        pipelines: Iterable[PipelineInstruction],
    ) -> None:
        """
        Creates DLT pipelines referenced by declarative copy activities.

        Args:
            client: Authenticated workspace client.
            pipelines: DLT pipeline creation instructions as a ``list[PipelineInstruction]``.

        Raises:
            ValueError: If the pipeline cannot be created.
        """
        for instruction in pipelines:
            response = client.pipelines.create(
                allow_duplicate_names=True,
                catalog=instruction.catalog,
                channel="CURRENT",
                continuous=False,
                development=False,
                libraries=[PipelineLibrary(notebook=NotebookLibrary(path=instruction.file_path))],
                name=instruction.name,
                photon=True,
                serverless=True,
                target=instruction.target,
            )
            pipeline_id = response.pipeline_id
            if pipeline_id is None:
                raise ValueError("Created pipeline ID cannot be None")
            pipeline_task = instruction.task_ref.get("pipeline_task")
            if isinstance(pipeline_task, PipelineTask):
                instruction.task_ref["pipeline_task"] = PipelineTask(pipeline_id=pipeline_id)
            else:
                instruction.task_ref["pipeline_task"] = {"pipeline_id": pipeline_id}

    @staticmethod
    def _materialize_secrets(
        client: WorkspaceClient,
        secrets_to_create: Iterable[SecretInstruction],
    ) -> None:
        """
        Ensures Databricks secret scopes and values exist for the workflow.

        Args:
            client: Authenticated workspace client.
            secrets_to_create: Secret instructions collected during translation.
        """
        if not secrets_to_create:
            return
        scopes = [scope.name for scope in client.secrets.list_scopes()]
        if DEFAULT_CREDENTIALS_SCOPE not in scopes:
            client.secrets.create_scope(scope=DEFAULT_CREDENTIALS_SCOPE)
        for secret in secrets_to_create:
            value = secret.provided_value or "PLACEHOLDER_SECRET_VALUE"
            client.secrets.put_secret(scope=secret.scope, key=secret.key, string_value=value)

    def _ensure_notebook_dependencies(
        self,
        client: WorkspaceClient,
        tasks: Iterable[dict],
    ) -> None:
        """
        Validates notebook tasks to ensure referenced notebooks exist.

        Args:
            client: Authenticated workspace client.
            tasks: Job tasks to verify.
        """
        for task in tasks:
            if "notebook_task" in task:
                self._ensure_notebook_exists(client, task)
            for_each_task = task.get("for_each_task")
            if for_each_task:
                inner_task = for_each_task.get("task")
                if inner_task is None:
                    continue
                inner_task_list = inner_task if isinstance(inner_task, list) else [inner_task]
                self._ensure_notebook_dependencies(client, inner_task_list)

    @staticmethod
    def _ensure_notebook_exists(client: WorkspaceClient, task: dict) -> None:
        """
        Verifies that a notebook referenced by a task exists in the workspace.

        Args:
            client: Authenticated workspace client.
            task: Notebook task dictionary.

        Raises:
            ValueError: If the expected notebook task properties are missing.
            ValueError: If the notebook path is not found in the notebook task properties.
        """
        notebook_task = task.get("notebook_task")
        if notebook_task is None:
            raise ValueError('No "notebook_task" found in task')
        if isinstance(notebook_task, NotebookTask):
            notebook_path_value = notebook_task.notebook_path
        else:
            notebook_path_value = notebook_task.get("notebook_path")
        if notebook_path_value is None:
            raise ValueError('No "notebook_path" found in notebook_task')
        notebook_path = f"/Workspace{notebook_path_value}"
        try:
            client.workspace.get_status(path=notebook_path)
        except Exception:
            warnings.warn(f"Notebook {notebook_path} not found in target workspace", stacklevel=3)

    def _write_asset_bundle(
        self,
        prepared: PreparedWorkflow,
        bundle_dir: str,
        download_notebooks: bool = False,
    ) -> None:
        """
        Writes an asset bundle layout containing job configuration and related artifacts.

        When *download_notebooks* is True the workflow is expected to have been
        prepared with ``defer_root_path=True`` so that workspace paths are still
        in their original form for downloading.  After extraction the root_path
        rewrite is applied to the prepared workflow.

        Args:
            prepared: Prepared workflow artifacts.
            bundle_dir: Destination directory for the bundle.
            download_notebooks: If True, downloads referenced notebooks from the workspace.
        """
        os.makedirs(bundle_dir, exist_ok=True)
        bundle_name = prepared.pipeline.name or "workflow"
        jobs_dir = os.path.join(bundle_dir, "resources", "jobs")
        pipelines_dir = os.path.join(bundle_dir, "resources", "pipelines")
        notebooks_dir = os.path.join(bundle_dir, "notebooks")

        os.makedirs(jobs_dir, exist_ok=True)
        os.makedirs(pipelines_dir, exist_ok=True)
        os.makedirs(notebooks_dir, exist_ok=True)

        all_tasks = prepared.tasks + [t for wf in prepared.inner_workflows for t in wf.tasks]

        if download_notebooks:
            workspace_paths = {
                p for p in self._extract_workspace_notebook_paths(all_tasks) if not p.startswith("/wkmigrate/")
            }
            if workspace_paths:
                self._update_notebook_paths_for_bundle(
                    all_tasks,
                    self._download_workspace_notebooks(workspace_paths, notebooks_dir),
                )

        # Apply deferred root_path rewrite after download-path extraction.
        # Skip bundle-relative paths (starting with './') that were already
        # rewritten by _update_notebook_paths_for_bundle.
        root_path = self._effective_root_path()
        if root_path is not None and download_notebooks:
            for activity, task in zip(prepared.activities, self._apply_root_path_override(prepared.tasks, root_path)):
                activity.task = task
            prepared = dataclasses.replace(
                prepared,
                activities=self._apply_root_path_to_activities(list(prepared.activities), root_path),
            )
            # Also rewrite task dicts inside inner workflows so that inner job
            # YAML files receive the root_path prefix.
            for inner_wf in prepared.inner_workflows:
                rewritten = self._apply_root_path_override(inner_wf.tasks, root_path)
                for activity, task in zip(inner_wf.activities, rewritten):
                    activity.task = task

        if prepared.inner_workflows:
            self._assign_inner_job_refs(prepared.tasks)

        job_settings = {
            "name": prepared.pipeline.name,
            "parameters": prepared.pipeline.parameters,
            "schedule": prepared.pipeline.schedule,
            "tags": prepared.pipeline.tags,
            "tasks": prepared.tasks,
        }
        job_file = os.path.join(jobs_dir, f"{bundle_name}.yml")
        job_resource = {"resources": {"jobs": {bundle_name: self._serialize_for_json(job_settings)}}}
        with open(job_file, "w", encoding="utf-8") as job_handle:
            yaml.safe_dump(job_resource, job_handle, sort_keys=False)

        for inner_wf in prepared.inner_workflows:
            inner_name = inner_wf.pipeline.name or "inner_job"
            inner_job_file = os.path.join(jobs_dir, f"{inner_name}.yml")
            self._assign_inner_job_refs(inner_wf.tasks)
            inner_settings = {
                "name": inner_wf.pipeline.name,
                "parameters": inner_wf.pipeline.parameters,
                "schedule": inner_wf.pipeline.schedule,
                "tags": inner_wf.pipeline.tags,
                "tasks": inner_wf.tasks,
            }
            inner_resource = {"resources": {"jobs": {inner_name: self._serialize_for_json(inner_settings)}}}
            with open(inner_job_file, "w", encoding="utf-8") as inner_handle:
                yaml.safe_dump(inner_resource, inner_handle, sort_keys=False)

        self._write_notebooks(prepared.all_notebooks, notebooks_dir)
        self._write_pipeline_resources(prepared.all_pipelines, pipelines_dir)
        self._write_secrets(prepared.all_secrets, bundle_dir)
        self._write_unsupported(prepared.pipeline.not_translatable or [], bundle_dir)
        self._write_bundle_manifest(bundle_name, job_file, prepared.all_pipelines, bundle_dir, prepared.inner_workflows)

    def _write_notebooks(self, notebooks: Iterable[NotebookArtifact], output_dir: str) -> None:
        """
        Writes generated notebooks as Python files to a ``notebooks`` folder within the output directory.

        Args:
            notebooks: Notebook artifacts produced during translation as a ``list[NotebookArtifact]``.
            output_dir: Destination directory for the ``notebooks`` folder.
        """
        os.makedirs(output_dir, exist_ok=True)
        for notebook in notebooks:
            file_path = os.path.join(output_dir, notebook.file_path.lstrip("/"))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as notebook_file:
                notebook_file.write(notebook.content)

    def _extract_workspace_notebook_paths(self, tasks: Iterable[dict]) -> set[str]:
        """
        Recursively extracts notebook paths referenced in notebook tasks.

        Args:
            tasks: Job tasks to scan for notebook paths.

        Returns:
            Set of unique workspace notebook paths.
        """
        paths: set[str] = set()
        for task in tasks:
            notebook_task = task.get("notebook_task")
            if notebook_task:
                path = notebook_task.get("notebook_path")
                if path:
                    paths.add(path)
            for_each_task = task.get("for_each_task")
            if for_each_task:
                nested_task = for_each_task.get("task")
                if isinstance(nested_task, dict):
                    paths.update(self._extract_workspace_notebook_paths([nested_task]))
                elif isinstance(nested_task, list):
                    paths.update(self._extract_workspace_notebook_paths(nested_task))
        return paths

    def _download_workspace_notebooks(
        self,
        notebook_paths: Iterable[str],
        notebooks_dir: str,
    ) -> dict[str, str]:
        """
        Downloads notebooks from the workspace and saves them locally.

        Args:
            notebook_paths: Workspace paths to notebooks to download.
            notebooks_dir: Local directory to save the notebooks.

        Returns:
            Mapping of original workspace paths to bundle-relative paths.
        """
        client = self._get_workspace_client()
        os.makedirs(notebooks_dir, exist_ok=True)
        path_mapping: dict[str, str] = {}

        for workspace_path in notebook_paths:
            result = self._download_single_notebook(client, workspace_path, notebooks_dir)
            if result:
                path_mapping[workspace_path] = result

        return path_mapping

    def _download_single_notebook(
        self,
        client: WorkspaceClient,
        workspace_path: str,
        notebooks_dir: str,
    ) -> str | None:
        """
        Downloads a single notebook from the workspace.

        Args:
            client: Authenticated workspace client.
            workspace_path: Workspace path to the notebook.
            notebooks_dir: Local directory to save the notebook.

        Returns:
            Bundle-relative path if successful, None otherwise.
        """
        try:
            return self._download_notebook_content(client, workspace_path, notebooks_dir)
        except Exception as e:
            warnings.warn(f"Failed to download notebook {workspace_path}: {e}", stacklevel=2)
            return None

    def _download_notebook_content(
        self, client: WorkspaceClient, workspace_path: str, notebooks_dir: str
    ) -> str | None:
        """
        Downloads the content of a notebook from the workspace.

        Args:
            client: Authenticated workspace client.
            workspace_path: Workspace path to the notebook.
            notebooks_dir: Local directory to save the notebook.

        Returns:
            Content of the notebook as a string.
        """
        export_response = client.workspace.export(path=workspace_path, format=ExportFormat.SOURCE)
        content = export_response.content
        if content is None:
            warnings.warn(f"Notebook {workspace_path} has no content", stacklevel=2)
            return None

        file_name = os.path.basename(workspace_path)
        if not file_name.endswith(".py"):
            file_name += ".py"
        local_full_path = os.path.join(notebooks_dir, file_name)
        os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

        with open(local_full_path, "w", encoding="utf-8") as f:
            f.write(base64.b64decode(content).decode("utf-8"))
        return f"./notebooks/{file_name}"

    def _update_notebook_paths_for_bundle(self, tasks: list[dict], path_mapping: dict[str, str]) -> None:
        """
        Updates notebook paths in tasks to use local bundle paths.

        Args:
            tasks: Job tasks to update.
            path_mapping: Mapping of workspace paths to local paths.
        """
        for task in tasks:
            notebook_task = task.get("notebook_task")
            if notebook_task:
                original_path = notebook_task.get("notebook_path")
                if original_path and original_path in path_mapping:
                    notebook_task["notebook_path"] = path_mapping[original_path]
            for_each_task = task.get("for_each_task")
            if for_each_task:
                nested_task = for_each_task.get("task")
                if isinstance(nested_task, dict):
                    self._update_notebook_paths_for_bundle([nested_task], path_mapping)
                elif isinstance(nested_task, list):
                    self._update_notebook_paths_for_bundle(nested_task, path_mapping)

    def _write_pipeline_resources(self, pipelines: Iterable[PipelineInstruction], pipelines_dir: str) -> None:
        """
        Writes pipeline resource definitions used by copy-data activities.

        Args:
            pipelines: Pipeline instructions to materialize.
            pipelines_dir: Destination directory for pipeline JSON files.
        """
        os.makedirs(pipelines_dir, exist_ok=True)
        for instruction in pipelines:
            pipeline_payload = {
                "resources": {
                    "pipelines": {
                        instruction.name: {
                            "name": instruction.name,
                            "allow_duplicate_names": True,
                            "catalog": instruction.catalog,
                            "channel": "CURRENT",
                            "development": False,
                            "continuous": False,
                            "photon": True,
                            "serverless": True,
                            "target": instruction.target,
                            "libraries": [{"notebook": {"path": instruction.file_path}}],
                        }
                    }
                }
            }
            pipeline_file = os.path.join(pipelines_dir, f"{instruction.name}.yml")
            with open(pipeline_file, "w", encoding="utf-8") as pipeline_handle:
                yaml.safe_dump(pipeline_payload, pipeline_handle, sort_keys=False)

    def _write_bundle_manifest(
        self,
        bundle_name: str,
        job_file: str,
        pipelines: Iterable[PipelineInstruction],
        bundle_dir: str,
        inner_workflows: Iterable[PreparedWorkflow] | None = None,
    ) -> None:
        """
        Writes a minimal Databricks asset bundle manifest (databricks.yml).

        Args:
            bundle_name: Name for the bundle and job resource.
            job_file: Path to the job definition JSON.
            pipelines: Pipeline instructions to include.
            bundle_dir: Destination directory for the manifest.
            inner_workflows: Inner workflows whose jobs are included in the bundle.
        """
        pipeline_resources = [os.path.join("resources", "pipelines", f"{pipeline.name}.yml") for pipeline in pipelines]
        job_resources = [os.path.relpath(job_file, bundle_dir)]
        for inner_wf in inner_workflows or []:
            inner_name = inner_wf.pipeline.name
            if inner_name:
                job_resources.append(os.path.join("resources", "jobs", f"{inner_name}.yml"))
        bundle_resources = job_resources + pipeline_resources
        manifest = {
            "bundle": {"name": bundle_name},
            "targets": {
                "default": {
                    "workspace": {
                        "host": self.host_name,
                    }
                }
            },
            "include": bundle_resources,
        }
        manifest_path = os.path.join(bundle_dir, "databricks.yml")
        with open(manifest_path, "w", encoding="utf-8") as manifest_handle:
            yaml.safe_dump(manifest, manifest_handle, sort_keys=False)

    _NEW_CLUSTER_EXCLUDED_KEYS = {"service_name", "service_type", "host_name"}

    def _serialize_for_json(self, obj, parent_key: str | None = None):
        """
        Recursively converts dataclasses and other non-JSON-native objects into serializable structures.
        """
        if hasattr(obj, "as_dict"):
            items = obj.as_dict().items()
            if parent_key == "new_cluster":
                return {
                    k: self._serialize_for_json(v, k)
                    for k, v in items
                    if v is not None and k not in self._NEW_CLUSTER_EXCLUDED_KEYS
                }
            return {k: self._serialize_for_json(v, k) for k, v in items if v is not None}
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            items = dataclasses.asdict(obj).items()
            if parent_key == "new_cluster":
                return {
                    k: self._serialize_for_json(v, k)
                    for k, v in items
                    if v is not None and k not in self._NEW_CLUSTER_EXCLUDED_KEYS
                }
            return {k: self._serialize_for_json(v, k) for k, v in items if v is not None}
        if isinstance(obj, dict):
            if parent_key == "new_cluster":
                return {
                    k: self._serialize_for_json(v, k)
                    for k, v in obj.items()
                    if v is not None and k not in self._NEW_CLUSTER_EXCLUDED_KEYS
                }
            return {k: self._serialize_for_json(v, k) for k, v in obj.items() if v is not None}
        if isinstance(obj, list):
            return [self._serialize_for_json(v) for v in obj if v is not None]
        if isinstance(obj, tuple):
            return tuple(self._serialize_for_json(v) for v in obj if v is not None)
        return obj

    @staticmethod
    def _write_secrets(secrets_to_write: Iterable[SecretInstruction], output_dir: str) -> None:
        """
        Writes resolved secret metadata to a ``secrets.json`` file in the output directory.

        Args:
            secrets_to_write: Secret instructions produced during translation.
            output_dir: Destination directory for the ``secrets.json`` file.
        """
        secrets_file = os.path.join(output_dir, "secrets.json")
        formatted = [
            {
                "scope": secret.scope,
                "key": secret.key,
                "linked_service_name": secret.service_name,
                "linked_service_type": secret.service_type,
                "provided_value": secret.provided_value,
            }
            for secret in secrets_to_write
        ]
        with open(secrets_file, "w", encoding="utf-8") as secrets_handle:
            json.dump(formatted, secrets_handle, indent=2, ensure_ascii=False)

    def _write_unsupported(self, unsupported: Iterable[dict], output_dir: str) -> None:
        """
        Writes the unsupported report to an ``unsupported.json`` file in the output directory.

        Args:
            unsupported: Warning entries collected during translation as a ``list[dict]``.
            output_dir: Destination directory for the ``unsupported.json`` file.
        """
        unsupported_file = os.path.join(output_dir, "unsupported.json")
        formatted = self._format_unsupported_entries(unsupported)
        with open(unsupported_file, "w", encoding="utf-8") as unsupported_handle:
            json.dump(formatted, unsupported_handle, indent=2, ensure_ascii=False)

    def _login_workspace_client(self) -> WorkspaceClient:
        """
        Authenticates with Databricks using the configured auth type.

        Returns:
            Databricks ``WorkspaceClient`` object.

        Raises:
            ValueError: If the authentication type is not one of 'pat', 'basic', or 'azure-client-secret'.
        """
        if self.authentication_type == "pat":
            return self._login_pat()
        if self.authentication_type == "basic":
            return self._login_basic()
        if self.authentication_type == "azure-client-secret":
            return self._login_client_secret()
        raise ValueError("Unsupported authentication type")

    def _login_pat(self) -> WorkspaceClient:
        """
        Authenticates via personal access token.

        Returns:
            Databricks ``WorkspaceClient`` object.

        Raises:
            ValueError: If the authentication type is not provided.
            ValueError: If the host name is not provided.
            ValueError: If the personal access token is not provided.
        """
        if self.authentication_type is None:
            raise ValueError('No value provided for "authentication_type"')
        if self.host_name is None:
            raise ValueError('No value provided for "host_name"')
        if self.pat is None:
            raise ValueError('No value provided for "pat" with access token authentication')
        return WorkspaceClient(auth_type=self.authentication_type, host=self.host_name, token=self.pat)

    def _login_basic(self) -> WorkspaceClient:
        """
        Authenticates via username/password.

        Returns:
            Databricks ``WorkspaceClient`` object.

        Raises:
            ValueError: If the authentication type is not provided.
            ValueError: If the host name is not provided.
            ValueError: If the username is not provided.
            ValueError: If the password is not provided.
        """
        if self.authentication_type is None:
            raise ValueError('No value provided for "authentication_type"')
        if self.host_name is None:
            raise ValueError('No value provided for "host_name"')
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

    def _login_client_secret(self) -> WorkspaceClient:
        """
        Authenticates via Azure service principal credentials.

        Returns:
            Databricks ``WorkspaceClient`` object.

        Raises:
            ValueError: If the authentication type is not provided.
            ValueError: If the host name is not provided.
            ValueError: If the resource ID is not provided.
            ValueError: If the tenant ID is not provided.
            ValueError: If the client ID is not provided.
            ValueError: If the client secret is not provided.
        """
        if self.authentication_type is None:
            raise ValueError('No value provided for "authentication_type"')
        if self.host_name is None:
            raise ValueError('No value provided for "host_name"')
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

    def _get_workspace_client(self) -> WorkspaceClient:
        """
        Returns an authenticated Databricks client.

        Returns:
            Databricks ``WorkspaceClient`` object.

        Raises:
            ValueError: If the client has not been created.
        """
        if self.workspace_client is None:
            raise ValueError("workspace_client is not initialized")
        return self.workspace_client

    def _build_job_payload_for_api(self, prepared_workflow: PreparedWorkflow) -> dict:
        """
        Converts prepared settings to the structure expected by the Jobs API.

        Args:
            prepared_workflow: Prepared workflow as a ``PreparedWorkflow``.

        Returns:
            Jobs API payload as a ``dict``.
        """
        api_payload: dict[str, Any] = {}
        api_payload["name"] = prepared_workflow.pipeline.name
        api_payload["tags"] = prepared_workflow.pipeline.tags
        api_payload["tasks"] = [Task.from_dict(self._serialize_for_json(task)) for task in prepared_workflow.tasks]

        if prepared_workflow.pipeline.parameters is not None:
            api_payload["parameters"] = [
                JobParameterDefinition.from_dict(parameter) for parameter in prepared_workflow.pipeline.parameters
            ]

        if prepared_workflow.pipeline.schedule is not None:
            api_payload["schedule"] = CronSchedule.from_dict(prepared_workflow.pipeline.schedule)

        return api_payload

    def _create_inner_jobs(
        self, client: WorkspaceClient, inner_workflows: Iterable[PreparedWorkflow]
    ) -> dict[str, int]:
        """
        Creates additional jobs required for nested activities and returns their IDs.
        """
        job_ids: dict[str, int] = {}
        for inner_wf in inner_workflows:
            inner_payload = self._build_job_payload_for_api(inner_wf)
            response = client.jobs.create(**inner_payload)
            job_id = response.job_id
            if job_id is None:
                raise ValueError("Failed to create inner job")
            inner_name = inner_wf.pipeline.name
            if inner_name:
                job_ids[inner_name] = job_id
        return job_ids

    def _assign_inner_job_ids(self, tasks: Iterable[dict], job_id_map: dict[str, int]) -> None:
        """
        Replaces placeholder run_job_task job IDs with created inner job IDs.
        """
        for task in tasks:
            run_job_task = task.get("run_job_task")
            if run_job_task and isinstance(run_job_task, str) and run_job_task.startswith("__INNER_JOB__:"):
                job_name = run_job_task.split(":", 1)[1]
                if job_name in job_id_map:
                    task["run_job_task"] = {"job_id": job_id_map[job_name]}
            for_each_task = task.get("for_each_task")
            if for_each_task:
                nested_task = for_each_task.get("task")
                if isinstance(nested_task, dict):
                    self._assign_inner_job_ids([nested_task], job_id_map)
                elif isinstance(nested_task, list):
                    self._assign_inner_job_ids(nested_task, job_id_map)

    def _assign_inner_job_refs(self, tasks: Iterable[dict]) -> None:
        """
        Replaces placeholder run_job_task job IDs with bundle resource references.
        """
        for task in tasks:
            run_job_task = task.get("run_job_task")
            if run_job_task:
                if isinstance(run_job_task, str) and run_job_task.startswith("__INNER_JOB__:"):
                    job_name = run_job_task.split(":", 1)[1]
                    task["run_job_task"] = {"job_id": f"${{resources.jobs.{job_name}.id}}"}
            job_id = task.get("job_id")
            if job_id and isinstance(job_id, str):
                task["job_id"] = f"${{resources.jobs.{job_id}.id}}"
            for_each_task = task.get("for_each_task")
            if for_each_task:
                nested_task = for_each_task.get("task")
                if isinstance(nested_task, dict):
                    self._assign_inner_job_refs([nested_task])
                elif isinstance(nested_task, list):
                    self._assign_inner_job_refs(nested_task)

    @staticmethod
    def _format_unsupported_entries(warning_entries: Iterable[dict]) -> list[dict]:
        """
        Normalizes warning entries before writing to an ``unsupported.json`` file.

        Args:
            warning_entries: Raw warning entries produced during translation as a ``list[dict]``.

        Returns:
            Normalized unsupported entries as a ``list[dict]``.
        """
        formatted = []
        for warning in warning_entries:
            activity_name = warning.get("activity_name") or warning.get("property", "pipeline")
            activity_type = warning.get("activity_type") or "not_translatable"
            metadata = {key: value for key, value in warning.items() if key not in {"activity_name", "activity_type"}}
            formatted.append(
                {
                    "activity_name": activity_name,
                    "activity_type": activity_type,
                    "reason": warning.get("message", "Property could not be translated"),
                    "metadata": metadata,
                }
            )
        return formatted

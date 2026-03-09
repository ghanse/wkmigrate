"""This module defines internal representations for pipelines and activities.

Pipelines in this module represent the top-level container for a pipeline. Each pipeline contains
metadata, parameters, schedules, and tasks. Pipelines are translated from ADF payloads into internal
representations that can be used to generate Databricks Lakeflow jobs.

Activities in this module represent the core components of a pipeline. Each activity contains
metadata about the activity's type, name, and parameters. Activities are translated from ADF
payloads into internal representations that can be used to generate Databricks Lakeflow jobs.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
from wkmigrate.models.ir.datasets import Dataset


@dataclass(slots=True)
class Pipeline:
    """
    Pipeline IR object produced by the translator.

    Attributes:
        name: Logical pipeline name derived from the ADF pipeline.
        parameters: List of pipeline parameter definitions, or ``None`` when no parameters are defined.
        schedule: Serialized schedule definition for the pipeline trigger, if any.
        tasks: Ordered list of ``PipelineTask`` wrappers that make up the workflow.
        tags: Dictionary of system and user-defined tags attached to the workflow.
        not_translatable: Collection of warnings describing properties that could not be translated.
    """

    name: str
    parameters: list[dict] | None
    schedule: dict | None
    tasks: list[Activity]
    tags: dict
    not_translatable: list[dict] = field(default_factory=list)


@dataclass(slots=True)
class Activity:
    """
    Base class for translated pipeline activities.

    Attributes:
        name: Logical name of the activity as defined in ADF.
        task_key: Unique identifier used to reference this task within a workflow.
        description: Free-form description of what the activity does.
        timeout_seconds: Maximum allowed execution time in seconds before the task is cancelled.
        max_retries: Maximum number of retry attempts on failure.
        min_retry_interval_millis: Minimum delay between retry attempts in milliseconds.
        depends_on: List of upstream task dependencies that must complete before this task runs.
        new_cluster: Cluster configuration dictionary for tasks that provision a new cluster.
        libraries: List of library dependencies (e.g. JARs or Python wheels) used by the activity.
    """

    name: str
    task_key: str
    description: str | None = None
    timeout_seconds: int | None = None
    max_retries: int | None = None
    min_retry_interval_millis: int | None = None
    depends_on: list["Dependency"] | None = None
    new_cluster: dict | None = None
    libraries: list[dict[str, Any]] | None = None


@dataclass(slots=True, kw_only=True)
class DatabricksNotebookActivity(Activity):
    """
    Databricks notebook activity metadata.

    Attributes:
        notebook_path: Workspace path to the Databricks notebook to execute.
        base_parameters: Mapping of parameter names to default values passed to the notebook.
        linked_service_definition: Raw ADF linked-service dictionary used to configure the cluster.
    """

    notebook_path: str
    base_parameters: dict[str, str] | None = None
    linked_service_definition: dict | None = None


@dataclass(slots=True)
class CopyActivity(Activity):
    """
    Copy activity metadata including datasets and mappings.

    Attributes:
        source_dataset: Parsed IR representation of the source dataset.
        sink_dataset: Parsed IR representation of the sink dataset.
        source_properties: Parsed dataset properties associated with the source.
        sink_properties: Parsed dataset properties associated with the sink.
        column_mapping: Column-level mappings from source to sink, if provided.
    """

    source_dataset: Dataset | None = None
    sink_dataset: Dataset | None = None
    source_properties: dict[str, Any] | None = None
    sink_properties: dict[str, Any] | None = None
    column_mapping: list[ColumnMapping] | None = None


@dataclass(slots=True, kw_only=True)
class ForEachActivity(Activity):
    """
    ForEach activity metadata including inner activities.

    Attributes:
        items_string: Serialized iterable expression that drives the loop.
        for_each_task: Task to execute for each item.
        concurrency: Maximum number of loop iterations to run in parallel.
    """

    items_string: str
    for_each_task: Activity
    concurrency: int | None = None


@dataclass(slots=True, kw_only=True)
class RunJobActivity(Activity):
    """
    Run Job activity metadata.

    Attributes:
        name: Name of the job to run.
        pipeline: Pipeline to run, if no existing job ID is provided.
        existing_job_id: ID of the existing job to run.
        job_parameters: Key-value pairs passed to the job at runtime, overriding job defaults.
    """

    name: str
    pipeline: Pipeline | None = None
    existing_job_id: str | None = None
    job_parameters: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class SparkJarActivity(Activity):
    """
    Spark JAR activity metadata.

    Attributes:
        main_class_name: Fully qualified main class to invoke within the JAR.
        parameters: List of string arguments passed to the main class.
        libraries: Additional library descriptors attached to the task.
    """

    main_class_name: str
    parameters: list[str] | None = None
    libraries: list[dict[str, Any]] | None = None


@dataclass(slots=True, kw_only=True)
class SparkPythonActivity(Activity):
    """
    Spark Python activity metadata.

    Attributes:
        python_file: Path to the Python file or wheel to execute.
        parameters: List of string arguments passed to the Python entry point.
    """

    python_file: str
    parameters: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class LookupActivity(Activity):
    """
    Lookup activity metadata.

    Translates an ADF Lookup activity into a notebook task that reads data with
    Spark and publishes the result as a Databricks task value.

    Attributes:
        source_dataset: Parsed IR representation of the lookup dataset.
        source_properties: Parsed source format/connection properties from the ADF source block.
        first_row_only: When ``True`` only the first row is returned; mirrors the ADF setting.
        source_query: Optional SQL query or stored-procedure call for database sources.
    """

    source_dataset: Dataset | None = None
    source_properties: dict[str, Any] | None = None
    first_row_only: bool = True
    source_query: str | None = None


@dataclass(slots=True, kw_only=True)
class WebActivity(Activity):
    """
    Web activity metadata.

    Translates an ADF Web activity into a notebook task that submits an HTTP request
    using the Python ``requests`` library and publishes the response as Databricks task values.

    Attributes:
        url: Target URL for the HTTP request.
        method: HTTP method (for example ``GET``, ``POST``, ``PUT``, ``DELETE``).
        body: Optional request body. Passed as JSON when the body is a dict, or as raw data otherwise.
        headers: Optional HTTP headers dictionary.
        authentication: Parsed authentication configuration, or ``None`` when no auth is required.
        disable_cert_validation: When ``True``, TLS certificate verification is skipped.
        http_request_timeout_seconds: Optional HTTP request timeout in seconds from the ADF activity.
        turn_off_async: When ``True``, the activity executes synchronously rather than polling.
    """

    url: str
    method: str
    body: Any = None
    headers: dict[str, str] | None = None
    authentication: Authentication | None = None
    disable_cert_validation: bool = False
    http_request_timeout_seconds: int | None = None
    turn_off_async: bool = False


@dataclass(slots=True, kw_only=True)
class IfConditionActivity(Activity):
    """
    If Condition activity metadata.

    Attributes:
        op: Name of the comparison operator derived from the ADF expression.
        left: Left-hand operand used in the conditional expression.
        right: Right-hand operand used in the conditional expression.
        child_activities: Activities that form the body of the conditional branch.
    """

    op: str
    left: str
    right: str
    child_activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class SetVariableActivity(Activity):
    """
    SetVariable activity metadata.

    Attributes:
        variable_name: Variable name to set.
        variable_value: Python expression string that evaluates to the variable value.
    """

    variable_name: str
    variable_value: str


@dataclass(slots=True)
class ColumnMapping:
    """
    Represents a column-level mapping between datasets.

    Attributes:
        source_column_name: Column name or ordinal-derived alias in the source dataset.
        sink_column_name: Column name in the sink dataset that receives the value.
        sink_column_type: Target column data type, if available.
    """

    source_column_name: str
    sink_column_name: str
    sink_column_type: str | None = None


@dataclass(slots=True)
class Dependency:
    """
    Represents a dependency on another task.

    Attributes:
        task_key: Task key of the upstream activity this task depends on.
        outcome: Required outcome of the upstream task (for example ``Succeeded``) for this dependency.
    """

    task_key: str
    outcome: str | None = None


@dataclass(slots=True)
class Authentication:
    """
    Authentication configuration for an HTTP request.

    Attributes:
        auth_type: Authentication type (e.g. 'basic').
        username: Optional username for Basic authentication.
        password_secret_key: Optional secret scope key that holds the password.
    """

    auth_type: str
    username: str | None = None
    password_secret_key: str | None = None

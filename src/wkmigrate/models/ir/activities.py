"""This module defines internal representations for pipeline activities.

Activities in this module represent the core components of a pipeline. Each activity contains
metadata about the activity's type, name, and parameters. Activities are translated from ADF
payloads into internal representations that can be used to generate Databricks Lakeflow jobs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from wkmigrate.models.ir.datasets import Dataset


@dataclass
class Activity:
    """
    Base class for translated pipeline activities.

    Attributes:
        name: Logical name of the activity as defined in ADF.
        task_key: Unique identifier used to reference this task within a workflow.
        activity_type: Original ADF activity type string (for example ``DatabricksNotebook``).
        description: Free-form description of what the activity does.
        timeout_seconds: Maximum allowed execution time in seconds before the task is cancelled.
        max_retries: Maximum number of retry attempts on failure.
        min_retry_interval_millis: Minimum delay between retry attempts in milliseconds.
        depends_on: List of upstream task dependencies that must complete before this task runs.
        new_cluster: Cluster configuration dictionary for tasks that provision a new cluster.
    """

    name: str
    task_key: str
    activity_type: str
    description: str | None = None
    timeout_seconds: int | None = None
    max_retries: int | None = None
    min_retry_interval_millis: int | None = None
    depends_on: list["Dependency"] | None = None
    new_cluster: dict | None = None


@dataclass
class DatabricksNotebookActivity(Activity):
    """
    Databricks notebook activity metadata.

    Attributes:
        notebook_path: Workspace path to the Databricks notebook to execute.
        base_parameters: Mapping of parameter names to default values passed to the notebook.
        linked_service_definition: Raw ADF linked-service dictionary used to configure the cluster.
    """

    notebook_path: str | None = None
    base_parameters: dict[str, str] | None = None
    linked_service_definition: dict | None = None


@dataclass
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


@dataclass
class ForEachActivity(Activity):
    """
    ForEach activity metadata including inner activities.

    Attributes:
        items_string: Serialized iterable expression that drives the loop.
        inner_activities: List of activities to execute for each item.
        concurrency: Maximum number of loop iterations to run in parallel.
    """

    items_string: str | None = None
    inner_activities: list[Activity] = field(default_factory=list)
    concurrency: int | None = None


@dataclass
class SparkJarActivity(Activity):
    """
    Spark JAR activity metadata.

    Attributes:
        main_class_name: Fully qualified main class to invoke within the JAR.
        parameters: List of string arguments passed to the main class.
        libraries: Additional library descriptors attached to the task.
    """

    main_class_name: str | None = None
    parameters: list[str] | None = None
    libraries: list[dict[str, Any]] | None = None


@dataclass
class SparkPythonActivity(Activity):
    """
    Spark Python activity metadata.

    Attributes:
        python_file: Path to the Python file or wheel to execute.
        parameters: List of string arguments passed to the Python entry point.
    """

    python_file: str | None = None
    parameters: list[str] | None = None


@dataclass
class IfConditionActivity(Activity):
    """
    If Condition activity metadata.

    Attributes:
        op: Name of the comparison operator derived from the ADF expression.
        left: Left-hand operand used in the conditional expression.
        right: Right-hand operand used in the conditional expression.
        child_activities: Activities that form the body of the conditional branch.
    """

    op: str | None = None
    left: str | None = None
    right: str | None = None
    child_activities: list[Activity] = field(default_factory=list)


@dataclass
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


@dataclass
class Dependency:
    """
    Represents a dependency on another task.

    Attributes:
        task_key: Task key of the upstream activity this task depends on.
        outcome: Required outcome of the upstream task (for example ``Succeeded``) for this dependency.
    """

    task_key: str | None
    outcome: str | None

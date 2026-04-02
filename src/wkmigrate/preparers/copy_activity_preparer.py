"""
This module defines a preparer for Copy activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Copy activity. This includes a notebook or pipeline task
definition, notebook artifacts, and secrets to be created in the target workspace.
"""

from __future__ import annotations

from dataclasses import asdict
from urllib.parse import urlparse

import autopep8  # type: ignore

from wkmigrate.parsers.dataset_parsers import (
    collect_data_source_secrets,
    merge_dataset_definition,
    parse_spark_data_type,
)
from wkmigrate.code_generator import (
    DEFAULT_CREDENTIALS_SCOPE,
    get_file_uri,
    get_option_expressions,
    get_read_expression,
    sftp_file_uri,
    get_sftp_write_expression,
)
import warnings

from wkmigrate.models.ir.pipeline import CopyActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.models.workflows.instructions import PipelineInstruction
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_copy_activity(
    activity: CopyActivity,
    default_files_to_delta_sinks: bool | None,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
    default_catalog_name: str = "wkmigrate",
) -> PreparedActivity:
    """
    Builds tasks and artifacts for a Copy activity.

    Args:
        activity: Activity definition emitted by the translators.
        default_files_to_delta_sinks: Optional override for DLT generation.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.
        default_catalog_name: Default Unity Catalog catalog name for generated artifacts.

    Returns:
        PreparedActivity containing task configuration and artifacts.
    """
    source_definition = merge_dataset_definition(activity.source_dataset, activity.source_properties)
    sink_definition = merge_dataset_definition(activity.sink_dataset, activity.sink_properties)
    column_mapping = [asdict(mapping) for mapping in (activity.column_mapping or [])]
    if not column_mapping:
        raise ValueError("No column mapping provided for copy data task")

    data_source_secrets = collect_data_source_secrets(source_definition, credentials_scope)
    data_sink_secrets = collect_data_source_secrets(sink_definition, credentials_scope)
    secrets_to_collect = data_source_secrets + data_sink_secrets

    source_provider_type = source_definition.get("provider_type")

    if source_provider_type == "sftp":
        return _prepare_sftp_copy(
            activity,
            source_definition,
            sink_definition,
            column_mapping,
            secrets_to_collect,
            credentials_scope,
            default_files_to_delta_sinks,
            default_catalog_name,
        )

    files_to_delta_sinks = sink_definition.get("type") == "delta"
    if default_files_to_delta_sinks is not None:
        files_to_delta_sinks = default_files_to_delta_sinks

    notebook_path, notebook = _create_copy_data_notebook(
        source_definition,
        sink_definition,
        column_mapping,
        files_to_delta_sinks,
        credentials_scope,
    )

    base_task = get_base_task(activity)

    if not files_to_delta_sinks:
        # Standard notebook execution
        task = parse_mapping(
            {
                **base_task,
                "notebook_task": {"notebook_path": notebook_path},
            }
        )
        return PreparedActivity(
            task=task,
            notebooks=[notebook],
            secrets=secrets_to_collect if secrets_to_collect else None,
        )

    # DLT pipeline execution - pipeline_id will be resolved later
    pipeline_name = f"{activity.task_key}_pipeline"
    task = parse_mapping(
        {
            **base_task,
            "pipeline_task": {"pipeline_id": "__PIPELINE_ID__"},
        }
    )
    return PreparedActivity(
        task=task,
        notebooks=[notebook],
        secrets=secrets_to_collect if secrets_to_collect else None,
        pipelines=[
            PipelineInstruction(
                task_ref=task,
                file_path=notebook.file_path,
                name=pipeline_name,
            )
        ],
    )


def _prepare_sftp_copy(
    activity: CopyActivity,
    source_definition: dict,
    sink_definition: dict,
    column_mapping: list[dict],
    secrets_to_collect: list,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
    default_files_to_delta_sinks: bool | None = None,
    default_catalog_name: str = "wkmigrate",
) -> PreparedActivity:
    """
    Builds tasks and artifacts for a Copy activity that reads from an SFTP source.

    SFTP sources use Auto Loader to read files directly from the SFTP server
    via a Unity Catalog connection.  A one-time setup notebook is generated to
    create the UC connection.  For non-Delta sinks, a UC external volume is
    created for the sink bucket/container.

    Args:
        activity: Copy activity definition from the translator layer.
        source_definition: Merged source dataset properties.
        sink_definition: Merged sink dataset properties.
        column_mapping: Column-level mappings from source to sink.
        secrets_to_collect: Secret instructions for the source and sink.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.
        default_files_to_delta_sinks: Optional override for DLT generation.
        default_catalog_name: Default Unity Catalog catalog name for generated artifacts.

    Returns:
        PreparedActivity with notebook tasks and setup notebook artifacts.
    """
    sink_is_delta = sink_definition.get("type") == "delta"

    # Delta sinks must always use the DLT pipeline path because the streaming
    # notebook writes via `.option("path", ...)` which requires a filesystem
    # path -- a three-part table name is invalid there.
    if sink_is_delta:
        files_to_delta_sinks = True
    elif default_files_to_delta_sinks is not None:
        files_to_delta_sinks = default_files_to_delta_sinks
    else:
        files_to_delta_sinks = False

    setup_notebook = _build_sftp_setup_notebook(
        source_definition,
        sink_definition,
        credentials_scope,
        default_catalog_name,
        create_sink_volume=not files_to_delta_sinks,
    )
    setup_task = parse_mapping(
        {
            "task_key": f"{activity.task_key}_sftp_setup",
            "notebook_task": {"notebook_path": setup_notebook.file_path},
        }
    )
    base_task = get_base_task(activity)

    if not files_to_delta_sinks:
        notebook_path, notebook = _create_sftp_streaming_notebook(
            activity.task_key,
            source_definition,
            sink_definition,
            column_mapping,
            credentials_scope=credentials_scope,
            default_catalog_name=default_catalog_name,
        )
        notebooks = [setup_notebook, notebook]
        task = parse_mapping(
            {
                **base_task,
                "notebook_task": {"notebook_path": notebook_path},
            }
        )
        return PreparedActivity(
            task=task,
            notebooks=notebooks,
            secrets=secrets_to_collect if secrets_to_collect else None,
            setup_task=setup_task,
        )

    notebook_path, notebook = _create_copy_data_notebook(
        source_definition,
        sink_definition,
        column_mapping,
        files_to_delta_sinks=True,
        credentials_scope=credentials_scope,
    )
    notebooks = [setup_notebook, notebook]
    pipeline_name = f"{activity.task_key}_pipeline"
    task = parse_mapping(
        {
            **base_task,
            "pipeline_task": {"pipeline_id": "__PIPELINE_ID__"},
        }
    )
    return PreparedActivity(
        task=task,
        notebooks=notebooks,
        secrets=secrets_to_collect if secrets_to_collect else None,
        pipelines=[
            PipelineInstruction(
                task_ref=task,
                file_path=notebook.file_path,
                name=pipeline_name,
            )
        ],
        setup_task=setup_task,
    )


def _create_sftp_streaming_notebook(
    activity_key: str,
    source_definition: dict,
    sink_definition: dict,
    column_mapping: list[dict],
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
    default_catalog_name: str = "wkmigrate",
) -> tuple[str, NotebookArtifact]:
    """
    Generates a notebook that reads from SFTP via Auto Loader and writes with Structured Streaming.

    Auto Loader (``cloudFiles``) is a streaming source only, so this notebook
    uses ``readStream`` / ``writeStream`` with ``trigger(availableNow=True)``
    to process all available files in a single micro-batch and then stop.

    Args:
        activity_key: Task key used for checkpoint path naming.
        source_definition: Merged source dataset definition dictionary.
        sink_definition: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.
        default_catalog_name: Default Unity Catalog catalog name for generated artifacts.

    Returns:
        Tuple of ``(notebook_path, NotebookArtifact)``.
    """
    source_name = source_definition.get("dataset_name", "source")
    sink_name = sink_definition.get("dataset_name", "sink")
    sink_type = sink_definition.get("type", "delta")
    database_name = sink_definition.get("database_name", "default")
    table_name = sink_definition.get("table_name", sink_name)
    checkpoint_path = f"/Volumes/{default_catalog_name}/sftp/_checkpoints/{activity_key}"

    if sink_type == "delta":
        sink_path = f"{default_catalog_name}.{database_name}.{table_name}"
        sink_format = "delta"
    else:
        sink_container = sink_definition.get("container", "")
        sink_folder = sink_definition.get("folder_path", "")
        sink_path = f"/Volumes/{default_catalog_name}/sftp/{sink_container}/{sink_folder}"
        sink_format = sink_type

    script_lines = [
        "# Databricks notebook source",
        "import pyspark.sql.types as T",
        "import pyspark.sql.functions as F",
        "",
        "# Set the source options:",
    ]
    script_lines.extend(get_option_expressions(source_definition, credentials_scope))
    script_lines.append("# Read from the SFTP source via Auto Loader (streaming):")
    script_lines.append(get_read_expression(source_definition))
    script_lines.append("# Map the source columns to the target columns:")
    script_lines.append(_get_mapping(source_definition, sink_definition, column_mapping, True))
    script_lines.append("# Write to the target (streaming):")
    script_lines.append(get_sftp_write_expression(sink_name, sink_format, sink_path, checkpoint_path))

    notebook_content = autopep8.fix_code("\n".join(script_lines))
    notebook_path = f"/wkmigrate/copy_data_notebooks/copy_{source_name}_to_{sink_name}"
    notebook_artifact = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    return notebook_path, notebook_artifact


def _build_sftp_setup_notebook(
    source_definition: dict,
    sink_definition: dict,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
    default_catalog_name: str = "wkmigrate",
    create_sink_volume: bool = False,
) -> NotebookArtifact:
    """
    Generates a one-time setup notebook for SFTP connection configuration.

    The notebook creates a Unity Catalog connection to the SFTP server.
    No volume is created for the SFTP source because volumes cannot be
    backed by an SFTP connection.  For non-Delta sinks, an external volume
    is created for the sink bucket/container.

    Args:
        source_definition: Merged source dataset properties.
        sink_definition: Merged sink dataset properties.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.
        default_catalog_name: Default Unity Catalog catalog name for generated artifacts.
        create_sink_volume: Whether to create a UC external volume for the sink.

    Returns:
        NotebookArtifact containing the setup notebook content.
    """
    source_name = source_definition.get("dataset_name", "source")
    service_name = source_definition.get("service_name", source_name)
    auth_type = source_definition.get("authentication_type", "Basic")

    url = source_definition.get("url", "")
    parsed = urlparse(url)
    host = parsed.hostname or source_definition.get("host", "<SFTP_HOST>")
    port = parsed.port or source_definition.get("port", 22)
    connection_name = f"{service_name}_sftp_connection"
    source_uri = sftp_file_uri(source_definition)
    notebook_path = f"/wkmigrate/sftp_setup/{connection_name}_setup"

    lines = [
        "# Databricks notebook source",
        "# SFTP Connection - One-Time Setup Notebook",
        f"# Source: {source_name} ({host}:{port})",
        f"# Connection: {connection_name}",
        "",
        "# COMMAND ----------",
        "",
        "# Step 1: Create the Unity Catalog connection to the SFTP server",
        f'connection_name = "{connection_name}"',
        f'host = "{host}"',
        f"port = {port}",
    ]

    if auth_type and auth_type.lower() == "basic":
        lines.extend(
            [
                f'user_name = dbutils.secrets.get(scope="{credentials_scope}", key="{service_name}_user_name")',
                f'password = dbutils.secrets.get(scope="{credentials_scope}", key="{service_name}_password")',
                "",
                "# Escape single quotes in credentials to prevent SQL injection",
                "host = host.replace(\"'\", \"''\")",
                "user_name = user_name.replace(\"'\", \"''\")",
                "password = password.replace(\"'\", \"''\")",
                "",
                'spark.sql(f"""',
                "    CREATE CONNECTION IF NOT EXISTS `{connection_name}`",
                "    TYPE sftp",
                "    OPTIONS (",
                "        host '{host}',",
                "        port '{port}',",
                "        user '{user_name}',",
                "        password '{password}'",
                "    )",
                '""")',
            ]
        )
    else:
        warnings.warn(
            f"SFTP authentication type '{auth_type}' is not translatable for connection '{connection_name}'. "
            "The setup notebook contains placeholder parameters that must be filled in manually.",
            stacklevel=2,
        )
        lines.extend(
            [
                f"# WARNING: Authentication type '{auth_type}' is not directly translatable.",
                "# Fill in the connection options below manually.",
                f'user_name = "<{auth_type}_USER>"',
                f'password = "<{auth_type}_PASSWORD>"',
                f'key_fingerprint = "<{auth_type}_KEY_FINGERPRINT>"',
                "",
                "# Escape single quotes to prevent SQL injection",
                "host = host.replace(\"'\", \"''\")",
                "",
                'spark.sql(f"""',
                "    CREATE CONNECTION IF NOT EXISTS `{connection_name}`",
                "    TYPE sftp",
                "    OPTIONS (",
                "        host '{host}',",
                "        port '{port}',",
                "        user '{user_name}',",
                "        password '{password}',",
                "        key_fingerprint '{key_fingerprint}'",
                "    )",
                '""")',
            ]
        )

    lines.extend(
        [
            "",
            "# COMMAND ----------",
            "",
            f"# Step 2: Verify SFTP connection",
            f'spark.sql("""',
            f"    CREATE SCHEMA IF NOT EXISTS {default_catalog_name}.sftp",
            '""")',
            "",
            f'print("SFTP connection \\"{connection_name}\\" configured.")',
            f'print("SFTP source URI: {source_uri}")',
        ]
    )

    if create_sink_volume:
        sink_container = sink_definition.get("container", "")
        sink_provider_type = sink_definition.get("provider_type", "abfs")
        sink_service_name = sink_definition.get("service_name", "sink")
        volume_location = _get_sink_volume_location(sink_definition, sink_provider_type)
        lines.extend(
            [
                "",
                "# COMMAND ----------",
                "",
                f"# Step 3: Create external volume for the sink bucket/container",
                f'spark.sql("""',
                f"    CREATE EXTERNAL VOLUME IF NOT EXISTS {default_catalog_name}.sftp.`{sink_service_name}_{sink_container}`",
                f"    LOCATION '{volume_location}'",
                '""")',
                "",
                f'print("Sink volume {default_catalog_name}.sftp.{sink_service_name}_{sink_container} configured.")',
            ]
        )

    content = autopep8.fix_code("\n".join(lines))
    return NotebookArtifact(file_path=notebook_path, content=content)


def _get_sink_volume_location(sink_definition: dict, provider_type: str) -> str:
    """
    Builds the cloud storage URI for a sink dataset for use as a volume location.

    Args:
        sink_definition: Sink dataset definition dictionary.
        provider_type: Cloud provider type.

    Returns:
        Cloud storage URI for the sink container/bucket.
    """
    container = sink_definition.get("container", "")
    if provider_type == "s3":
        return f"s3://{container}"
    if provider_type == "gcs":
        return f"gs://{container}"
    storage_account_name = sink_definition.get("storage_account_name", "")
    if provider_type == "azure_blob":
        return f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/"
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/"


def _create_copy_data_notebook(
    source_definition: dict,
    sink_definition: dict,
    column_mapping: list[dict],
    files_to_delta_sinks: bool,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> tuple[str, NotebookArtifact]:
    """
    Generates a Python notebook that copies data between datasets.

    Args:
        source_definition: Merged source dataset definition dictionary.
        sink_definition: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.
        files_to_delta_sinks: Whether to generate a DLT materialised-view definition.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        Tuple of ``(notebook_path, NotebookArtifact)``.
    """
    script_lines = [
        "# Databricks notebook source",
        "import pyspark.sql.types as T",
        "import pyspark.sql.functions as F",
        "",
        "# Set the source options:",
    ]
    script_lines.extend(get_option_expressions(source_definition, credentials_scope))
    if not files_to_delta_sinks:
        script_lines.append("# Set the target options:")
        script_lines.extend(get_option_expressions(sink_definition, credentials_scope))
        script_lines.append("# Read from the source:")
        script_lines.append(get_read_expression(source_definition))
        script_lines.append("# Map the source columns to the target columns:")
        script_lines.append(_get_mapping(source_definition, sink_definition, column_mapping, True))
        script_lines.append("# Write to the target:")
        script_lines.append(_get_write_expression(sink_definition))
    else:
        script_lines.append("# Load the data with DLT as a materialized view:")
        script_lines.append(
            _get_dlt_definition(
                source_definition,
                sink_definition,
                column_mapping,
            )
        )
    notebook_content = autopep8.fix_code("\n".join(script_lines))
    source_dataset_name = source_definition.get("dataset_name")
    sink_dataset_name = sink_definition.get("dataset_name")
    notebook_path = f"/wkmigrate/copy_data_notebooks/copy_{source_dataset_name}_to_{sink_dataset_name}"
    notebook_artifact = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    return notebook_path, notebook_artifact


def _get_dlt_definition(source_dataset: dict, sink_dataset: dict, column_mapping: list[dict]) -> str:
    """
    Generates a DLT materialised-view definition for a copy activity.

    Args:
        source_dataset: Merged source dataset definition dictionary.
        sink_dataset: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.

    Returns:
        Python source fragment defining a DLT table.
    """
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    return f"""@dlt.table(
                        name="{sink_name}",
                        comment="Data copied from {source_name}; Previously targeted {sink_name}."
                        tbl_properties={{'delta.createdBy.wkmigrate': 'true'}}
                    )
                    def {sink_name}():
                        {get_read_expression(source_dataset)}
                        {_get_mapping(source_dataset, sink_dataset, column_mapping, True)}
                        return {sink_name}_df
                """


def _get_mapping(
    source_dataset: dict,
    sink_dataset: dict,
    column_mapping: list[dict],
    cast_column_types: bool,
) -> str:
    """
    Generates a ``selectExpr`` statement that maps source columns to sink columns.

    Args:
        source_dataset: Merged source dataset definition dictionary.
        sink_dataset: Merged sink dataset definition dictionary.
        column_mapping: Column-level mappings from source to sink.
        cast_column_types: Whether to wrap each expression in a ``CAST``.

    Returns:
        Python source fragment that maps a source DataFrame to a sink DataFrame.
    """
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    expressions = []
    for mapping in column_mapping:
        source_col = mapping["source_column_name"]
        sink_col = mapping["sink_column_name"]
        sink_type = parse_spark_data_type(mapping["sink_column_type"], sink_dataset["type"])
        if cast_column_types:
            expressions.append(f'"cast({source_col} as {sink_type}) as {sink_col}"')
        else:
            expressions.append(f'"{source_col} as {sink_col}"')
    newline_characters = ", \n\t"
    return f"{sink_name}_df = {source_name}_df.selectExpr(\n\t{newline_characters.join(expressions)}\n)"


def _get_write_expression(sink_definition: dict) -> str:
    """
    Generates a Spark write statement for the sink dataset.

    Args:
        sink_definition: Merged sink dataset definition dictionary.

    Returns:
        Python source fragment that writes a DataFrame to the sink.

    Raises:
        ValueError: If the sink type is not supported for writing.
    """
    sink_name = sink_definition.get("dataset_name")
    sink_type = sink_definition.get("type")
    if sink_type == "avro":
        return rf"""{sink_name}_df.write.format("avro")  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "csv":
        return rf"""{sink_name}_df.write.format("csv")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "delta":
        database_name = sink_definition.get("database_name")
        table_name = sink_definition.get("table_name")
        return rf"""{sink_name}_df.write.format("delta")  \
                        .mode("overwrite")  \
                        .saveAsTable("hive_metastore.{database_name}.{table_name}")
                    """
    if sink_type == "json":
        return rf"""{sink_name}_df.write.format("json")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "orc":
        return rf"""{sink_name}_df.write.format("orc")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type == "parquet":
        return rf"""{sink_name}_df.write.format("parquet")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("{get_file_uri(sink_definition)}")
                    """
    if sink_type in {"sqlserver", "postgresql", "mysql", "oracle"}:
        return rf"""{sink_name}_df.write.format("jdbc")  \
                        .options(**{sink_name}_options)  \
                        .save()
                    """
    raise ValueError(f'Writing data to "{sink_type}" not supported')

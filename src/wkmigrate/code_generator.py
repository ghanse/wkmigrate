"""This module defines shared Spark code-generation helpers used by activity preparers.

Helpers in this module emit Python source fragments that read data, configure options,
and manage credentials.  They are consumed by the Copy, Lookup, and SetVariable activity
preparers to build Databricks notebooks.
"""

from __future__ import annotations

import autopep8  # type: ignore

from wkmigrate.datasets import DATASET_OPTIONS, DATASET_SECRETS


def get_set_variable_notebook_content(variable_name: str, variable_value: str) -> str:
    """
    Generates notebook source for a SetVariable activity.

    The notebook evaluates ``variable_value`` and publishes the result as a
    Databricks task value under ``key=variable_name`` so that downstream tasks
    can retrieve it with ``dbutils.jobs.taskValues.get()``.

    Args:
        variable_name: ADF variable name (used as the task-value key).
        variable_value: Python expression string produced by the expression parser.

    Returns:
        Formatted Python notebook source string.
    """
    script_lines = [
        "# Databricks notebook source",
        "",
        f"# Set variable: {variable_name}",
        f"value = {variable_value}",
        "",
        "# Publish as a Databricks task value:",
        f"dbutils.jobs.taskValues.set(key={variable_name!r}, value=str(value))",
    ]
    return autopep8.fix_code("\n".join(script_lines))


def get_option_expressions(dataset_definition: dict) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for the specified dataset definition.

    Args:
        dataset_definition: Dataset definition dictionary.

    Returns:
        List of Python source lines that creates an options dictionary.
    """
    dataset_type = dataset_definition.get("type")
    if dataset_type in {"avro", "csv", "json", "orc", "parquet"}:
        return get_file_options(dataset_definition, dataset_type)
    if dataset_type == "sqlserver":
        return get_database_options(dataset_definition, dataset_type)
    return []


def get_file_options(dataset_definition: dict, file_type: str) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for a file dataset.

    Args:
        dataset_definition: Dataset definition dictionary.
        file_type: File type (for example ``"csv"`` or ``"parquet"``).

    Returns:
        List of Python source lines that create the options dictionary.
    """
    dataset_name = dataset_definition["dataset_name"]
    service_name = dataset_definition["service_name"]
    config_lines = [
        rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
        for option in DATASET_OPTIONS.get(file_type, [])
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


def get_database_options(dataset_definition: dict, database_type: str) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for interacting with a database.

    Args:
        dataset_definition: Dataset definition dictionary.
        database_type: Database type (for example ``"sqlserver"``).

    Returns:
        List of Python source lines that create the options dictionary.
    """
    dataset_name = dataset_definition["dataset_name"]
    service_name = dataset_definition["service_name"]
    secrets_lines = [
        f"""{dataset_name}_options["{secret}"] = dbutils.secrets.get(
                scope="wkmigrate_credentials_scope", 
                key="{service_name}_{secret}"
            )
            """
        for secret in DATASET_SECRETS[database_type]
    ]
    options_lines = [
        f"""{dataset_name}_options["{option}"] = '{dataset_definition.get(option)}'"""
        for option in DATASET_OPTIONS.get(database_type, [])
        if dataset_definition.get(option)
    ]
    return [f"{dataset_name}_options = {{}}", *secrets_lines, *options_lines]


def get_read_expression(source_definition: dict, source_query: str | None = None) -> str:
    """
    Generates code to read data from a data source into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.
        source_query: Optional SQL query for database sources.

    Returns:
        Python source lines that read data into a DataFrame.

    Raises:
        ValueError: If the dataset type is not supported for reading.
    """
    source_type = source_definition.get("type")

    if source_type in {"avro", "csv", "json", "orc", "parquet"}:
        return get_file_read_expression(source_definition)
    if source_type == "delta":
        return get_delta_read_expression(source_definition)
    if source_type == "sqlserver":
        return get_jdbc_read_expression(source_definition, source_query)

    raise ValueError(f'Reading data from "{source_type}" not supported')


def get_file_read_expression(source_definition: dict) -> str:
    """
    Generates code to read data from a file dataset into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.

    Returns:
        Python source lines that read data into a DataFrame.
    """
    source_name = source_definition["dataset_name"]
    source_type = source_definition["type"]
    container_name = source_definition["container"]
    storage_account_name = source_definition["storage_account_name"]
    folder_path = source_definition["folder_path"]

    return f"""{source_name}_df = ( 
                        spark.read.format("{source_type}")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """


def get_delta_read_expression(source_definition: dict) -> str:
    """
    Generates code to read data from a Delta table into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.

    Returns:
        Python source lines that read data into a DataFrame.
    """
    source_name = source_definition["dataset_name"]
    database_name = source_definition["database_name"]
    table_name = source_definition["table_name"]
    if not table_name:
        raise ValueError("No value for 'table_name' in Delta dataset definition")

    return f'{source_name}_df = spark.read.table("hive_metastore.{database_name}.{table_name}")\n'


def get_jdbc_read_expression(source_definition: dict, source_query: str | None = None) -> str:
    """
    Generates code to read data from a database into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.
        source_query: Optional SQL query string (default:  ``None``).

    Returns:
        Python source lines that read data into a DataFrame.
    """
    source_name = source_definition["dataset_name"]
    schema_name = source_definition["schema_name"]
    table_name = source_definition["table_name"]

    lines = [
        f"{source_name}_df = (",
        f'    spark.read.format("{source_definition.get("type")}")',
        f"        .options(**{source_name}_options)",
    ]

    if source_query:
        escaped_query = source_query.replace('"', '\\"')
        lines.append(f'        .option("query", "{escaped_query}")')
    else:
        lines.append(f'        .option("dbtable", "{schema_name}.{table_name}")')

    lines.append("        .load()")
    lines.append(")")
    return "\n".join(lines) + "\n"

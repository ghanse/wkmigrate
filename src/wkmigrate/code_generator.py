"""This module defines shared Spark code-generation helpers used by activity preparers.

Helpers in this module emit Python source fragments that read data, configure options,
and manage credentials.  They are consumed by the Copy, Lookup, SetVariable, and Web
activity preparers to build Databricks notebooks.
"""

from __future__ import annotations

from typing import Any

import autopep8  # type: ignore

from wkmigrate.datasets import DATASET_OPTIONS, DATASET_PROVIDER_SECRETS, DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.not_translatable import NotTranslatableWarning, not_translatable_context


def get_set_variable_notebook_content(variable_name: str, variable_value: str) -> str:
    """
    Generates code to set a task value parameter. The notebook evaluates ``variable_value`` and sets a Databricks task
    value parameter.

    Args:
        variable_name: ADF variable name (used as the task-value key).
        variable_value: Python expression string produced by the expression parser.

    Returns:
        Python notebook source string.
    """
    script_lines = ["# Databricks notebook source"]
    if "json.loads(" in variable_value:
        script_lines.append("import json")
    script_lines.extend(
        [
            "",
            f"# Set variable: {variable_name}",
            f"value = {variable_value}",
            "",
            "# Publish as a Databricks task value:",
            f"dbutils.jobs.taskValues.set(key={variable_name!r}, value=str(value))",
        ]
    )
    return autopep8.fix_code("\n".join(script_lines))


def get_option_expressions(dataset_definition: dict, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for the specified dataset definition.

    Args:
        dataset_definition: Dataset definition dictionary.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that creates an options dictionary.
    """
    dataset_type = dataset_definition.get("type")
    if dataset_type in {"avro", "csv", "json", "orc", "parquet"}:
        return get_file_options(dataset_definition, dataset_type, credentials_scope=credentials_scope)
    if dataset_type == "sqlserver":
        return get_database_options(dataset_definition, dataset_type, credentials_scope=credentials_scope)
    return []


def get_file_options(
    dataset_definition: dict, file_type: str, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for a file dataset.

    Args:
        dataset_definition: Dataset definition dictionary.
        file_type: File type (for example ``"csv"`` or ``"parquet"``).
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that create the options dictionary.
    """
    dataset_name = dataset_definition["dataset_name"]
    service_name = dataset_definition["service_name"]
    provider_type = dataset_definition.get("provider_type", "abfs")
    config_lines = [
        rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
        for option in DATASET_OPTIONS.get(file_type, [])
        if dataset_definition.get(option)
    ]
    if "records_per_file" in dataset_definition:
        records_per_file = dataset_definition.get("records_per_file")
        config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
    config_lines.extend(_get_file_credential_lines(dataset_definition, service_name, provider_type, credentials_scope))
    return [f"{dataset_name}_options = {{}}", *config_lines]


def get_database_options(
    dataset_definition: dict, database_type: str, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for interacting with a database.

    Args:
        dataset_definition: Dataset definition dictionary.
        database_type: Database type (for example ``"sqlserver"``).
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that create the options dictionary.
    """
    dataset_name = dataset_definition["dataset_name"]
    service_name = dataset_definition["service_name"]
    secrets_lines = [
        f"""{dataset_name}_options["{secret}"] = dbutils.secrets.get(
                scope="{credentials_scope}",
                key="{service_name}_{secret}"
            )
            """
        for secret in DATASET_PROVIDER_SECRETS[database_type]
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


def get_file_uri(definition: dict) -> str:
    """
    Builds the cloud storage URI for a file dataset definition.

    Args:
        definition: Dataset definition dictionary containing provider_type, container,
            folder_path, and (for Azure) storage_account_name.

    Returns:
        Cloud storage URI string (for example ``s3a://bucket/path`` or
        ``abfss://container@account.dfs.core.windows.net/path``).
    """
    provider_type = definition.get("provider_type", "abfs")
    container = definition.get("container", "")
    folder_path = definition.get("folder_path", "")

    if provider_type == "s3":
        return f"s3a://{container}/{folder_path}"
    if provider_type == "gcs":
        return f"gs://{container}/{folder_path}"
    if provider_type == "azure_blob":
        storage_account_name = definition.get("storage_account_name", "")
        return f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/{folder_path}"
    # Default: ABFS (ADLS Gen2)
    storage_account_name = definition.get("storage_account_name", "")
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{folder_path}"


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
    uri = get_file_uri(source_definition)

    return f"""{source_name}_df = (
                        spark.read.format("{source_type}")
                            .options(**{source_name}_options)
                            .load("{uri}")
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


def get_web_activity_notebook_content(
    activity_name: str,
    activity_type: str,
    url: str,
    method: str,
    body: Any,
    headers: dict[str, str] | None,
    authentication: Authentication | None = None,
    disable_cert_validation: bool = False,
    http_request_timeout_seconds: int | None = None,
    turn_off_async: bool = False,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> str:
    """
    Generates notebook source for a Web activity.

    The generated notebook submits an HTTP request using the ``requests`` library
    and publishes the response body and status code as Databricks task values.

    Args:
        activity_name: Logical name of the activity being translated.
        activity_type: Activity type string emitted by ADF.
        url: Target URL for the HTTP request.
        method: HTTP method (for example ``GET``, ``POST``, ``PUT``, ``DELETE``).
        body: Optional request body. Passed as JSON when the body is a dict, or as raw data otherwise.
        headers: Optional HTTP headers dictionary.
        authentication: Parsed authentication configuration, or ``None``.
        disable_cert_validation: When ``True``, TLS certificate verification is skipped.
        http_request_timeout_seconds: Optional HTTP request timeout in seconds.
        turn_off_async: When ``True``, noted in the notebook as a comment for visibility.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        Formatted Python notebook source as a ``str``.
    """
    script_lines = [
        "# Databricks notebook source",
        "import requests",
        "",
        f"url = {url!r}",
        f"method = {method!r}",
        f"headers = {headers!r}",
        f"body = {body!r}",
        "",
        "kwargs = {}",
        "if headers:",
        '    kwargs["headers"] = headers',
        "if body is not None:",
        "    if isinstance(body, dict):",
        '        kwargs["json"] = body',
        "    else:",
        '        kwargs["data"] = body',
    ]

    if disable_cert_validation:
        script_lines.append('kwargs["verify"] = False')

    if http_request_timeout_seconds is not None:
        script_lines.append(f'kwargs["timeout"] = {http_request_timeout_seconds}')

    if authentication:
        script_lines.extend(_get_authentication_lines(activity_name, activity_type, authentication, credentials_scope))

    if turn_off_async:
        script_lines.append("")
        script_lines.append("# Note: ADF turnOffAsync was enabled — this request runs synchronously.")

    script_lines.extend(
        [
            "",
            "response = requests.request(method, url, **kwargs)",
            "",
            "# Publish response as Databricks task values:",
            'dbutils.jobs.taskValues.set(key="status_code", value=str(response.status_code))',
            'dbutils.jobs.taskValues.set(key="response_body", value=response.text)',
            "response.raise_for_status()",
        ]
    )
    return autopep8.fix_code("\n".join(script_lines))


def _get_file_credential_lines(
    dataset_definition: dict, service_name: str, provider_type: str, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates Spark configuration lines for cloud storage credentials.

    Args:
        dataset_definition: Dataset definition dictionary.
        service_name: Linked service name used as a secret key prefix.
        provider_type: Cloud provider identifier (``"abfs"``, ``"s3"``, ``"gcs"``, or ``"azure_blob"``).
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that configure Spark credentials.
    """
    if provider_type == "s3":
        return [
            f"""spark.conf.set(
                "fs.s3a.access.key",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_access_key_id"
                )
            )
            """,
            f"""spark.conf.set(
                "fs.s3a.secret.key",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_secret_access_key"
                )
            )
            """,
        ]
    if provider_type == "gcs":
        return [
            f"""spark.conf.set(
                "fs.gs.hmac.key.access",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_access_key_id"
                )
            )
            """,
            f"""spark.conf.set(
                "fs.gs.hmac.key.secret",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_secret_access_key"
                )
            )
            """,
        ]
    if provider_type == "azure_blob":
        storage_account_name = dataset_definition.get("storage_account_name")
        return [
            f"""spark.conf.set(
                "fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_storage_account_key"
                )
            )
            """,
        ]
    # Default: ABFS (ADLS Gen2)
    return [
        f"""spark.conf.set(
                "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_storage_account_key"
                )
            )
            """,
    ]


def _get_authentication_lines(
    activity_name: str,
    activity_type: str,
    authentication: Authentication,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> list[str]:
    """
    Generates notebook source lines for an authentication configuration.

    Args:
        activity_name: Logical name of the activity being translated.
        activity_type: Activity type string emitted by ADF.
        authentication: Parsed authentication configuration.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines to append to the notebook script.
    """
    with not_translatable_context(activity_name, activity_type):
        match authentication.auth_type.lower():
            case "basic":
                return _get_basic_authentication_lines(authentication, credentials_scope)
            case _:
                raise NotTranslatableWarning(
                    "authentication_type", f"Unsupported authentication type '{authentication.auth_type}'"
                )


def _get_basic_authentication_lines(
    authentication: Authentication, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates notebook source lines for Basic authentication.

    Args:
        authentication: Parsed authentication configuration.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines to append to the notebook script.
    """
    return [
        f'kwargs["auth"] = ({authentication.username!r}, '
        f'dbutils.secrets.get(scope="{credentials_scope}", key="{authentication.password_secret_key}"))'
    ]

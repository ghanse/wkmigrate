"""Tests for the code_generator module.

This module tests the notebook content generation helpers, including
the Web activity notebook builder and read expression generators.
"""

from __future__ import annotations
import pytest

from wkmigrate.code_generator import (
    get_database_options,
    get_delta_read_expression,
    get_file_read_expression,
    get_jdbc_read_expression,
    get_jdbc_url,
    get_read_expression,
    get_web_activity_notebook_content,
)
from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.not_translatable import NotTranslatableWarning


def test_web_activity_notebook_with_auth_and_cert_validation() -> None:
    """Generated notebook includes auth, verify=False, and timeout."""
    content = get_web_activity_notebook_content(
        activity_name="test_web_activity",
        activity_type="WebActivity",
        url="https://api.example.com/secure",
        method="POST",
        body=None,
        headers=None,
        authentication=Authentication(auth_type="Basic", username="testuser", password_secret_key="testuser_password"),
        disable_cert_validation=True,
        http_request_timeout_seconds=330,
        turn_off_async=True,
    )

    assert "verify" in content
    assert "False" in content
    assert "timeout" in content
    assert "330" in content
    assert "auth" in content
    assert "testuser" in content
    assert "synchronously" in content


def test_web_activity_notebook_contains_request_call() -> None:
    """get_web_activity_notebook_content produces valid notebook content."""
    content = get_web_activity_notebook_content(
        activity_name="test_web_activity",
        activity_type="WebActivity",
        url="https://api.example.com/data",
        method="GET",
        headers={"Accept": "application/json"},
        body=None,
    )

    assert "requests.request" in content
    assert "https://api.example.com/data" in content
    assert "GET" in content
    assert "taskValues.set" in content
    assert "status_code" in content
    assert "response_body" in content


def test_web_activity_notebook_with_unsupported_auth_type() -> None:
    """get_web_activity_notebook_content raises NotTranslatableWarning for unsupported auth type."""
    with pytest.raises(NotTranslatableWarning) as exc_info:
        get_web_activity_notebook_content(
            activity_name="test_web_activity_invalid_auth",
            activity_type="WebActivity",
            url="https://api.example.com/data",
            method="GET",
            body=None,
            headers=None,
            authentication=Authentication(auth_type="UNSUPPORTED_AUTH_TYPE"),
        )
    assert "UNSUPPORTED_AUTH_TYPE" in str(exc_info.value)


_FILE_SOURCE = {
    "dataset_name": "sales_data",
    "type": "parquet",
    "container": "raw",
    "storage_account_name": "myaccount",
    "folder_path": "data/sales",
}

_DELTA_SOURCE = {
    "dataset_name": "customers",
    "type": "delta",
    "database_name": "production",
    "table_name": "customers",
}

_SQLSERVER_SOURCE = {
    "dataset_name": "orders",
    "type": "sqlserver",
    "service_name": "sql_svc",
    "host": "sql.example.com",
    "database": "salesdb",
    "port": 1433,
    "schema_name": "dbo",
    "table_name": "Orders",
    "dbtable": "dbo.Orders",
}

_MYSQL_SOURCE = {
    "dataset_name": "products",
    "type": "mysql",
    "service_name": "mysql_svc",
    "host": "mysql.example.com",
    "database": "shopdb",
    "port": 3306,
    "schema_name": None,
    "table_name": "products",
    "dbtable": "products",
}

_POSTGRESQL_SOURCE = {
    "dataset_name": "events",
    "type": "postgresql",
    "service_name": "pg_svc",
    "host": "pg.example.com",
    "database": "analytics",
    "port": 5432,
    "schema_name": "public",
    "table_name": "events",
    "dbtable": "public.events",
}

_ORACLE_SOURCE = {
    "dataset_name": "inventory",
    "type": "oracle",
    "service_name": "oracle_svc",
    "host": "ora.example.com",
    "database": "ORCL",
    "port": 1521,
    "schema_name": "APP",
    "table_name": "INVENTORY",
    "dbtable": "APP.INVENTORY",
}


def test_dispatches_to_file_for_parquet() -> None:
    result = get_read_expression(_FILE_SOURCE)
    assert "spark.read.format" in result
    assert "parquet" in result


def test_dispatches_to_delta() -> None:
    result = get_read_expression(_DELTA_SOURCE)
    assert "spark.read.table" in result


def test_dispatches_to_jdbc_for_sqlserver() -> None:
    result = get_read_expression(_SQLSERVER_SOURCE)
    assert 'format("jdbc")' in result


def test_dispatches_to_jdbc_for_mysql() -> None:
    result = get_read_expression(_MYSQL_SOURCE)
    assert 'format("jdbc")' in result


def test_unsupported_type_raises() -> None:
    with pytest.raises(ValueError, match="not supported"):
        get_read_expression({"type": "cosmosdb", "dataset_name": "x"})


@pytest.mark.parametrize("file_type", ["avro", "csv", "json", "orc", "parquet"])
def test_dispatches_all_file_types(file_type: str) -> None:
    source = {**_FILE_SOURCE, "type": file_type}
    result = get_read_expression(source)
    assert file_type in result


def test_contains_format_and_abfss_path() -> None:
    result = get_file_read_expression(_FILE_SOURCE)
    assert 'format("parquet")' in result
    assert "abfss://raw@myaccount.dfs.core.windows.net/data/sales" in result
    assert "sales_data_df" in result
    assert "sales_data_options" in result


def test_contains_read_table() -> None:
    result = get_delta_read_expression(_DELTA_SOURCE)
    assert "customers_df" in result
    assert 'spark.read.table("hive_metastore.production.customers")' in result


def test_missing_table_name_raises() -> None:
    source = {**_DELTA_SOURCE, "table_name": None}
    with pytest.raises(ValueError, match="table_name"):
        get_delta_read_expression(source)


def test_uses_jdbc_format_not_database_type() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE)
    assert 'format("jdbc")' in result
    assert 'format("sqlserver")' not in result


def test_sqlserver_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE)
    assert '"dbtable", "dbo.Orders"' in result
    assert "orders_df" in result
    assert "orders_options" in result


def test_mysql_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_MYSQL_SOURCE)
    assert '"dbtable", "products"' in result
    assert 'format("jdbc")' in result


def test_postgresql_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_POSTGRESQL_SOURCE)
    assert '"dbtable", "public.events"' in result


def test_oracle_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_ORACLE_SOURCE)
    assert '"dbtable", "APP.INVENTORY"' in result


def test_source_query_uses_query_option() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE, source_query="SELECT * FROM dbo.Orders WHERE active = 1")
    assert '"query"' in result
    assert "SELECT * FROM dbo.Orders WHERE active = 1" in result
    assert '"dbtable"' not in result


def test_source_query_escapes_quotes() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE, source_query='SELECT * FROM "Orders"')
    assert '\\"Orders\\"' in result


def test_fallback_dbtable_when_not_set() -> None:
    source = {**_SQLSERVER_SOURCE, "dbtable": None}
    result = get_jdbc_read_expression(source)
    assert '"dbtable", "dbo.Orders"' in result


def test_output_contains_load() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE)
    assert ".load()" in result


def test_jdbc_url_sqlserver() -> None:
    url = get_jdbc_url(_SQLSERVER_SOURCE)
    assert url == "jdbc:sqlserver://sql.example.com:1433;databaseName=salesdb"


def test_jdbc_url_postgresql() -> None:
    url = get_jdbc_url(_POSTGRESQL_SOURCE)
    assert url == "jdbc:postgresql://pg.example.com:5432/analytics"


def test_jdbc_url_mysql() -> None:
    url = get_jdbc_url(_MYSQL_SOURCE)
    assert url == "jdbc:mysql://mysql.example.com:3306/shopdb"


def test_jdbc_url_oracle() -> None:
    url = get_jdbc_url(_ORACLE_SOURCE)
    assert url == "jdbc:oracle:thin:@ora.example.com:1521:ORCL"


def test_jdbc_url_default_port_sqlserver() -> None:
    source = {**_SQLSERVER_SOURCE, "port": None}
    url = get_jdbc_url(source)
    assert ":1433;" in url


def test_jdbc_url_default_port_postgresql() -> None:
    source = {**_POSTGRESQL_SOURCE, "port": None}
    url = get_jdbc_url(source)
    assert ":5432/" in url


def test_jdbc_url_default_port_mysql() -> None:
    source = {**_MYSQL_SOURCE, "port": None}
    url = get_jdbc_url(source)
    assert ":3306/" in url


def test_jdbc_url_default_port_oracle() -> None:
    source = {**_ORACLE_SOURCE, "port": None}
    url = get_jdbc_url(source)
    assert ":1521:" in url


def test_jdbc_url_custom_port() -> None:
    source = {**_POSTGRESQL_SOURCE, "port": 9999}
    url = get_jdbc_url(source)
    assert ":9999/" in url


def test_jdbc_url_unsupported_type_returns_empty() -> None:
    source = {"type": "cosmosdb", "host": "x", "database": "y"}
    assert get_jdbc_url(source) == ""


# --- get_database_options ---


def test_database_options_contains_url() -> None:
    lines = get_database_options(_SQLSERVER_SOURCE, "sqlserver")
    joined = "\n".join(lines)
    assert '"url"' in joined
    assert "jdbc:sqlserver://sql.example.com:1433;databaseName=salesdb" in joined


def test_database_options_contains_secrets() -> None:
    lines = get_database_options(_SQLSERVER_SOURCE, "sqlserver")
    joined = "\n".join(lines)
    assert '"user_name"' in joined
    assert '"password"' in joined
    assert 'dbutils.secrets.get' in joined


def test_database_options_no_host_or_database_secrets() -> None:
    lines = get_database_options(_SQLSERVER_SOURCE, "sqlserver")
    joined = "\n".join(lines)
    assert 'key="sql_svc_host"' not in joined
    assert 'key="sql_svc_database"' not in joined


def test_database_options_contains_dbtable() -> None:
    lines = get_database_options(_SQLSERVER_SOURCE, "sqlserver")
    joined = "\n".join(lines)
    assert '"dbtable"' in joined
    assert "dbo.Orders" in joined

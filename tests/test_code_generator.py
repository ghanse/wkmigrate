"""Tests for the code_generator module.

This module tests the notebook content generation helpers, including
the Web activity notebook builder and configurable credentials scope.
"""

from __future__ import annotations
import pytest

from wkmigrate.code_generator import (
    DEFAULT_CREDENTIALS_SCOPE,
    get_database_options,
    get_file_options,
    get_option_expressions,
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


# --- Configurable credentials scope tests (fix for #35) ---


def test_default_credentials_scope_constant() -> None:
    """DEFAULT_CREDENTIALS_SCOPE is 'wkmigrate_credentials_scope'."""
    assert DEFAULT_CREDENTIALS_SCOPE == "wkmigrate_credentials_scope"


def test_file_options_uses_default_scope() -> None:
    """File options use the default credentials scope when not overridden."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "csv",
        "storage_account_name": "mystorage",
    }
    lines = get_file_options(dataset_def, "csv")
    joined = "\n".join(lines)
    assert 'scope="wkmigrate_credentials_scope"' in joined


def test_file_options_uses_custom_scope() -> None:
    """File options use the custom credentials scope when provided."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "csv",
        "storage_account_name": "mystorage",
    }
    lines = get_file_options(dataset_def, "csv", credentials_scope="my_custom_scope")
    joined = "\n".join(lines)
    assert 'scope="my_custom_scope"' in joined
    assert "wkmigrate_credentials_scope" not in joined


def test_database_options_uses_custom_scope() -> None:
    """Database options use the custom credentials scope when provided."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "sql_svc",
        "type": "sqlserver",
    }
    lines = get_database_options(dataset_def, "sqlserver", credentials_scope="prod_secrets")
    joined = "\n".join(lines)
    assert 'scope="prod_secrets"' in joined
    assert "wkmigrate_credentials_scope" not in joined


def test_get_option_expressions_passes_scope_to_file() -> None:
    """get_option_expressions threads credentials_scope through to file options."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "parquet",
        "storage_account_name": "mystorage",
    }
    lines = get_option_expressions(dataset_def, credentials_scope="custom_scope")
    joined = "\n".join(lines)
    assert 'scope="custom_scope"' in joined


def test_get_option_expressions_passes_scope_to_database() -> None:
    """get_option_expressions threads credentials_scope through to database options."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "sql_svc",
        "type": "sqlserver",
    }
    lines = get_option_expressions(dataset_def, credentials_scope="db_scope")
    joined = "\n".join(lines)
    assert 'scope="db_scope"' in joined


def test_web_activity_auth_uses_default_scope() -> None:
    """Web activity notebook uses default credentials scope for auth."""
    content = get_web_activity_notebook_content(
        activity_name="test_auth_scope",
        activity_type="WebActivity",
        url="https://api.example.com/data",
        method="POST",
        body=None,
        headers=None,
        authentication=Authentication(auth_type="Basic", username="admin", password_secret_key="admin_pwd"),
    )
    assert 'scope="wkmigrate_credentials_scope"' in content


def test_web_activity_auth_uses_custom_scope() -> None:
    """Web activity notebook uses custom credentials scope for auth when provided."""
    content = get_web_activity_notebook_content(
        activity_name="test_auth_scope",
        activity_type="WebActivity",
        url="https://api.example.com/data",
        method="POST",
        body=None,
        headers=None,
        authentication=Authentication(auth_type="Basic", username="admin", password_secret_key="admin_pwd"),
        credentials_scope="enterprise_vault",
    )
    assert 'scope="enterprise_vault"' in content
    assert "wkmigrate_credentials_scope" not in content

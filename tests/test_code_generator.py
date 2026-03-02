"""Tests for the code_generator module.

This module tests the notebook content generation helpers, including
the Web activity notebook builder.
"""

from __future__ import annotations

from wkmigrate.code_generator import get_web_activity_notebook_content
from wkmigrate.models.ir.pipeline import Authentication


def test_web_activity_notebook_with_auth_and_cert_validation() -> None:
    """Generated notebook includes auth, verify=False, and timeout."""
    content = get_web_activity_notebook_content(
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

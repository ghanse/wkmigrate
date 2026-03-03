"""Tests for the utils module."""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import parse_authentication


def test_parse_authentication_none_returns_none() -> None:
    """None input returns None."""
    assert parse_authentication("test_secret_key", None) is None


def test_parse_authentication_basic_returns_authentication() -> None:
    """Basic auth config is parsed into an Authentication with derived secret key."""
    result = parse_authentication("test_secret_key", {"type": "Basic", "username": "admin"})

    assert isinstance(result, Authentication)
    assert result.auth_type == "Basic"
    assert result.username == "admin"
    assert result.password_secret_key == "test_secret_key"


def test_parse_authentication_missing_type_returns_unsupported() -> None:
    """Auth config without a type key returns UnsupportedValue."""
    result = parse_authentication("test_secret_key", {"username": "orphan"})

    assert isinstance(result, UnsupportedValue)
    assert "Missing value 'type'" in result.message


def test_parse_authentication_unsupported_type_returns_unsupported() -> None:
    """Non-Basic auth types return UnsupportedValue."""
    result = parse_authentication("test_secret_key", {"type": "MSI", "resource": "https://management.azure.com/"})

    assert isinstance(result, UnsupportedValue)
    assert "Unsupported authentication type 'MSI'" in result.message


def test_parse_authentication_missing_username_returns_unsupported() -> None:
    """Basic auth without a username returns UnsupportedValue."""
    result = parse_authentication("test_secret_key", {"type": "Basic"})

    assert isinstance(result, UnsupportedValue)
    assert "Missing value 'username'" in result.message

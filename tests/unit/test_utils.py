"""Tests for the utils module."""

from __future__ import annotations

import pytest

from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.utils import DEFAULT_TIMEOUT_SECONDS, parse_timeout_string, parse_authentication


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


def test_timeout_zero_day_twelve_hours() -> None:
    """0.12:00:00 is 12 hours = 43200 seconds."""
    assert parse_timeout_string("0.12:00:00") == 43200


def test_timeout_one_day() -> None:
    """1.00:00:00 is 1 day = 86400 seconds."""
    assert parse_timeout_string("1.00:00:00") == 86400


def test_timeout_two_days_five_hours() -> None:
    """2.05:00:00 is 2 days 5 hours = 190800 seconds."""
    assert parse_timeout_string("2.05:00:00") == 190800


def test_timeout_twelve_days_complex() -> None:
    """12.05:30:15 is 12 days 5 hours 30 min 15 sec = 1056615 seconds."""
    assert parse_timeout_string("12.05:30:15") == 12 * 86400 + 5 * 3600 + 30 * 60 + 15


def test_timeout_no_day_prefix() -> None:
    """00:30:00 (no day prefix) is 30 minutes = 1800 seconds."""
    assert parse_timeout_string("00:30:00") == 1800


def test_timeout_no_day_prefix_hours() -> None:
    """12:00:00 (no day prefix) is 12 hours = 43200 seconds."""
    assert parse_timeout_string("12:00:00") == 43200


def test_timeout_with_prefix() -> None:
    """Prefix '0.' is prepended before parsing."""
    assert parse_timeout_string("12:00:00", prefix="0.") == 43200


def test_timeout_seven_days() -> None:
    """7.00:00:00 is 7 days = 604800 seconds."""
    assert parse_timeout_string("7.00:00:00") == 604800


def test_timeout_large_day_count() -> None:
    """30.00:00:00 is 30 days — should work beyond calendar day limits."""
    assert parse_timeout_string("30.00:00:00") == 30 * 86400


def test_timeout_999_days() -> None:
    """999.23:59:59 — large but valid ADF timeout."""
    assert parse_timeout_string("999.23:59:59") == 999 * 86400 + 23 * 3600 + 59 * 60 + 59


def test_timeout_invalid_format_warns_and_returns_default() -> None:
    """Invalid format emits NotTranslatableWarning and returns default."""
    with pytest.warns(NotTranslatableWarning, match="Invalid timeout format"):
        result = parse_timeout_string("abc")
    assert result == DEFAULT_TIMEOUT_SECONDS


def test_timeout_empty_string_warns_and_returns_default() -> None:
    """Empty string emits NotTranslatableWarning and returns default."""
    with pytest.warns(NotTranslatableWarning, match="Invalid timeout format"):
        result = parse_timeout_string("")
    assert result == DEFAULT_TIMEOUT_SECONDS


def test_timeout_zero_warns_and_returns_default() -> None:
    """All-zero timeout emits NotTranslatableWarning and returns default."""
    with pytest.warns(NotTranslatableWarning, match="Timeout must be positive"):
        result = parse_timeout_string("0.00:00:00")
    assert result == DEFAULT_TIMEOUT_SECONDS


def test_timeout_only_seconds() -> None:
    """00:00:30 is 30 seconds."""
    assert parse_timeout_string("00:00:30") == 30

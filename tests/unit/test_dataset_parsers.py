"""Tests for the dataset_parsers module.

This module tests the SQL write behavior parser and related edge cases.
"""

from __future__ import annotations

import pytest

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.dataset_parsers import _parse_sql_write_behavior


@pytest.mark.parametrize(
    "write_mode, expected",
    [
        (None, None),
        ("insert", "append"),
    ],
    ids=["none_returns_none", "insert_returns_append"],
)
def test_parse_sql_write_behavior_valid(write_mode: str | None, expected: str | None) -> None:
    """Known write modes are correctly normalised."""
    assert _parse_sql_write_behavior(write_mode) == expected


@pytest.mark.parametrize(
    "write_mode",
    ["upsert", "truncate", "merge", ""],
    ids=["upsert", "truncate", "merge", "empty_string"],
)
def test_parse_sql_write_behavior_unsupported(write_mode: str) -> None:
    """Unrecognised write modes return an UnsupportedValue."""
    result = _parse_sql_write_behavior(write_mode)
    assert isinstance(result, UnsupportedValue)
    assert write_mode in str(result.message)

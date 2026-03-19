"""Tests for the Web activity preparer module."""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import WebActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.web_activity_preparer import prepare_web_activity


def test_prepare_web_activity_returns_prepared_activity() -> None:
    """prepare_web_activity returns a PreparedActivity with a notebook artifact."""
    activity = WebActivity(
        name="CallApi",
        task_key="call_api",
        url="https://api.example.com/data",
        method="POST",
        headers={},
        body='{"key": "value"}',
    )

    result = prepare_web_activity(activity)

    assert result is not None
    assert isinstance(result, PreparedActivity)
    assert len(result.notebooks) == 1
    assert "call_api" in result.notebooks[0].file_path

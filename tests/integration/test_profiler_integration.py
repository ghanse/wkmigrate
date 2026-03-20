"""Integration tests for the profiler against a real Azure Data Factory.

These tests use the ``factory_client`` fixture from conftest to profile a
live factory and verify that the returned ``FactoryProfile`` has sensible
structure and non-negative counts.

Mark: all tests carry the ``integration`` marker so they can be run in
isolation with ``pytest -m integration``.
"""

from __future__ import annotations

import pytest

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.profiler import FactoryProfile, format_profile, profile_factory

pytestmark = pytest.mark.integration


def test_profile_factory(factory_client: FactoryClient) -> None:
    """Profile the test factory and verify counts are non-negative."""
    result = profile_factory(factory_client)
    assert isinstance(result, FactoryProfile)
    assert result.factory_name == factory_client.factory_name
    assert result.pipelines.total >= 0
    assert result.activities.total >= 0
    assert result.datasets.total >= 0
    assert result.linked_services.total >= 0
    assert result.triggers.total >= 0
    assert result.integration_runtimes.total >= 0


def test_profile_supported_lte_total(factory_client: FactoryClient) -> None:
    """Supported counts should never exceed total counts."""
    result = profile_factory(factory_client)
    assert result.pipelines.supported <= result.pipelines.total
    assert result.activities.supported <= result.activities.total
    assert result.datasets.supported <= result.datasets.total
    assert result.linked_services.supported <= result.linked_services.total


def test_profile_unsupported_plus_supported_equals_total(factory_client: FactoryClient) -> None:
    """Supported + unsupported should equal total for every object category."""
    result = profile_factory(factory_client)
    for count in (result.activities, result.datasets, result.linked_services):
        assert count.supported + count.unsupported == count.total


def test_format_profile_returns_string(factory_client: FactoryClient) -> None:
    """format_profile should return a non-empty string for any profile."""
    result = profile_factory(factory_client)
    output = format_profile(result)
    assert isinstance(output, str)
    assert factory_client.factory_name in output

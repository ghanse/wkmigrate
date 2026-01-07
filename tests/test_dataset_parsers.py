from contextlib import nullcontext as does_not_raise
import pytest

from wkmigrate.enums.isolation_level import IsolationLevel
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.datasets.parsers import (
    _parse_query_timeout_seconds,
    _parse_query_isolation_level,
    _parse_dataset_type,
    parse_delimited_file_dataset,
)


class TestDatasetParsers:
    """Unit tests for dataset-level parsing helpers."""

    @pytest.mark.parametrize(
        "properties, expected_result, context",
        [
            (None, 0, does_not_raise()),
            ({}, 0, does_not_raise()),
            ({"query_timeout": "00:05:00"}, 300, does_not_raise()),
            ({"query_timeout": "02:30:00"}, 9000, does_not_raise()),
        ],
    )
    def test_parse_query_timeout_seconds(self, properties, expected_result, context):
        with context:
            assert _parse_query_timeout_seconds(properties) == expected_result

    @pytest.mark.parametrize(
        "properties, expected_result, context",
        [
            (None, "READ_COMMITTED", does_not_raise()),
            ({}, "READ_COMMITTED", does_not_raise()),
            ({"isolation_level": "ReadCommitted"}, IsolationLevel.READ_COMMITTED.name, does_not_raise()),
            ({"isolation_level": "Serializable"}, IsolationLevel.SERIALIZABLE.name, does_not_raise()),
        ],
    )
    def test_parse_query_isolation_level(self, properties, expected_result, context):
        with context:
            assert _parse_query_isolation_level(properties) == expected_result

    def test_parse_dataset_type_unsupported_raises_warning(self):
        with pytest.raises(NotTranslatableWarning, match="Unsupported dataset type: UnknownSource"):
            _parse_dataset_type("UnknownSource")

    def test_parse_delimited_file_dataset_unparsable_linked_service_raises_error(self):

        dataset_def = {
            "name": "test_dataset",
            "properties": {
                "type": "DelimitedText",
                "location": {
                    "type": "AzureBlobFSLocation",
                    "folder_path": "test-container/test-folder",
                    "file_name": "test-file.csv",
                },
                "format": {"type": "DelimitedText"},
                "linked_service_name": {"reference_name": "dummy", "type": "LinkedServiceReference"},
            },
            "linked_service_definition": {},
        }

        with pytest.raises(ValueError, match="Missing linked service definition"):
            parse_delimited_file_dataset(dataset_def)

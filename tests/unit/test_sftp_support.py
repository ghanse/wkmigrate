"""Tests for SFTP Copy activity support.

This module tests the SFTP linked service translator, dataset translation,
code generator helpers, and copy activity preparer for SFTP sources.
"""

from __future__ import annotations


from wkmigrate.code_generator import (
    DEFAULT_CREDENTIALS_SCOPE,
    get_option_expressions,
    get_read_expression,
    get_sftp_file_uri,
    get_sftp_options,
    get_sftp_read_expression,
    get_sftp_write_expression,
)
from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.linked_services import SftpLinkedService
from wkmigrate.models.ir.pipeline import ColumnMapping, CopyActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.dataset_parsers import (
    CLOUD_LOCATION_TYPES,
    DATASET_PROVIDER_SECRETS,
    collect_data_source_secrets,
)
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
from wkmigrate.translators.dataset_translators import translate_dataset, translate_file_dataset
from wkmigrate.translators.linked_service_translators import translate_sftp_spec

# ---------------------------------------------------------------------------
# SFTP linked service translator tests
# ---------------------------------------------------------------------------


def test_translate_sftp_spec_full_configuration() -> None:
    """Test translation of SFTP linked service with full configuration."""
    spec = {
        "name": "sftp-server-1",
        "properties": {
            "host": "sftp.example.com",
            "port": 2222,
            "user_name": "sftpuser",
            "authentication_type": "Basic",
        },
    }
    result = translate_sftp_spec(spec)

    assert isinstance(result, SftpLinkedService)
    assert result.service_name == "sftp-server-1"
    assert result.service_type == "sftp"
    assert result.host == "sftp.example.com"
    assert result.port == 2222
    assert result.user_name == "sftpuser"
    assert result.authentication_type == "Basic"


def test_translate_sftp_spec_minimal_configuration() -> None:
    """Test translation of SFTP linked service with minimal configuration."""
    spec = {
        "name": "sftp-minimal",
        "properties": {
            "host": "192.168.1.100",
        },
    }
    result = translate_sftp_spec(spec)

    assert isinstance(result, SftpLinkedService)
    assert result.service_name == "sftp-minimal"
    assert result.host == "192.168.1.100"
    assert result.port == 22
    assert result.user_name is None
    assert result.authentication_type is None


def test_translate_sftp_spec_missing_host_returns_unsupported() -> None:
    """Test that missing host returns UnsupportedValue."""
    spec = {
        "name": "sftp-no-host",
        "properties": {
            "port": 22,
        },
    }
    result = translate_sftp_spec(spec)

    assert isinstance(result, UnsupportedValue)
    assert "host" in result.message


def test_translate_sftp_spec_null_returns_unsupported() -> None:
    """Test that null input returns UnsupportedValue."""
    result = translate_sftp_spec({})

    assert isinstance(result, UnsupportedValue)
    assert "SFTP" in result.message


def test_translate_sftp_spec_none_returns_unsupported() -> None:
    """Test that None input returns UnsupportedValue."""
    result = translate_sftp_spec(None)

    assert isinstance(result, UnsupportedValue)
    assert "SFTP" in result.message


def test_translate_sftp_spec_generates_uuid_when_no_name() -> None:
    """Test that UUID is generated when no name is provided."""
    spec = {"properties": {"host": "sftp.example.com"}}
    result = translate_sftp_spec(spec)

    assert isinstance(result, SftpLinkedService)
    assert result.service_name is not None
    assert len(result.service_name) > 0


# ---------------------------------------------------------------------------
# SFTP dataset translator tests
# ---------------------------------------------------------------------------


def _build_sftp_dataset(
    dataset_type: str = "DelimitedText",
    dataset_name: str = "sftp_csv_dataset",
    folder_path: str = "uploads",
    file_name: str = "data.csv",
    linked_service: dict | None = None,
) -> dict:
    """Build an SFTP file dataset definition for testing."""
    if linked_service is None:
        linked_service = {
            "name": "sftp-server-1",
            "properties": {
                "host": "sftp.example.com",
                "port": 22,
                "user_name": "sftpuser",
                "authentication_type": "Basic",
            },
        }
    return {
        "name": dataset_name,
        "properties": {
            "type": dataset_type,
            "location": {
                "type": "SftpLocation",
                "folder_path": folder_path,
                "file_name": file_name,
            },
        },
        "linked_service_definition": linked_service,
    }


def test_sftp_location_in_cloud_location_types() -> None:
    """SftpLocation is registered in CLOUD_LOCATION_TYPES."""
    assert "SftpLocation" in CLOUD_LOCATION_TYPES
    assert CLOUD_LOCATION_TYPES["SftpLocation"] == "sftp"


def test_translate_sftp_dataset_csv() -> None:
    """Test SFTP dataset translation with DelimitedText format."""
    dataset = _build_sftp_dataset()
    result = translate_file_dataset("DelimitedText", dataset, "sftp")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "sftp_csv_dataset"
    assert result.dataset_type == "DelimitedText"
    assert result.folder_path == "uploads/data.csv"
    assert result.service_name == "sftp-server-1"
    assert result.provider_type == "sftp"


def test_translate_sftp_dataset_parquet() -> None:
    """Test SFTP dataset translation with Parquet format."""
    dataset = _build_sftp_dataset(
        dataset_type="Parquet",
        dataset_name="sftp_parquet_dataset",
        folder_path="warehouse",
        file_name="events.parquet",
    )
    result = translate_file_dataset("Parquet", dataset, "sftp")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "sftp_parquet_dataset"
    assert result.dataset_type == "Parquet"
    assert result.provider_type == "sftp"


def test_translate_sftp_dataset_no_folder() -> None:
    """Test SFTP dataset with no folder path."""
    dataset = _build_sftp_dataset(folder_path="", file_name="root_file.csv")
    result = translate_file_dataset("DelimitedText", dataset, "sftp")

    assert isinstance(result, FileDataset)
    assert result.folder_path == "root_file.csv"


def test_translate_sftp_dataset_null_returns_unsupported() -> None:
    """Test null SFTP dataset returns UnsupportedValue."""
    result = translate_file_dataset("DelimitedText", {}, "sftp")

    assert isinstance(result, UnsupportedValue)
    assert "sftp" in result.message.lower()


def test_translate_sftp_dataset_missing_linked_service_host() -> None:
    """Test SFTP dataset with missing host in linked service returns UnsupportedValue."""
    dataset = _build_sftp_dataset(
        linked_service={
            "name": "sftp-no-host",
            "properties": {"port": 22},
        },
    )
    result = translate_file_dataset("DelimitedText", dataset, "sftp")

    assert isinstance(result, UnsupportedValue)
    assert "host" in result.message


def test_translate_dataset_dispatches_sftp() -> None:
    """Test that translate_dataset correctly dispatches SftpLocation."""
    dataset = _build_sftp_dataset()
    result = translate_dataset(dataset)

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "sftp_csv_dataset"
    assert result.provider_type == "sftp"


# ---------------------------------------------------------------------------
# SFTP secrets registry tests
# ---------------------------------------------------------------------------


def test_sftp_provider_secrets_registered() -> None:
    """SFTP provider secrets include user_name and password."""
    assert "sftp" in DATASET_PROVIDER_SECRETS
    assert "user_name" in DATASET_PROVIDER_SECRETS["sftp"]
    assert "password" in DATASET_PROVIDER_SECRETS["sftp"]


def test_collect_sftp_secrets() -> None:
    """collect_data_source_secrets returns SFTP credential instructions."""
    definition = {
        "type": "csv",
        "service_name": "sftp-server-1",
        "provider_type": "sftp",
        "user_name": "sftpuser",
        "password": "s3cret",
    }
    secrets = collect_data_source_secrets(definition)

    assert len(secrets) == 2
    keys = {s.key for s in secrets}
    assert "sftp-server-1_user_name" in keys
    assert "sftp-server-1_password" in keys


def test_collect_sftp_secrets_custom_scope() -> None:
    """collect_data_source_secrets respects custom credentials_scope for SFTP."""
    definition = {
        "type": "csv",
        "service_name": "sftp-server-1",
        "provider_type": "sftp",
    }
    secrets = collect_data_source_secrets(definition, credentials_scope="custom_vault")

    for secret in secrets:
        assert secret.scope == "custom_vault"


# ---------------------------------------------------------------------------
# SFTP code generator tests
# ---------------------------------------------------------------------------


def test_get_sftp_file_uri() -> None:
    """get_sftp_file_uri builds a volume path."""
    definition = {
        "service_name": "my_sftp",
        "folder_path": "data/incoming",
    }
    uri = get_sftp_file_uri(definition)

    assert uri == "/Volumes/wkmigrate/sftp/my_sftp/data/incoming"


def test_get_sftp_options() -> None:
    """get_sftp_options generates connection_name and format options."""
    definition = {
        "dataset_name": "sftp_csv",
        "service_name": "my_sftp",
        "header": "true",
        "sep": ",",
    }
    lines = get_sftp_options(definition, "csv")

    joined = "\n".join(lines)
    assert "sftp_csv_options = {}" in joined
    assert "cloudFiles.connectionName" in joined
    assert "my_sftp_sftp_connection" in joined


def test_get_option_expressions_sftp_dispatches() -> None:
    """get_option_expressions dispatches to SFTP options when provider_type is sftp."""
    definition = {
        "dataset_name": "sftp_csv",
        "service_name": "my_sftp",
        "type": "csv",
        "provider_type": "sftp",
    }
    lines = get_option_expressions(definition)

    joined = "\n".join(lines)
    assert "sftp_csv_options = {}" in joined
    assert "cloudFiles.connectionName" in joined


def test_get_sftp_read_expression() -> None:
    """get_sftp_read_expression uses Auto Loader (cloudFiles)."""
    definition = {
        "dataset_name": "sftp_csv",
        "type": "csv",
        "service_name": "my_sftp",
        "folder_path": "data/incoming",
        "provider_type": "sftp",
    }
    expr = get_sftp_read_expression(definition)

    assert "cloudFiles" in expr
    assert "cloudFiles.format" in expr
    assert "csv" in expr
    assert "/Volumes/wkmigrate/sftp/my_sftp/data/incoming" in expr
    assert "sftp_csv_df" in expr


def test_get_sftp_read_expression_uses_readstream() -> None:
    """get_sftp_read_expression uses spark.readStream (not spark.read)."""
    definition = {
        "dataset_name": "sftp_csv",
        "type": "csv",
        "service_name": "my_sftp",
        "folder_path": "data/incoming",
        "provider_type": "sftp",
    }
    expr = get_sftp_read_expression(definition)

    assert "readStream" in expr
    assert "spark.read.format" not in expr


def test_get_sftp_write_expression() -> None:
    """get_sftp_write_expression generates a streaming write with trigger(availableNow=True)."""
    expr = get_sftp_write_expression(
        dataset_name="my_sink",
        sink_table="catalog.schema.target_table",
        checkpoint_path="/Volumes/wkmigrate/sftp/_checkpoints/my_task",
    )

    assert "my_sink_df.writeStream" in expr
    assert "availableNow=True" in expr
    assert "checkpointLocation" in expr
    assert "/Volumes/wkmigrate/sftp/_checkpoints/my_task" in expr
    assert 'toTable("catalog.schema.target_table")' in expr


def test_get_read_expression_sftp_dispatches() -> None:
    """get_read_expression dispatches to SFTP read when provider_type is sftp."""
    definition = {
        "dataset_name": "sftp_csv",
        "type": "csv",
        "service_name": "my_sftp",
        "folder_path": "uploads",
        "provider_type": "sftp",
    }
    expr = get_read_expression(definition)

    assert "cloudFiles" in expr
    assert "sftp_csv_df" in expr
    assert "readStream" in expr


# ---------------------------------------------------------------------------
# SFTP copy activity preparer tests
# ---------------------------------------------------------------------------


_SFTP_SOURCE = {
    "type": "csv",
    "dataset_name": "sftp_csv_source",
    "service_name": "sftp-server-1",
    "url": "sftp://sftp.example.com:22",
    "folder_path": "uploads/data.csv",
    "provider_type": "sftp",
    "header": "true",
    "sep": ",",
}

_CSV_SINK = {
    "type": "csv",
    "dataset_name": "my_sink_csv",
    "service_name": "my_blob",
    "storage_account_name": "mystorageacct",
    "container": "curated",
    "folder_path": "data/output",
}


def _make_sftp_copy_activity(name: str = "CopySftpToCsv") -> CopyActivity:
    return CopyActivity(
        name=name,
        task_key=name.lower(),
        source_dataset=_SFTP_SOURCE,
        sink_dataset=_CSV_SINK,
        source_properties={"type": "csv"},
        sink_properties={"type": "csv"},
        column_mapping=[
            ColumnMapping(
                source_column_name="col_a",
                sink_column_name="col_a",
                sink_column_type="string",
            ),
        ],
    )


def test_sftp_copy_preparer_produces_setup_notebook() -> None:
    """SFTP copy produces both a setup notebook and a copy notebook."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    assert result.notebooks is not None
    assert len(result.notebooks) == 2

    setup_notebook = result.notebooks[0]
    assert "sftp_setup" in setup_notebook.file_path
    assert "CREATE CONNECTION" in setup_notebook.content
    assert "sftp.example.com" in setup_notebook.content

    copy_notebook = result.notebooks[1]
    assert "copy_data_notebooks" in copy_notebook.file_path


def test_sftp_copy_preparer_setup_notebook_has_volume_creation() -> None:
    """SFTP setup notebook creates an external volume."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    setup_content = result.notebooks[0].content
    assert "CREATE EXTERNAL VOLUME" in setup_content
    assert "sftp-server-1" in setup_content


def test_sftp_copy_preparer_has_notebook_task() -> None:
    """SFTP copy creates a notebook_task (not pipeline_task)."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    assert "notebook_task" in result.task
    assert result.pipelines is None


def test_sftp_copy_preparer_collects_secrets() -> None:
    """SFTP copy collects SFTP credential secrets."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    assert result.secrets is not None
    keys = {s.key for s in result.secrets}
    assert "sftp-server-1_user_name" in keys
    assert "sftp-server-1_password" in keys


def test_sftp_copy_preparer_copy_notebook_uses_autoloader() -> None:
    """SFTP copy notebook uses Auto Loader (cloudFiles) for reading."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    copy_notebook = result.notebooks[1]
    assert "cloudFiles" in copy_notebook.content


def test_sftp_copy_preparer_custom_credentials_scope() -> None:
    """SFTP copy preparer respects custom credentials_scope."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(
        activity,
        default_files_to_delta_sinks=None,
        credentials_scope="sftp_vault",
    )

    setup_content = result.notebooks[0].content
    assert 'scope="sftp_vault"' in setup_content
    assert DEFAULT_CREDENTIALS_SCOPE not in setup_content


def test_sftp_copy_preparer_uses_streaming() -> None:
    """SFTP copy notebook uses readStream + writeStream with trigger(availableNow=True)."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    copy_notebook = result.notebooks[1]
    assert "readStream" in copy_notebook.content
    assert "writeStream" in copy_notebook.content
    assert "availableNow=True" in copy_notebook.content
    assert "checkpointLocation" in copy_notebook.content


def test_sftp_copy_preparer_respects_files_to_delta_sinks() -> None:
    """SFTP copy preparer threads default_files_to_delta_sinks through."""
    activity = _make_sftp_copy_activity()

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=True)

    # When files_to_delta_sinks=True, should produce a pipeline task
    assert "pipeline_task" in result.task
    assert result.pipelines is not None


def test_sftp_copy_preparer_setup_notebook_parses_host_from_url() -> None:
    """Setup notebook parses host/port from the url field, not raw host/port keys."""
    # Build a CopyActivity using a FileDataset-like source with url but no host/port keys
    source = {
        "type": "csv",
        "dataset_name": "sftp_url_test",
        "service_name": "sftp-server-url",
        "url": "sftp://myhost.example.com:2222",
        "folder_path": "data/files.csv",
        "provider_type": "sftp",
    }
    activity = CopyActivity(
        name="CopySftpUrlTest",
        task_key="copysftp_url_test",
        source_dataset=source,
        sink_dataset=_CSV_SINK,
        source_properties={"type": "csv"},
        sink_properties={"type": "csv"},
        column_mapping=[
            ColumnMapping(
                source_column_name="col_a",
                sink_column_name="col_a",
                sink_column_type="string",
            ),
        ],
    )

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    setup_content = result.notebooks[0].content
    assert "myhost.example.com" in setup_content
    assert "2222" in setup_content


def test_sftp_copy_end_to_end_from_file_dataset_ir() -> None:
    """Integration: FileDataset IR -> prepare_copy_activity -> setup notebook has real host/port."""
    from wkmigrate.parsers.dataset_parsers import merge_dataset_definition

    # Simulate a FileDataset produced by _translate_sftp_file_dataset
    sftp_dataset = FileDataset(
        dataset_name="sftp_e2e_csv",
        dataset_type="DelimitedText",
        container=None,
        folder_path="incoming/data.csv",
        storage_account_name=None,
        service_name="prod-sftp",
        url="sftp://prod.sftp.example.com:2222",
        format_options={"header": "true", "sep": ","},
        provider_type="sftp",
    )
    source_properties = {"type": "csv"}
    source_definition = merge_dataset_definition(sftp_dataset, source_properties)

    # Verify url is in the merged definition and host/port are NOT
    assert "url" in source_definition
    assert source_definition["url"] == "sftp://prod.sftp.example.com:2222"
    assert "host" not in source_definition
    assert "port" not in source_definition

    # Build a CopyActivity with the dataset
    sink = {
        "type": "csv",
        "dataset_name": "sink_e2e",
        "service_name": "blob_store",
        "storage_account_name": "myacct",
        "container": "out",
        "folder_path": "target",
    }
    activity = CopyActivity(
        name="E2eSftpCopy",
        task_key="e2e_sftp_copy",
        source_dataset=sftp_dataset,
        sink_dataset=sink,
        source_properties=source_properties,
        sink_properties={"type": "csv"},
        column_mapping=[
            ColumnMapping(
                source_column_name="id",
                sink_column_name="id",
                sink_column_type="int",
            ),
        ],
    )

    result = prepare_copy_activity(activity, default_files_to_delta_sinks=None)

    # Setup notebook must contain the real host and port parsed from url
    setup_content = result.notebooks[0].content
    assert "prod.sftp.example.com" in setup_content
    assert "2222" in setup_content
    assert "CREATE CONNECTION" in setup_content

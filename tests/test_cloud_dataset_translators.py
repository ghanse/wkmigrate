"""Tests for cloud file dataset translators (S3, GCS, Azure Blob).

This module tests dataset translation for Amazon S3, Google Cloud Storage,
and Azure Blob Storage datasets.  Cloud file datasets use standard ADF file
types (e.g. ``DelimitedText``, ``Parquet``) with cloud-specific location
types that determine the storage provider.
"""

from __future__ import annotations


from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators import (
    translate_cloud_file_dataset,
    translate_dataset,
)


def _build_cloud_dataset(
    dataset_type: str,
    location_type: str,
    dataset_name: str,
    bucket_name: str,
    folder_path: str,
    file_name: str,
    linked_service: dict,
    container_key: str = "bucket_name",
) -> dict:
    """Build a cloud file dataset definition for testing."""
    location = {
        "type": location_type,
        container_key: bucket_name,
        "folder_path": folder_path,
        "file_name": file_name,
    }
    properties: dict = {
        "type": dataset_type,
        "location": location,
    }
    return {
        "name": dataset_name,
        "properties": properties,
        "linked_service_definition": linked_service,
    }


# --- Amazon S3 dataset translation tests ---


class TestS3FileDataset:
    """Tests for S3 file dataset translation."""

    def test_translate_s3_dataset_delimited_text(self) -> None:
        """Test S3 dataset translation with DelimitedText format."""
        dataset = _build_cloud_dataset(
            dataset_type="DelimitedText",
            location_type="AmazonS3Location",
            dataset_name="s3_csv_dataset",
            bucket_name="my-data-bucket",
            folder_path="csv",
            file_name="csv_file.csv",
            linked_service={
                "name": "AmazonS31",
                "properties": {
                    "access_key_id": "adsfe",
                    "service_url": "https://s3.amazonaws.com/afeaef",
                },
            },
        )
        result = translate_cloud_file_dataset("DelimitedText", dataset, "s3")

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "s3_csv_dataset"
        assert result.dataset_type == "DelimitedText"
        assert result.container == "my-data-bucket"
        assert result.folder_path == "csv/csv_file.csv"
        assert result.service_name == "AmazonS31"
        assert result.url == "https://s3.amazonaws.com/afeaef"
        assert result.provider_type == "s3"

    def test_translate_s3_dataset_parquet(self) -> None:
        """Test S3 dataset translation with Parquet format."""
        dataset = _build_cloud_dataset(
            dataset_type="Parquet",
            location_type="AmazonS3Location",
            dataset_name="s3_parquet_dataset",
            bucket_name="my-data-bucket",
            folder_path="raw/data",
            file_name="events.parquet",
            linked_service={
                "name": "s3-linked-service",
                "properties": {
                    "access_key_id": "MY_ACCESS_KEY_ID",
                    "service_url": "https://s3.amazonaws.com",
                },
            },
        )
        result = translate_cloud_file_dataset("Parquet", dataset, "s3")

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "s3_parquet_dataset"
        assert result.dataset_type == "Parquet"
        assert result.container == "my-data-bucket"
        assert result.folder_path == "raw/data/events.parquet"
        assert result.service_name == "s3-linked-service"
        assert result.url == "https://s3.amazonaws.com"
        assert result.provider_type == "s3"

    def test_translate_s3_dataset_no_folder(self) -> None:
        """Test S3 dataset with no folder path."""
        dataset = _build_cloud_dataset(
            dataset_type="Parquet",
            location_type="AmazonS3Location",
            dataset_name="s3_root_file",
            bucket_name="my-bucket",
            folder_path="",
            file_name="data.parquet",
            linked_service={
                "name": "s3-service",
                "properties": {},
            },
        )
        result = translate_cloud_file_dataset("Parquet", dataset, "s3")

        assert isinstance(result, FileDataset)
        assert result.folder_path == "data.parquet"

    def test_translate_s3_dataset_missing_location(self) -> None:
        """Test S3 dataset with missing location returns UnsupportedValue."""
        dataset = {
            "name": "s3_no_location",
            "properties": {"type": "DelimitedText"},
            "linked_service_definition": {"name": "svc", "properties": {}},
        }
        result = translate_cloud_file_dataset("DelimitedText", dataset, "s3")

        assert isinstance(result, UnsupportedValue)
        assert "location" in result.message

    def test_translate_s3_dataset_null_returns_unsupported(self) -> None:
        """Test null S3 dataset returns UnsupportedValue."""
        result = translate_cloud_file_dataset("DelimitedText", {}, "s3")

        assert isinstance(result, UnsupportedValue)
        assert "s3" in result.message.lower()

    def test_translate_dataset_dispatches_s3(self) -> None:
        """Test that translate_dataset correctly dispatches S3 location."""
        dataset = _build_cloud_dataset(
            dataset_type="DelimitedText",
            location_type="AmazonS3Location",
            dataset_name="s3_dispatch",
            bucket_name="bucket",
            folder_path="path",
            file_name="file.csv",
            linked_service={
                "name": "s3-svc",
                "properties": {},
            },
        )
        result = translate_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "s3_dispatch"
        assert result.provider_type == "s3"


# --- Google Cloud Storage dataset translation tests ---


class TestGcsFileDataset:
    """Tests for GCS file dataset translation."""

    def test_translate_gcs_dataset_delimited_text(self) -> None:
        """Test GCS dataset translation with DelimitedText format."""
        dataset = _build_cloud_dataset(
            dataset_type="DelimitedText",
            location_type="GoogleCloudStorageLocation",
            dataset_name="gcs_csv_dataset",
            bucket_name="gcs-data-bucket",
            folder_path="csv_files",
            file_name="csv_file.csv",
            linked_service={
                "name": "GoogleCloudStorage1",
                "properties": {
                    "access_key_id": "a;ldkfjea",
                    "service_url": "https://storage.googleapis.com/alkfjea",
                },
            },
        )
        result = translate_cloud_file_dataset("DelimitedText", dataset, "gcs")

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "gcs_csv_dataset"
        assert result.dataset_type == "DelimitedText"
        assert result.container == "gcs-data-bucket"
        assert result.folder_path == "csv_files/csv_file.csv"
        assert result.service_name == "GoogleCloudStorage1"
        assert result.url == "https://storage.googleapis.com/alkfjea"
        assert result.provider_type == "gcs"

    def test_translate_gcs_dataset_parquet(self) -> None:
        """Test GCS dataset translation with Parquet format."""
        dataset = _build_cloud_dataset(
            dataset_type="Parquet",
            location_type="GoogleCloudStorageLocation",
            dataset_name="gcs_parquet_dataset",
            bucket_name="gcs-data-bucket",
            folder_path="analytics/raw",
            file_name="events.parquet",
            linked_service={
                "name": "gcs-linked-service",
                "properties": {
                    "access_key_id": "my-key",
                    "service_url": "https://storage.googleapis.com",
                },
            },
        )
        result = translate_cloud_file_dataset("Parquet", dataset, "gcs")

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "gcs_parquet_dataset"
        assert result.dataset_type == "Parquet"
        assert result.provider_type == "gcs"

    def test_translate_gcs_dataset_missing_file_name(self) -> None:
        """Test GCS dataset with missing file_name returns UnsupportedValue."""
        dataset = {
            "name": "gcs_no_file",
            "properties": {
                "type": "DelimitedText",
                "location": {
                    "type": "GoogleCloudStorageLocation",
                    "bucket_name": "my-bucket",
                    "folder_path": "data",
                },
            },
            "linked_service_definition": {"name": "svc", "properties": {}},
        }
        result = translate_cloud_file_dataset("DelimitedText", dataset, "gcs")

        assert isinstance(result, UnsupportedValue)
        assert "file_name" in result.message

    def test_translate_gcs_dataset_null_returns_unsupported(self) -> None:
        """Test null GCS dataset returns UnsupportedValue."""
        result = translate_cloud_file_dataset("DelimitedText", {}, "gcs")

        assert isinstance(result, UnsupportedValue)
        assert "gcs" in result.message.lower()

    def test_translate_dataset_dispatches_gcs(self) -> None:
        """Test that translate_dataset correctly dispatches GCS location."""
        dataset = _build_cloud_dataset(
            dataset_type="DelimitedText",
            location_type="GoogleCloudStorageLocation",
            dataset_name="gcs_dispatch",
            bucket_name="bucket",
            folder_path="path",
            file_name="file.csv",
            linked_service={
                "name": "gcs-svc",
                "properties": {},
            },
        )
        result = translate_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "gcs_dispatch"
        assert result.provider_type == "gcs"


# --- Azure Blob Storage dataset translation tests ---


class TestAzureBlobFileDataset:
    """Tests for Azure Blob Storage file dataset translation."""

    def test_translate_azure_blob_dataset_parquet(self) -> None:
        """Test Azure Blob dataset translation with Parquet format."""
        dataset = _build_cloud_dataset(
            dataset_type="Parquet",
            location_type="AzureBlobStorageLocation",
            dataset_name="blob_parquet_dataset",
            bucket_name="blob-container",
            folder_path="warehouse/bronze",
            file_name="transactions.parquet",
            linked_service={
                "name": "blob-linked-service",
                "properties": {
                    "connection_string": (
                        "DefaultEndpointsProtocol=https;AccountName=myblob;" "EndpointSuffix=core.windows.net;"
                    ),
                },
            },
            container_key="container",
        )
        result = translate_cloud_file_dataset("Parquet", dataset, "azure_blob")

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "blob_parquet_dataset"
        assert result.dataset_type == "Parquet"
        assert result.container == "blob-container"
        assert result.folder_path == "warehouse/bronze/transactions.parquet"
        assert result.service_name == "blob-linked-service"
        assert result.storage_account_name == "myblob"
        assert result.provider_type == "azure_blob"

    def test_translate_azure_blob_dataset_csv(self) -> None:
        """Test Azure Blob dataset translation with DelimitedText format."""
        dataset = _build_cloud_dataset(
            dataset_type="DelimitedText",
            location_type="AzureBlobStorageLocation",
            dataset_name="blob_csv_dataset",
            bucket_name="csv-container",
            folder_path="raw",
            file_name="events.csv",
            linked_service={
                "name": "blob-csv-service",
                "properties": {
                    "service_endpoint": "https://myaccount.blob.core.windows.net/",
                },
            },
        )
        result = translate_cloud_file_dataset("DelimitedText", dataset, "azure_blob")

        assert isinstance(result, FileDataset)
        assert result.dataset_type == "DelimitedText"
        assert result.provider_type == "azure_blob"

    def test_translate_azure_blob_dataset_missing_linked_service_connection(self) -> None:
        """Test Azure Blob dataset with linked service missing connection info returns UnsupportedValue."""
        dataset = _build_cloud_dataset(
            dataset_type="Parquet",
            location_type="AzureBlobStorageLocation",
            dataset_name="blob_no_conn",
            bucket_name="container",
            folder_path="data",
            file_name="file.parquet",
            linked_service={
                "name": "blob-no-conn-service",
                "properties": {},
            },
        )
        result = translate_cloud_file_dataset("Parquet", dataset, "azure_blob")

        assert isinstance(result, UnsupportedValue)
        assert "connection_string" in result.message.lower() or "service_endpoint" in result.message.lower()

    def test_translate_azure_blob_dataset_null_returns_unsupported(self) -> None:
        """Test null Azure Blob dataset returns UnsupportedValue."""
        result = translate_cloud_file_dataset("Parquet", {}, "azure_blob")

        assert isinstance(result, UnsupportedValue)
        assert "azure_blob" in result.message.lower()

    def test_translate_dataset_dispatches_azure_blob(self) -> None:
        """Test that translate_dataset correctly dispatches Azure Blob location."""
        dataset = _build_cloud_dataset(
            dataset_type="Parquet",
            location_type="AzureBlobStorageLocation",
            dataset_name="blob_dispatch",
            bucket_name="container",
            folder_path="path",
            file_name="file.parquet",
            linked_service={
                "name": "blob-svc",
                "properties": {
                    "connection_string": (
                        "DefaultEndpointsProtocol=https;AccountName=account;" "EndpointSuffix=core.windows.net;"
                    ),
                },
            },
        )
        result = translate_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "blob_dispatch"
        assert result.provider_type == "azure_blob"

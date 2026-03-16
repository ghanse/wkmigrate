"""Tests for cloud file dataset translators (S3, GCS, Azure Blob Storage).

This module tests dataset translation for Amazon S3, Google Cloud Storage,
and Azure Blob Storage datasets. Tests cover successful translation,
missing properties, and null inputs.
"""

from __future__ import annotations

from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators import (
    translate_azure_blob_file_dataset,
    translate_dataset,
    translate_gcs_file_dataset,
    translate_s3_file_dataset,
)


def _build_cloud_dataset(
    dataset_type: str,
    dataset_name: str,
    bucket_name: str,
    folder_path: str,
    file_name: str,
    linked_service: dict,
    file_format: dict | None = None,
) -> dict:
    """Build a cloud file dataset definition for testing.

    Args:
        dataset_type: ADF dataset type string (e.g. AmazonS3Dataset).
        dataset_name: Logical dataset name.
        bucket_name: Cloud storage bucket or container name.
        folder_path: Directory path within the bucket.
        file_name: File name within the folder.
        linked_service: Linked service definition dictionary.
        file_format: Optional file format descriptor.

    Returns:
        Dataset definition dictionary suitable for translator input.
    """
    location = {
        "bucket_name": bucket_name,
        "folder_path": folder_path,
        "file_name": file_name,
    }
    properties: dict = {
        "type": dataset_type,
        "location": location,
    }
    if file_format:
        properties["format"] = file_format
    return {
        "name": dataset_name,
        "properties": properties,
        "linked_service_definition": linked_service,
    }


# --- Amazon S3 dataset translation tests ---


class TestS3FileDataset:
    """Tests for S3 file dataset translation."""

    def test_translate_s3_dataset_parquet(self) -> None:
        """Test S3 dataset translation with Parquet format."""
        dataset = _build_cloud_dataset(
            dataset_type="AmazonS3Dataset",
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
            file_format={"type": "ParquetFormat"},
        )
        result = translate_s3_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "s3_parquet_dataset"
        assert result.dataset_type == "Parquet"
        assert result.container == "my-data-bucket"
        assert result.folder_path == "raw/data/events.parquet"
        assert result.service_name == "s3-linked-service"
        assert result.url == "https://s3.amazonaws.com"
        assert result.provider_type == "s3"

    def test_translate_s3_dataset_csv(self) -> None:
        """Test S3 dataset translation with CSV (TextFormat) format."""
        dataset = _build_cloud_dataset(
            dataset_type="AmazonS3Dataset",
            dataset_name="s3_csv_dataset",
            bucket_name="csv-bucket",
            folder_path="exports",
            file_name="report.csv",
            linked_service={
                "name": "s3-csv-service",
                "properties": {},
            },
            file_format={"type": "TextFormat"},
        )
        result = translate_s3_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_type == "DelimitedText"

    def test_translate_s3_dataset_no_folder(self) -> None:
        """Test S3 dataset with no folder path."""
        dataset = _build_cloud_dataset(
            dataset_type="AmazonS3Dataset",
            dataset_name="s3_root_file",
            bucket_name="my-bucket",
            folder_path="",
            file_name="data.parquet",
            linked_service={
                "name": "s3-service",
                "properties": {},
            },
        )
        result = translate_s3_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.folder_path == "data.parquet"

    def test_translate_s3_dataset_missing_location(self) -> None:
        """Test S3 dataset with missing location returns UnsupportedValue."""
        dataset = {
            "name": "s3_no_location",
            "properties": {"type": "AmazonS3Dataset"},
            "linked_service_definition": {"name": "svc", "properties": {}},
        }
        result = translate_s3_file_dataset(dataset)

        assert isinstance(result, UnsupportedValue)
        assert "location" in result.message

    def test_translate_s3_dataset_null_returns_unsupported(self) -> None:
        """Test null S3 dataset returns UnsupportedValue."""
        result = translate_s3_file_dataset({})

        assert isinstance(result, UnsupportedValue)
        assert "Missing S3 dataset definition" in result.message

    def test_translate_dataset_dispatches_s3(self) -> None:
        """Test that translate_dataset correctly dispatches AmazonS3Dataset."""
        dataset = _build_cloud_dataset(
            dataset_type="AmazonS3Dataset",
            dataset_name="s3_dispatch",
            bucket_name="bucket",
            folder_path="path",
            file_name="file.parquet",
            linked_service={
                "name": "s3-svc",
                "properties": {},
            },
        )
        result = translate_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "s3_dispatch"


# --- Google Cloud Storage dataset translation tests ---


class TestGcsFileDataset:
    """Tests for GCS file dataset translation."""

    def test_translate_gcs_dataset_parquet(self) -> None:
        """Test GCS dataset translation with Parquet format."""
        dataset = _build_cloud_dataset(
            dataset_type="GoogleCloudStorageDataset",
            dataset_name="gcs_parquet_dataset",
            bucket_name="gcs-data-bucket",
            folder_path="analytics/raw",
            file_name="events.parquet",
            linked_service={
                "name": "gcs-linked-service",
                "properties": {
                    "project_id": "my-gcp-project",
                    "service_url": "https://storage.googleapis.com",
                },
            },
            file_format={"type": "ParquetFormat"},
        )
        result = translate_gcs_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "gcs_parquet_dataset"
        assert result.dataset_type == "Parquet"
        assert result.container == "gcs-data-bucket"
        assert result.folder_path == "analytics/raw/events.parquet"
        assert result.service_name == "gcs-linked-service"
        assert result.url == "https://storage.googleapis.com"
        assert result.provider_type == "gcs"

    def test_translate_gcs_dataset_json(self) -> None:
        """Test GCS dataset translation with JSON format."""
        dataset = _build_cloud_dataset(
            dataset_type="GoogleCloudStorageDataset",
            dataset_name="gcs_json_dataset",
            bucket_name="json-bucket",
            folder_path="logs",
            file_name="events.json",
            linked_service={
                "name": "gcs-json-service",
                "properties": {},
            },
            file_format={"type": "JsonFormat"},
        )
        result = translate_gcs_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_type == "Json"

    def test_translate_gcs_dataset_missing_file_name(self) -> None:
        """Test GCS dataset with missing file_name returns UnsupportedValue."""
        dataset = {
            "name": "gcs_no_file",
            "properties": {
                "type": "GoogleCloudStorageDataset",
                "location": {"bucket_name": "my-bucket", "folder_path": "data"},
            },
            "linked_service_definition": {"name": "svc", "properties": {}},
        }
        result = translate_gcs_file_dataset(dataset)

        assert isinstance(result, UnsupportedValue)
        assert "file_name" in result.message

    def test_translate_gcs_dataset_null_returns_unsupported(self) -> None:
        """Test null GCS dataset returns UnsupportedValue."""
        result = translate_gcs_file_dataset({})

        assert isinstance(result, UnsupportedValue)
        assert "Missing GCS dataset definition" in result.message

    def test_translate_dataset_dispatches_gcs(self) -> None:
        """Test that translate_dataset correctly dispatches GoogleCloudStorageDataset."""
        dataset = _build_cloud_dataset(
            dataset_type="GoogleCloudStorageDataset",
            dataset_name="gcs_dispatch",
            bucket_name="bucket",
            folder_path="path",
            file_name="file.parquet",
            linked_service={
                "name": "gcs-svc",
                "properties": {},
            },
        )
        result = translate_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "gcs_dispatch"


# --- Azure Blob Storage dataset translation tests ---


class TestAzureBlobFileDataset:
    """Tests for Azure Blob Storage file dataset translation."""

    def test_translate_azure_blob_dataset_parquet(self) -> None:
        """Test Azure Blob dataset translation with Parquet format."""
        dataset = _build_cloud_dataset(
            dataset_type="AzureBlobStorageDataset",
            dataset_name="blob_parquet_dataset",
            bucket_name="blob-container",
            folder_path="warehouse/bronze",
            file_name="transactions.parquet",
            linked_service={
                "name": "blob-linked-service",
                "properties": {
                    "url": "https://myblob.blob.core.windows.net/",
                    "storage_account_name": "myblob",
                },
            },
            file_format={"type": "ParquetFormat"},
        )
        # Azure Blob uses container key
        dataset["properties"]["location"]["container"] = dataset["properties"]["location"].pop("bucket_name")
        result = translate_azure_blob_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "blob_parquet_dataset"
        assert result.dataset_type == "Parquet"
        assert result.container == "blob-container"
        assert result.folder_path == "warehouse/bronze/transactions.parquet"
        assert result.service_name == "blob-linked-service"
        assert result.storage_account_name == "myblob"
        assert result.url == "https://myblob.blob.core.windows.net/"
        assert result.provider_type == "azure_blob"

    def test_translate_azure_blob_dataset_avro(self) -> None:
        """Test Azure Blob dataset translation with Avro format."""
        dataset = _build_cloud_dataset(
            dataset_type="AzureBlobStorageDataset",
            dataset_name="blob_avro_dataset",
            bucket_name="avro-container",
            folder_path="raw",
            file_name="events.avro",
            linked_service={
                "name": "blob-avro-service",
                "properties": {
                    "url": "https://myaccount.blob.core.windows.net/",
                },
            },
            file_format={"type": "AvroFormat"},
        )
        result = translate_azure_blob_file_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_type == "Avro"
        assert result.provider_type == "azure_blob"

    def test_translate_azure_blob_dataset_missing_linked_service_url(self) -> None:
        """Test Azure Blob dataset with linked service missing URL returns UnsupportedValue."""
        dataset = _build_cloud_dataset(
            dataset_type="AzureBlobStorageDataset",
            dataset_name="blob_no_url",
            bucket_name="container",
            folder_path="data",
            file_name="file.parquet",
            linked_service={
                "name": "blob-no-url-service",
                "properties": {},
            },
        )
        result = translate_azure_blob_file_dataset(dataset)

        assert isinstance(result, UnsupportedValue)
        assert "url" in result.message.lower()

    def test_translate_azure_blob_dataset_null_returns_unsupported(self) -> None:
        """Test null Azure Blob dataset returns UnsupportedValue."""
        result = translate_azure_blob_file_dataset({})

        assert isinstance(result, UnsupportedValue)
        assert "Missing Azure Blob dataset definition" in result.message

    def test_translate_dataset_dispatches_azure_blob(self) -> None:
        """Test that translate_dataset correctly dispatches AzureBlobStorageDataset."""
        dataset = _build_cloud_dataset(
            dataset_type="AzureBlobStorageDataset",
            dataset_name="blob_dispatch",
            bucket_name="container",
            folder_path="path",
            file_name="file.parquet",
            linked_service={
                "name": "blob-svc",
                "properties": {
                    "url": "https://account.blob.core.windows.net/",
                },
            },
        )
        result = translate_dataset(dataset)

        assert isinstance(result, FileDataset)
        assert result.dataset_name == "blob_dispatch"
        assert result.provider_type == "azure_blob"

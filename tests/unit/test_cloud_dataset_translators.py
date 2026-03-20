"""Tests for cloud file dataset translators (S3, GCS, Azure Blob, ABFS).

This module tests dataset translation for Amazon S3, Google Cloud Storage,
Azure Blob Storage, and ABFS datasets.  Cloud file datasets use standard ADF
file types (e.g. ``DelimitedText``, ``Parquet``) with cloud-specific location
types that determine the storage provider.
"""

from __future__ import annotations

import pytest


from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.dataset_translators import (
    translate_file_dataset,
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


def test_translate_s3_dataset_delimited_text() -> None:
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
    result = translate_file_dataset("DelimitedText", dataset, "s3")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "s3_csv_dataset"
    assert result.dataset_type == "DelimitedText"
    assert result.container == "my-data-bucket"
    assert result.folder_path == "csv/csv_file.csv"
    assert result.service_name == "AmazonS31"
    assert result.url == "https://s3.amazonaws.com/afeaef"
    assert result.provider_type == "s3"


def test_translate_s3_dataset_parquet() -> None:
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
    result = translate_file_dataset("Parquet", dataset, "s3")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "s3_parquet_dataset"
    assert result.dataset_type == "Parquet"
    assert result.container == "my-data-bucket"
    assert result.folder_path == "raw/data/events.parquet"
    assert result.service_name == "s3-linked-service"
    assert result.url == "https://s3.amazonaws.com"
    assert result.provider_type == "s3"


def test_translate_s3_dataset_no_folder() -> None:
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
    result = translate_file_dataset("Parquet", dataset, "s3")

    assert isinstance(result, FileDataset)
    assert result.folder_path == "data.parquet"


def test_translate_s3_dataset_missing_location() -> None:
    """Test S3 dataset with missing location returns UnsupportedValue."""
    dataset = {
        "name": "s3_no_location",
        "properties": {"type": "DelimitedText"},
        "linked_service_definition": {"name": "svc", "properties": {}},
    }
    result = translate_file_dataset("DelimitedText", dataset, "s3")

    assert isinstance(result, UnsupportedValue)
    assert "location" in result.message


def test_translate_s3_dataset_null_returns_unsupported() -> None:
    """Test null S3 dataset returns UnsupportedValue."""
    result = translate_file_dataset("DelimitedText", {}, "s3")

    assert isinstance(result, UnsupportedValue)
    assert "s3" in result.message.lower()


def test_translate_dataset_dispatches_s3() -> None:
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


def test_translate_gcs_dataset_delimited_text() -> None:
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
    result = translate_file_dataset("DelimitedText", dataset, "gcs")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "gcs_csv_dataset"
    assert result.dataset_type == "DelimitedText"
    assert result.container == "gcs-data-bucket"
    assert result.folder_path == "csv_files/csv_file.csv"
    assert result.service_name == "GoogleCloudStorage1"
    assert result.url == "https://storage.googleapis.com/alkfjea"
    assert result.provider_type == "gcs"


def test_translate_gcs_dataset_parquet() -> None:
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
    result = translate_file_dataset("Parquet", dataset, "gcs")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "gcs_parquet_dataset"
    assert result.dataset_type == "Parquet"
    assert result.provider_type == "gcs"


def test_translate_gcs_dataset_missing_file_name() -> None:
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
    result = translate_file_dataset("DelimitedText", dataset, "gcs")

    assert isinstance(result, UnsupportedValue)
    assert "file_name" in result.message


def test_translate_gcs_dataset_null_returns_unsupported() -> None:
    """Test null GCS dataset returns UnsupportedValue."""
    result = translate_file_dataset("DelimitedText", {}, "gcs")

    assert isinstance(result, UnsupportedValue)
    assert "gcs" in result.message.lower()


def test_translate_dataset_dispatches_gcs() -> None:
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


def test_translate_azure_blob_dataset_parquet() -> None:
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
    result = translate_file_dataset("Parquet", dataset, "azure_blob")

    assert isinstance(result, FileDataset)
    assert result.url == "https://myblob.blob.core.windows.net/"
    assert result.dataset_name == "blob_parquet_dataset"
    assert result.dataset_type == "Parquet"
    assert result.container == "blob-container"
    assert result.folder_path == "warehouse/bronze/transactions.parquet"
    assert result.service_name == "blob-linked-service"
    assert result.storage_account_name == "myblob"
    assert result.provider_type == "azure_blob"


def test_translate_azure_blob_dataset_csv() -> None:
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
    with pytest.warns(
        NotTranslatableWarning,
        match="Cannot use service principal or managed identity authentication with Azure Blob linked service",
    ):
        result = translate_file_dataset("DelimitedText", dataset, "azure_blob")

    assert isinstance(result, FileDataset)
    assert result.url == "https://myaccount.blob.core.windows.net/"
    assert result.dataset_type == "DelimitedText"
    assert result.provider_type == "azure_blob"


def test_translate_azure_blob_dataset_missing_linked_service_connection() -> None:
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
    result = translate_file_dataset("Parquet", dataset, "azure_blob")

    assert isinstance(result, UnsupportedValue)
    assert (
        result.message
        == "Missing property 'container_url', 'sas_uri', or 'service_endpoint' in storage account properties"
    )


def test_translate_azure_blob_dataset_null_returns_unsupported() -> None:
    """Test null Azure Blob dataset returns UnsupportedValue."""
    result = translate_file_dataset("Parquet", {}, "azure_blob")

    assert isinstance(result, UnsupportedValue)
    assert "azure_blob" in result.message.lower()


def test_translate_dataset_dispatches_azure_blob() -> None:
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


# --- ABFS dataset translation tests ---


def test_translate_abfs_dataset_delimited_text() -> None:
    """Test ABFS dataset translation with DelimitedText format."""
    dataset = _build_cloud_dataset(
        dataset_type="DelimitedText",
        location_type="AzureBlobFSLocation",
        dataset_name="abfs_csv_dataset",
        bucket_name="raw-data",
        folder_path="csv",
        file_name="data.csv",
        linked_service={
            "name": "abfs-storage-account",
            "properties": {
                "url": "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;EndpointSuffix=core.windows.net;",
                "storage_account_name": "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;EndpointSuffix=core.windows.net;",
            },
        },
        container_key="container",
    )
    result = translate_file_dataset("DelimitedText", dataset, "abfs")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "abfs_csv_dataset"
    assert result.dataset_type == "DelimitedText"
    assert result.container == "raw-data"
    assert result.folder_path == "csv/data.csv"
    assert result.service_name == "abfs-storage-account"
    assert result.url == "https://mystorageaccount.blob.core.windows.net/"
    assert result.storage_account_name == "mystorageaccount"
    assert result.provider_type == "abfs"


def test_translate_abfs_dataset_parquet() -> None:
    """Test ABFS dataset translation with Parquet format."""
    dataset = _build_cloud_dataset(
        dataset_type="Parquet",
        location_type="AzureBlobFSLocation",
        dataset_name="abfs_parquet_dataset",
        bucket_name="processed",
        folder_path="output",
        file_name="result.parquet",
        linked_service={
            "name": "abfs-linked-service",
            "properties": {
                "url": "DefaultEndpointsProtocol=https;AccountName=myaccount;EndpointSuffix=core.windows.net;",
                "storage_account_name": "DefaultEndpointsProtocol=https;AccountName=myaccount;EndpointSuffix=core.windows.net;",
            },
        },
        container_key="container",
    )
    result = translate_file_dataset("Parquet", dataset, "abfs")

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "abfs_parquet_dataset"
    assert result.dataset_type == "Parquet"
    assert result.container == "processed"
    assert result.folder_path == "output/result.parquet"
    assert result.url == "https://myaccount.blob.core.windows.net/"
    assert result.storage_account_name == "myaccount"
    assert result.provider_type == "abfs"


def test_translate_abfs_dataset_null_returns_unsupported() -> None:
    """Test null ABFS dataset returns UnsupportedValue."""
    result = translate_file_dataset("Parquet", {}, "abfs")

    assert isinstance(result, UnsupportedValue)
    assert "abfs" in result.message.lower()


def test_translate_abfs_dataset_missing_location() -> None:
    """Test ABFS dataset with missing location returns UnsupportedValue."""
    dataset = {
        "name": "abfs_no_location",
        "properties": {"type": "DelimitedText"},
        "linked_service_definition": {"name": "svc", "properties": {}},
    }
    result = translate_file_dataset("DelimitedText", dataset, "abfs")

    assert isinstance(result, UnsupportedValue)
    assert "location" in result.message


def test_translate_dataset_dispatches_abfs() -> None:
    """Test that translate_dataset correctly dispatches AzureBlobFSLocation."""
    dataset = _build_cloud_dataset(
        dataset_type="Parquet",
        location_type="AzureBlobFSLocation",
        dataset_name="abfs_dispatch",
        bucket_name="container",
        folder_path="path",
        file_name="file.parquet",
        linked_service={
            "name": "abfs-svc",
            "properties": {
                "url": "DefaultEndpointsProtocol=https;AccountName=myaccount;EndpointSuffix=core.windows.net;",
                "storage_account_name": "DefaultEndpointsProtocol=https;AccountName=myaccount;EndpointSuffix=core.windows.net;",
            },
        },
        container_key="container",
    )
    result = translate_dataset(dataset)

    assert isinstance(result, FileDataset)
    assert result.dataset_name == "abfs_dispatch"
    assert result.provider_type == "abfs"


# --- Unsupported location type tests ---


def test_translate_dataset_unsupported_location_type() -> None:
    """Test that an unrecognized file location type returns UnsupportedValue."""
    dataset = _build_cloud_dataset(
        dataset_type="DelimitedText",
        location_type="SomeUnknownLocation",
        dataset_name="unknown_location",
        bucket_name="bucket",
        folder_path="path",
        file_name="file.csv",
        linked_service={
            "name": "svc",
            "properties": {},
        },
    )
    result = translate_dataset(dataset)

    assert isinstance(result, UnsupportedValue)
    assert "SomeUnknownLocation" in result.message


def test_translate_dataset_empty_location_type() -> None:
    """Test that a missing location type returns UnsupportedValue."""
    dataset = {
        "name": "no_location_type",
        "properties": {
            "type": "DelimitedText",
            "location": {},
        },
        "linked_service_definition": {"name": "svc", "properties": {}},
    }
    result = translate_dataset(dataset)

    assert isinstance(result, UnsupportedValue)
    assert "Unsupported" in result.message

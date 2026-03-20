---
sidebar_label: storage_linked_service_translator
title: wkmigrate.translators.linked_service_translators.storage_linked_service_translator
---

Translators for cloud storage linked service definitions (ABFS, S3, GCS, Azure Blob).

This module normalizes ABFS, Amazon S3, Google Cloud Storage, and Azure Blob Storage
linked-service payloads into their respective internal representations.

#### translate\_abfs\_spec

```python
def translate_abfs_spec(
        abfs_spec: dict) -> AbfsLinkedService | UnsupportedValue
```

Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

**Arguments**:

- `abfs_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  ABFS linked-service metadata as a ``AbfsLinkedService`` object.

#### translate\_s3\_spec

```python
def translate_s3_spec(s3_spec: dict) -> S3LinkedService | UnsupportedValue
```

Parses an Amazon S3 linked service definition into an ``S3LinkedService`` object.

**Arguments**:

- `s3_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  S3 linked-service metadata as an ``S3LinkedService`` object.

#### translate\_gcs\_spec

```python
def translate_gcs_spec(gcs_spec: dict) -> GcsLinkedService | UnsupportedValue
```

Parses a Google Cloud Storage linked service definition into a ``GcsLinkedService`` object.

**Arguments**:

- `gcs_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  GCS linked-service metadata as a ``GcsLinkedService`` object.

#### translate\_azure\_blob\_spec

```python
def translate_azure_blob_spec(
        azure_blob_spec: dict) -> AzureBlobLinkedService | UnsupportedValue
```

Parses an Azure Blob Storage linked service definition into an ``AzureBlobLinkedService`` object.

**Arguments**:

- `azure_blob_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  Azure Blob linked-service metadata as an ``AzureBlobLinkedService`` object.


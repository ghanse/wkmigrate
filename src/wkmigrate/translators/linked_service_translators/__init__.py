"""Linked-service translators for Azure Data Factory definitions.

This package normalizes linked-service payloads into internal representations.
Each translator validates required fields, coerces connection settings, and emits
``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.translators.linked_service_translators.databricks_linked_service_translator import (
    translate_databricks_cluster_spec,
)
from wkmigrate.translators.linked_service_translators.sql_linked_service_translator import (
    translate_mysql_spec,
    translate_oracle_spec,
    translate_postgresql_spec,
    translate_sql_server_spec,
)
from wkmigrate.translators.linked_service_translators.storage_linked_service_translator import (
    translate_abfs_spec,
    translate_azure_blob_spec,
    translate_gcs_spec,
    translate_s3_spec,
)

__all__ = [
    "translate_abfs_spec",
    "translate_azure_blob_spec",
    "translate_databricks_cluster_spec",
    "translate_gcs_spec",
    "translate_mysql_spec",
    "translate_oracle_spec",
    "translate_postgresql_spec",
    "translate_s3_spec",
    "translate_sql_server_spec",
]

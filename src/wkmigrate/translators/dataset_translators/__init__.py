"""Dataset translators for Azure Data Factory definitions.

This package normalizes dataset payloads into internal representations. Each
translator validates required fields, coerces connection settings, and emits
``UnsupportedValue`` objects for any unparsable inputs.
"""

from wkmigrate.translators.dataset_translators.dataset_translator import (
    translate_dataset,
)
from wkmigrate.translators.dataset_translators.delta_table_dataset_translator import translate_delta_table_dataset
from wkmigrate.translators.dataset_translators.file_dataset_translator import translate_file_dataset
from wkmigrate.translators.dataset_translators.sql_dataset_translator import (
    translate_mysql_dataset,
    translate_oracle_dataset,
    translate_postgresql_dataset,
    translate_sql_server_dataset,
)

__all__ = [
    "translate_dataset",
    "translate_delta_table_dataset",
    "translate_file_dataset",
    "translate_mysql_dataset",
    "translate_oracle_dataset",
    "translate_postgresql_dataset",
    "translate_sql_server_dataset",
]

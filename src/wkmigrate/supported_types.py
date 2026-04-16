"""Canonical sets of ADF object types that wkmigrate can translate.

These constants are the single source of truth shared by translators and
the profiler. When translator support is added for a new ADF type, it
should be registered here so the profiler stays in sync automatically.
"""

SUPPORTED_ACTIVITY_TYPES: frozenset[str] = frozenset(
    {
        "Copy",
        "DatabricksJob",
        "DatabricksNotebook",
        "DatabricksSparkJar",
        "DatabricksSparkPython",
        "ForEach",
        "IfCondition",
        "Lookup",
        "SetVariable",
        "WebActivity",
    }
)

SUPPORTED_DATASET_TYPES: frozenset[str] = frozenset(
    {
        "Avro",
        "DelimitedText",
        "Json",
        "Orc",
        "Parquet",
        "AzureSqlTable",
        "AzurePostgreSqlTable",
        "AzureMySqlTable",
        "OracleTable",
        "AzureDatabricksDeltaLakeDataset",
    }
)

SUPPORTED_LINKED_SERVICE_TYPES: frozenset[str] = frozenset(
    {
        "AzureBlobFS",
        "AzureBlobStorage",
        "AzureSqlDatabase",
        "AzureDatabricks",
        "AmazonS3",
        "GoogleCloudStorage",
        "AzurePostgreSql",
        "AzureMySql",
        "Oracle",
    }
)

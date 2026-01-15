FILE_DATASET_TYPES = {"Avro", "DelimitedText", "Json", "Orc", "Parquet"}
SQL_DATASET_TYPES = {"AzureSqlTable"}
DELTA_DATASET_TYPES = {"AzureDatabricksDeltaLakeDataset"}

secrets = {
    "avro": ["storage_account_key"],
    "csv": ["storage_account_key"],
    "delta": [],
    "json": ["storage_account_key"],
    "orc": ["storage_account_key"],
    "parquet": ["storage_account_key"],
    "sqlserver": ["host", "database", "user_name", "password"],
}


options = {
    "csv": [
        "header",
        "sep",
        "lineSep",
        "quote",
        "quoteAll",
        "escape",
        "nullValue",
        "compression",
        "encoding",
    ],
    "json": ["encoding", "compression"],
    "orc": ["compression"],
    "parquet": ["compression"],
    "sqlserver": ["mode", "dbtable", "numPartitions", "batchsize", "sessionInitStatement"],
}

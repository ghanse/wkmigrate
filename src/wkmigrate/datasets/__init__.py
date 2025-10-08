from wkmigrate.datasets.parsers import (
    parse_avro_file_properties,
    parse_avro_file_dataset,
    parse_delimited_file_dataset,
    parse_delimited_file_properties,
    parse_delta_properties,
    parse_delta_table_dataset,
    parse_json_file_dataset,
    parse_json_file_properties,
    parse_orc_file_properties,
    parse_orc_file_dataset,
    parse_parquet_file_properties,
    parse_parquet_file_dataset,
    parse_sql_server_properties,
    parse_sql_server_dataset,
)


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
    "sqlserver": ["host", "database", "user_name", "password"],
}


dataset_parsers = {
    "Avro": parse_avro_file_dataset,
    "AzureDatabricksDeltaLakeDataset": parse_delta_table_dataset,
    "AzureSqlTable": parse_sql_server_dataset,
    "DelimitedText": parse_delimited_file_dataset,
    "Json": parse_json_file_dataset,
    "Orc": parse_orc_file_dataset,
    "Parquet": parse_parquet_file_dataset,
}


property_parsers = {
    "AvroSource": parse_avro_file_properties,
    "AvroSink": parse_avro_file_properties,
    "AzureDatabricksDeltaLakeSource": parse_delta_properties,
    "AzureDatabricksDeltaLakeSink": parse_delta_properties,
    "AzureSqlSource": parse_sql_server_properties,
    "AzureSqlSink": parse_sql_server_properties,
    "DelimitedTextSource": parse_delimited_file_properties,
    "DelimitedTextSink": parse_delimited_file_properties,
    "JsonSource": parse_json_file_properties,
    "JsonSink": parse_json_file_properties,
    "OrcSource": parse_orc_file_properties,
    "OrcSink": parse_orc_file_properties,
    "ParquetSource": parse_parquet_file_properties,
    "ParquetSink": parse_parquet_file_properties,
}

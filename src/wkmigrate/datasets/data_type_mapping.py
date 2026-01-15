"""This module defines methods for mapping data types from target systems to Spark.

The helpers in this module normalize upstream sink types (for example SQL Server column types)
into the canonical Spark SQL types used by generated DataFrame operations. Extend the mapping
dictionaries to support additional systems without touching dataset translators.
"""

sql_server_type_mapping = {
    "Boolean": "boolean",
    "Int16": "short",
    "Int32": "int",
    "Int64": "long",
    "Single": "float",
    "Double": "double",
    "Decimal": "decimal(38, 38)",
}


def parse_spark_data_type(sink_type: str, sink_system: str) -> str:
    """
    Converts a source-system data type to the Spark equivalent.

    Args:
        sink_type: Data type string defined in the source system.
        sink_system: Identifier for the source system (e.g., ``"sqlserver"``).

    Returns:
        Spark-compatible data type string.

    Raises:
        ValueError: If the sink system is unsupported.
        ValueError: If the sink type is not supported for the given system.
    """
    if sink_system == "delta":
        return sink_type
    if sink_system == "sqlserver":
        mapped_type = sql_server_type_mapping.get(sink_type)
        if mapped_type is None:
            raise ValueError(f"No data type mapping available for SQL Server type '{sink_type}'")
        return mapped_type
    raise ValueError(f"No data type mapping available for target system '{sink_system}'")

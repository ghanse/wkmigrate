"""This module defines methods for mapping data types from target systems to Spark."""


def parse_spark_data_type(sink_type: str, sink_system: str) -> str:
    """Parses a Spark data type DDL string from a data type DDL string in the target system.

    :param sink_type: Source type as a ``str``
    :param sink_system: Source system as a ``str``
    :returns: Data type in the target system as a ``str``
    """
    if sink_system == "delta":
        return sink_type
    if sink_system == "sqlserver":
        if sink_type == "Boolean":
            return "boolean"
        if sink_type == "Int16":
            return "short"
        if sink_type == "Int32":
            return "int"
        if sink_type == "Int64":
            return "long"
        if sink_type == "Single":
            return "float"
        if sink_type == "Double":
            return "double"
        if sink_type == "Decimal":
            return "decimal(38, 38)"
        if sink_type == "DateTime":
            return "timestamp"
        if sink_type == "String":
            return "string"
        if sink_type.startswith("Byte"):
            return "binary"
        if sink_type == "Guid":
            return "string"

    raise ValueError(f"No data type mapping available for source system '{sink_type}'")

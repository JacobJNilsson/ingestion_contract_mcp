"""Database type mapping utilities."""


def map_database_type_to_contract_type(db_type: str, database_type: str) -> str:
    """Map database-specific types to contract data types.

    Args:
        db_type: The database column type (e.g., 'VARCHAR', 'INTEGER')
        database_type: The database system (postgresql, mysql, sqlite)

    Returns:
        The contract data type (e.g., 'text', 'integer', 'datetime')
    """
    db_type_lower = db_type.lower()

    # Array types (PostgreSQL) - check first before other types
    if database_type == "postgresql" and ("array" in db_type_lower or "[]" in db_type_lower):
        return "array"

    # Common integer types
    if any(t in db_type_lower for t in ["int", "integer", "smallint", "tinyint", "mediumint"]):
        if "big" in db_type_lower:
            return "bigint"
        return "integer"

    # Text types
    if any(t in db_type_lower for t in ["char", "varchar", "text", "clob", "string"]):
        return "text"

    # Floating point types
    if any(t in db_type_lower for t in ["float", "real", "double"]):
        return "float"

    # Decimal/numeric types
    if any(t in db_type_lower for t in ["decimal", "numeric", "money"]):
        return "decimal"

    # Boolean types
    if any(t in db_type_lower for t in ["bool", "boolean", "bit"]):
        return "boolean"

    # Date/time types
    if "timestamp" in db_type_lower or "datetime" in db_type_lower:
        return "datetime"
    if "date" in db_type_lower:
        return "date"
    if "time" in db_type_lower:
        return "time"

    # JSON types
    if "json" in db_type_lower:
        return "json"

    # Binary types
    if any(t in db_type_lower for t in ["blob", "binary", "bytea", "image"]):
        return "binary"

    # UUID types
    if "uuid" in db_type_lower:
        return "uuid"

    # Default to text if unknown
    return "text"

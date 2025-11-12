"""Database introspection and analysis for contract generation"""

import re
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import MetaData, Table, create_engine, inspect, select, text
from sqlalchemy.engine import Engine

from mcp_server.models import QualityMetrics, SourceContract, SourceSchema


def sanitize_connection_string(connection_string: str) -> str:
    """
    Sanitize a database connection string by removing passwords for logging.

    Args:
        connection_string: The database connection string

    Returns:
        Sanitized connection string with password replaced by ***
    """
    try:
        parsed = urlparse(connection_string)
        if parsed.password:
            sanitized = connection_string.replace(parsed.password, "***")
            return sanitized
    except Exception:
        # If parsing fails, use regex as fallback
        pass

    # Fallback: Use regex to find and replace password patterns
    # Matches :password@ or :password/ patterns
    sanitized = re.sub(r"://([^:]+):([^@/]+)@", r"://\1:***@", connection_string)
    return sanitized


def map_database_type_to_contract_type(db_type: str, database_type: str) -> str:
    """
    Map database-specific types to contract data types.

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


def create_database_engine(connection_string: str, database_type: str) -> Engine:
    """
    Create a SQLAlchemy engine for the specified database.

    Args:
        connection_string: The database connection string
        database_type: The database type (postgresql, mysql, sqlite)

    Returns:
        SQLAlchemy Engine instance
    """
    # Add driver-specific connection arguments if needed
    connect_args: dict[str, Any] = {}

    if database_type == "sqlite":
        # SQLite-specific arguments
        connect_args = {"check_same_thread": False}

    engine = create_engine(connection_string, connect_args=connect_args, echo=False)
    return engine


def analyze_database_table(
    connection_string: str,
    database_type: str,
    source_name: str,
    schema: str | None = None,
    sample_size: int = 1000,
) -> tuple[SourceSchema, QualityMetrics, dict[str, Any]]:
    """
    Analyze a database table and extract schema, quality metrics, and metadata.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        source_name: Table name
        schema: Database schema name (optional, defaults to 'public' for PostgreSQL)
        sample_size: Number of rows to sample for quality analysis

    Returns:
        Tuple of (SourceSchema, QualityMetrics, metadata dict)
    """
    engine = create_database_engine(connection_string, database_type)

    try:
        # Get database inspector
        inspector = inspect(engine)

        # For PostgreSQL, default to 'public' schema if not specified
        if database_type == "postgresql" and schema is None:
            schema = "public"

        # Check if table exists
        tables = inspector.get_table_names(schema=schema)
        if source_name not in tables:
            raise ValueError(f"Table '{source_name}' not found in schema '{schema}' or database")

        # Get table columns
        columns = inspector.get_columns(source_name, schema=schema)

        # Extract field names and types
        field_names = [col["name"] for col in columns]
        data_types = [map_database_type_to_contract_type(str(col["type"]), database_type) for col in columns]

        # Create schema
        source_schema = SourceSchema(fields=field_names, data_types=data_types)

        # Get primary key info
        pk_constraint = inspector.get_pk_constraint(source_name, schema=schema)
        primary_keys = pk_constraint.get("constrained_columns", [])

        # Get row count
        metadata = MetaData()
        table = Table(source_name, metadata, autoload_with=engine, schema=schema)

        with engine.connect() as conn:
            # Count total rows
            count_query = select(text("COUNT(*)")).select_from(table)
            result = conn.execute(count_query)
            total_rows = result.scalar() or 0

            # Sample data
            sample_query = select(table).limit(sample_size)
            sample_result = conn.execute(sample_query)
            sample_rows = sample_result.fetchall()

            # Convert sample data to list of lists of strings
            sample_data = [[str(val) if val is not None else "" for val in row] for row in sample_rows[:10]]

        # Quality metrics
        issues: list[str] = []
        if total_rows == 0:
            issues.append("Table is empty")

        # Check for nullable columns
        nullable_columns = [col["name"] for col in columns if col.get("nullable", True)]
        if nullable_columns:
            issues.append(f"Nullable columns: {', '.join(nullable_columns[:5])}")

        quality_metrics = QualityMetrics(
            total_rows=total_rows,
            sample_data=sample_data,
            issues=issues,
        )

        # Additional metadata
        table_metadata: dict[str, Any] = {
            "database_type": database_type,
            "table_name": source_name,
            "schema": schema,
            "primary_keys": primary_keys,
            "column_count": len(field_names),
            "nullable_columns": nullable_columns,
            "sample_size": len(sample_rows),
        }

        # Add column details
        column_details = []
        for col in columns:
            col_info = {
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "default": str(col.get("default")) if col.get("default") is not None else None,
            }
            column_details.append(col_info)
        table_metadata["columns"] = column_details

        return source_schema, quality_metrics, table_metadata

    finally:
        engine.dispose()


def analyze_database_query(
    connection_string: str,
    database_type: str,
    query: str,
    sample_size: int = 1000,
) -> tuple[SourceSchema, QualityMetrics, dict[str, Any]]:
    """
    Analyze a SQL query result and extract schema and quality metrics.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        query: SQL query to execute
        sample_size: Number of rows to sample for quality analysis

    Returns:
        Tuple of (SourceSchema, QualityMetrics, metadata dict)
    """
    engine = create_database_engine(connection_string, database_type)

    try:
        with engine.connect() as conn:
            # Execute query with limit
            limited_query = f"SELECT * FROM ({query}) AS subquery LIMIT {sample_size}"
            result = conn.execute(text(limited_query))

            # Get column names and types
            columns = result.keys()
            field_names = list(columns)

            # Fetch sample rows
            sample_rows = result.fetchall()

            if not sample_rows:
                raise ValueError("Query returned no results")

            # Infer types from sample data
            data_types = []
            for i in range(len(field_names)):
                # Get sample values for this column
                sample_values = [row[i] for row in sample_rows if row[i] is not None]

                if not sample_values:
                    data_types.append("text")
                    continue

                # Infer type from first non-null value
                first_value = sample_values[0]
                if isinstance(first_value, bool):
                    data_types.append("boolean")
                elif isinstance(first_value, int):
                    data_types.append("integer")
                elif isinstance(first_value, float):
                    data_types.append("float")
                else:
                    data_types.append("text")

            # Create schema
            source_schema = SourceSchema(fields=field_names, data_types=data_types)

            # Convert sample data to list of lists of strings
            sample_data = [[str(val) if val is not None else "" for val in row] for row in sample_rows[:10]]

            # Try to get total row count (may fail for complex queries)
            try:
                count_query = f"SELECT COUNT(*) FROM ({query}) AS subquery"
                count_result = conn.execute(text(count_query))
                total_rows = count_result.scalar() or len(sample_rows)
            except Exception:
                total_rows = len(sample_rows)

            # Quality metrics
            issues: list[str] = []
            if total_rows == 0:
                issues.append("Query returned no results")

            quality_metrics = QualityMetrics(
                total_rows=total_rows,
                sample_data=sample_data,
                issues=issues,
            )

            # Additional metadata
            query_metadata: dict[str, Any] = {
                "database_type": database_type,
                "query": query,
                "column_count": len(field_names),
                "sample_size": len(sample_rows),
            }

            return source_schema, quality_metrics, query_metadata

    finally:
        engine.dispose()


def generate_database_source_contract(
    source_id: str,
    connection_string: str,
    database_type: str,
    source_type: str = "table",
    source_name: str | None = None,
    query: str | None = None,
    schema: str | None = None,
    sample_size: int = 1000,
    config: dict[str, Any] | None = None,
) -> SourceContract:
    """
    Generate a source contract from a database table or query.

    Args:
        source_id: Unique identifier for this source
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        source_type: Type of source ('table', 'view', or 'query')
        source_name: Table or view name (required if source_type is 'table' or 'view')
        query: SQL query (required if source_type is 'query')
        schema: Database schema name (optional, for databases that support schemas)
        sample_size: Number of rows to sample for analysis
        config: Additional configuration options

    Returns:
        SourceContract instance

    Raises:
        ValueError: If required parameters are missing or invalid
    """
    # Validate parameters
    if source_type in ("table", "view"):
        if not source_name:
            raise ValueError(f"source_name is required when source_type is '{source_type}'")
    elif source_type == "query":
        if not query:
            raise ValueError("query is required when source_type is 'query'")
    else:
        raise ValueError(f"Invalid source_type: {source_type}. Must be 'table', 'view', or 'query'")

    if database_type not in ("postgresql", "mysql", "sqlite"):
        raise ValueError(f"Unsupported database_type: {database_type}. Must be 'postgresql', 'mysql', or 'sqlite'")

    # Analyze the database
    if source_type in ("table", "view"):
        source_schema, quality_metrics, metadata = analyze_database_table(
            connection_string=connection_string,
            database_type=database_type,
            source_name=source_name,  # type: ignore
            schema=schema,
            sample_size=sample_size,
        )
    else:  # query
        source_schema, quality_metrics, metadata = analyze_database_query(
            connection_string=connection_string,
            database_type=database_type,
            query=query,  # type: ignore
            sample_size=sample_size,
        )

    # Add any additional config to metadata
    if config:
        metadata.update(config)

    # Create source contract
    # Using schema parameter (alias) instead of data_schema to satisfy Pydantic
    contract = SourceContract(
        source_id=source_id,
        database_type=database_type,
        source_type=source_type,
        source_name=source_name,
        database_schema=schema,
        schema=source_schema,
        quality_metrics=quality_metrics,
        metadata=metadata,
    )

    return contract

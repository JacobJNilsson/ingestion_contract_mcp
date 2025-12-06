"""Database schema introspection utilities."""

from typing import Any

from sqlalchemy import MetaData, Table, inspect, select, text

from core.models import QualityMetrics, SourceSchema
from core.sources.database.engine import create_database_engine
from core.sources.database.type_mapping import map_database_type_to_contract_type


def analyze_database_table(
    connection_string: str,
    database_type: str,
    source_name: str,
    schema: str | None = None,
    sample_size: int = 1000,
) -> tuple[SourceSchema, QualityMetrics, dict[str, Any]]:
    """Analyze a database table and extract schema, quality metrics, and metadata.

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


def inspect_table_schema(
    connection_string: str,
    database_type: str,
    table_name: str,
    schema: str | None = None,
) -> dict[str, Any]:
    """Inspect a database table and return its schema for destination contracts.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        table_name: Name of the table to inspect
        schema: Database schema name (optional, defaults to 'public' for PostgreSQL)

    Returns:
        Dictionary containing fields, types, and constraints

    Raises:
        ValueError: If table is not found
    """
    engine = create_database_engine(connection_string, database_type)

    try:
        inspector = inspect(engine)

        # For PostgreSQL, default to 'public' schema if not specified
        if database_type == "postgresql" and schema is None:
            schema = "public"

        # Check if table exists
        if not inspector.has_table(table_name, schema=schema):
            raise ValueError(f"Table '{table_name}' not found in database")

        # Get table columns
        columns = inspector.get_columns(table_name, schema=schema)
        pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)

        fields = []
        types = []
        constraints = {}

        pk_columns = set(pk_constraint.get("constrained_columns", []))

        for col in columns:
            name = col["name"]
            col_type = str(col["type"])

            fields.append(name)
            types.append(col_type)

            col_constraints = []
            if not col.get("nullable", True):
                col_constraints.append("NOT NULL")
            if name in pk_columns:
                col_constraints.append("PRIMARY KEY")

            if col_constraints:
                constraints[name] = col_constraints

        return {
            "fields": fields,
            "types": types,
            "constraints": constraints,
        }

    finally:
        engine.dispose()


def analyze_database_query(
    connection_string: str,
    database_type: str,
    query: str,
    sample_size: int = 1000,
) -> tuple[SourceSchema, QualityMetrics, dict[str, Any]]:
    """Analyze a SQL query result and extract schema and quality metrics.

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


def extract_table_list(
    connection_string: str,
    database_type: str,
    schema: str | None = None,
    with_fields: bool = False,
) -> list[dict[str, Any]]:
    """List tables in the database with optional field details.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        schema: Database schema name (optional)
        with_fields: Whether to include column details

    Returns:
        List of dictionaries containing table 'name' and optional 'columns'
    """
    engine = create_database_engine(connection_string, database_type)

    try:
        inspector = inspect(engine)

        if database_type == "postgresql" and schema is None:
            schema = "public"

        table_names = inspector.get_table_names(schema=schema)
        results = []

        for table_name in table_names:
            table_info: dict[str, Any] = {"name": table_name}

            if with_fields:
                try:
                    columns = inspector.get_columns(table_name, schema=schema)
                    table_info["columns"] = [
                        {
                            "name": col["name"],
                            "type": str(col["type"]),
                            "nullable": col.get("nullable", True),
                        }
                        for col in columns
                    ]
                    table_info["column_count"] = len(columns)
                except Exception:
                    table_info["error"] = "Failed to inspect columns"
                    table_info["column_count"] = 0
            else:
                try:
                    columns = inspector.get_columns(table_name, schema=schema)
                    table_info["column_count"] = len(columns)
                except Exception:
                    table_info["column_count"] = 0

            results.append(table_info)

        return results

    finally:
        engine.dispose()

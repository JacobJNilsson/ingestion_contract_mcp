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


def list_database_tables(
    connection_string: str,
    database_type: str,
    schema: str | None = None,
    include_views: bool = False,
    include_row_counts: bool = True,
) -> list[dict[str, Any]]:
    """
    List all tables in a database or schema with metadata.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        schema: Database schema name (optional, defaults to 'public' for PostgreSQL)
        include_views: Whether to include views in the results
        include_row_counts: Whether to query row counts (may be slow for large databases)

    Returns:
        List of dictionaries with table metadata

    Raises:
        ValueError: If database_type is not supported
    """
    if database_type not in ("postgresql", "mysql", "sqlite"):
        raise ValueError(f"Unsupported database_type: {database_type}. Must be 'postgresql', 'mysql', or 'sqlite'")

    engine = create_database_engine(connection_string, database_type)

    try:
        inspector = inspect(engine)

        # For PostgreSQL, default to 'public' schema if not specified
        if database_type == "postgresql" and schema is None:
            schema = "public"

        # Get table names
        table_names = inspector.get_table_names(schema=schema)

        # Get view names if requested
        view_names = []
        if include_views:
            view_names = inspector.get_view_names(schema=schema)

        # Combine tables and views
        all_tables = [(name, "table") for name in table_names] + [(name, "view") for name in view_names]

        results = []
        metadata = MetaData()

        with engine.connect() as conn:
            for table_name, table_type in all_tables:
                table_info: dict[str, Any] = {
                    "table_name": table_name,
                    "schema": schema,
                    "type": table_type,
                }

                # Get primary key info
                try:
                    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
                    table_info["has_primary_key"] = bool(pk_constraint.get("constrained_columns"))
                    if table_info["has_primary_key"]:
                        table_info["primary_key_columns"] = pk_constraint.get("constrained_columns", [])
                except Exception:
                    # Some databases/views may not support PK inspection
                    table_info["has_primary_key"] = False

                # Get row count if requested
                if include_row_counts and table_type == "table":
                    try:
                        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
                        count_query = select(text("COUNT(*)")).select_from(table)
                        result = conn.execute(count_query)
                        table_info["row_count"] = result.scalar() or 0
                    except Exception:
                        # If count fails, set to None
                        table_info["row_count"] = None
                else:
                    table_info["row_count"] = None

                # Get column count
                try:
                    columns = inspector.get_columns(table_name, schema=schema)
                    table_info["column_count"] = len(columns)
                except Exception:
                    table_info["column_count"] = None

                results.append(table_info)

        # Sort by table name
        results.sort(key=lambda x: x["table_name"])

        return results

    finally:
        engine.dispose()


def detect_foreign_keys(
    connection_string: str,
    database_type: str,
    table_name: str,
    schema: str | None = None,
) -> dict[str, Any]:
    """
    Detect foreign key relationships for a table.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        table_name: Table name to analyze
        schema: Database schema name (optional)

    Returns:
        Dictionary with foreign_keys and referenced_by lists
    """
    engine = create_database_engine(connection_string, database_type)

    try:
        inspector = inspect(engine)

        # For PostgreSQL, default to 'public' schema if not specified
        if database_type == "postgresql" and schema is None:
            schema = "public"

        relationships: dict[str, Any] = {
            "foreign_keys": [],
            "referenced_by": [],
        }

        # Get foreign keys from this table to other tables
        try:
            fks = inspector.get_foreign_keys(table_name, schema=schema)
            for fk in fks:
                relationships["foreign_keys"].append(
                    {
                        "constraint_name": fk.get("name"),
                        "columns": fk.get("constrained_columns", []),
                        "referred_table": fk.get("referred_table"),
                        "referred_columns": fk.get("referred_columns", []),
                        "referred_schema": fk.get("referred_schema"),
                    }
                )
        except Exception:
            # Some databases may not support FK inspection
            pass

        # Get tables that reference this table
        try:
            all_tables = inspector.get_table_names(schema=schema)
            for other_table in all_tables:
                if other_table == table_name:
                    continue
                try:
                    other_fks = inspector.get_foreign_keys(other_table, schema=schema)
                    for fk in other_fks:
                        if fk.get("referred_table") == table_name:
                            relationships["referenced_by"].append(
                                {
                                    "constraint_name": fk.get("name"),
                                    "table": other_table,
                                    "columns": fk.get("constrained_columns", []),
                                    "referred_columns": fk.get("referred_columns", []),
                                }
                            )
                except Exception:
                    continue
        except Exception:
            pass

        return relationships

    finally:
        engine.dispose()


def calculate_load_order(table_dependencies: dict[str, list[str]]) -> tuple[list[str], dict[str, int]]:
    """
    Calculate the load order for tables based on foreign key dependencies.
    Uses topological sort to determine safe load order.

    Args:
        table_dependencies: Dict mapping table names to list of tables they depend on

    Returns:
        Tuple of (sorted table list, load_order dict with levels)
    """
    # Build in-degree count (how many dependencies each table has)
    in_degree = {table: len(deps) for table, deps in table_dependencies.items()}

    # Topological sort using Kahn's algorithm
    queue = [table for table, degree in in_degree.items() if degree == 0]
    sorted_tables = []
    load_order_levels: dict[str, int] = {}
    level = 1

    while queue:
        # Process all tables at the current level
        current_level_tables = sorted(queue)  # Sort for consistent ordering
        queue = []

        for table in current_level_tables:
            sorted_tables.append(table)
            load_order_levels[table] = level

            # Reduce in-degree for tables that depend on this table
            for dep_table, deps in table_dependencies.items():
                if table in deps:
                    in_degree[dep_table] -= 1
                    if in_degree[dep_table] == 0:
                        queue.append(dep_table)

        level += 1

    # Check for circular dependencies
    if len(sorted_tables) < len(table_dependencies):
        # Some tables weren't included - circular dependency detected
        missing = set(table_dependencies.keys()) - set(sorted_tables)
        # Add remaining tables with a special marker
        for table in sorted(missing):
            sorted_tables.append(table)
            load_order_levels[table] = -1  # Indicates circular dependency

    return sorted_tables, load_order_levels


def generate_database_multi_source_contracts(
    connection_string: str,
    database_type: str,
    schema: str | None = None,
    tables: list[str] | None = None,
    include_relationships: bool = True,
    sample_size: int = 1000,
    config: dict[str, Any] | None = None,
) -> list[SourceContract]:
    """
    Generate source contracts for multiple tables with relationship metadata.

    Args:
        connection_string: Database connection string
        database_type: Database type (postgresql, mysql, sqlite)
        schema: Database schema name (optional)
        tables: List of specific tables to analyze (None = all tables)
        include_relationships: Whether to detect and include FK relationships
        sample_size: Number of rows to sample per table
        config: Optional configuration dictionary

    Returns:
        List of SourceContract instances with relationship metadata

    Raises:
        ValueError: If database_type is not supported or tables not found
    """
    if database_type not in ("postgresql", "mysql", "sqlite"):
        raise ValueError(f"Unsupported database_type: {database_type}")

    # Get list of tables
    if tables is None:
        table_list_result = list_database_tables(
            connection_string=connection_string,
            database_type=database_type,
            schema=schema,
            include_views=False,
            include_row_counts=False,
        )
        tables = [t["table_name"] for t in table_list_result]

    if not tables:
        return []

    # Detect relationships if requested
    relationships_map: dict[str, dict[str, Any]] = {}
    table_dependencies: dict[str, list[str]] = {}

    if include_relationships:
        for table_name in tables:
            relationships = detect_foreign_keys(
                connection_string=connection_string,
                database_type=database_type,
                table_name=table_name,
                schema=schema,
            )
            relationships_map[table_name] = relationships

            # Build dependency list (tables this table depends on)
            depends_on = []
            for fk in relationships["foreign_keys"]:
                referred_table = fk["referred_table"]
                if referred_table in tables:
                    depends_on.append(referred_table)
            table_dependencies[table_name] = depends_on

        # Calculate load order
        sorted_tables, load_order_levels = calculate_load_order(table_dependencies)
    else:
        sorted_tables = sorted(tables)
        load_order_levels = dict.fromkeys(tables, 1)

    # Generate contracts for each table
    contracts = []
    for table_name in sorted_tables:
        try:
            # Generate base contract
            contract = generate_database_source_contract(
                source_id=table_name,
                connection_string=connection_string,
                database_type=database_type,
                source_type="table",
                source_name=table_name,
                schema=schema,
                sample_size=sample_size,
                config=config,
            )

            # Add relationship metadata if available
            if include_relationships and table_name in relationships_map:
                contract.metadata["relationships"] = relationships_map[table_name]
                contract.metadata["load_order"] = load_order_levels.get(table_name, 1)
                contract.metadata["depends_on"] = table_dependencies.get(table_name, [])

            contracts.append(contract)

        except Exception:
            # Skip tables that fail to analyze
            continue

    return contracts

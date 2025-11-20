"""Database relationship analysis and table listing."""

import logging
from typing import Any

from sqlalchemy import MetaData, Table, inspect, select, text
from sqlalchemy.exc import DatabaseError, NoSuchTableError, OperationalError

from core.sources.database.engine import create_database_engine

logger = logging.getLogger(__name__)


def list_database_tables(
    connection_string: str,
    database_type: str,
    schema: str | None = None,
    include_views: bool = False,
    include_row_counts: bool = True,
) -> list[dict[str, Any]]:
    """List all tables in a database or schema with metadata.

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
                except NotImplementedError:
                    # Some databases/views don't support PK inspection
                    logger.debug(f"PK inspection not supported for {table_type} '{table_name}'")
                    table_info["has_primary_key"] = False
                except (NoSuchTableError, DatabaseError) as e:
                    logger.debug(f"Could not get PK info for '{table_name}': {e}")
                    table_info["has_primary_key"] = False
                except Exception as e:
                    logger.warning(f"Unexpected error getting PK for '{table_name}': {e}")
                    table_info["has_primary_key"] = False

                # Get row count if requested
                if include_row_counts and table_type == "table":
                    try:
                        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
                        count_query = select(text("COUNT(*)")).select_from(table)
                        result = conn.execute(count_query)
                        table_info["row_count"] = result.scalar() or 0
                    except (NoSuchTableError, DatabaseError, OperationalError) as e:
                        logger.debug(f"Could not count rows for '{table_name}': {e}")
                        table_info["row_count"] = None
                    except Exception as e:
                        logger.warning(f"Unexpected error counting rows for '{table_name}': {e}")
                        table_info["row_count"] = None
                else:
                    table_info["row_count"] = None

                # Get column count
                try:
                    columns = inspector.get_columns(table_name, schema=schema)
                    table_info["column_count"] = len(columns)
                except (NoSuchTableError, DatabaseError) as e:
                    logger.debug(f"Could not get column count for '{table_name}': {e}")
                    table_info["column_count"] = None
                except Exception as e:
                    logger.warning(f"Unexpected error getting column count for '{table_name}': {e}")
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
    """Detect foreign key relationships for a table.

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
        except NotImplementedError:
            # Some databases don't support FK introspection
            logger.debug(f"Foreign key introspection not supported for {database_type}")
        except (NoSuchTableError, DatabaseError) as e:
            logger.warning(f"Could not inspect foreign keys for table '{table_name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting foreign keys for '{table_name}': {e}", exc_info=True)

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
                except (NoSuchTableError, DatabaseError) as e:
                    logger.debug(f"Could not inspect FKs for table '{other_table}': {e}")
                except Exception as e:
                    logger.warning(f"Unexpected error inspecting '{other_table}' for reverse FKs: {e}")
        except (DatabaseError, OperationalError) as e:
            logger.warning(f"Could not list tables to find reverse foreign keys: {e}")
        except Exception as e:
            logger.error(f"Unexpected error finding reverse foreign keys: {e}", exc_info=True)

        return relationships

    finally:
        engine.dispose()


def calculate_load_order(table_dependencies: dict[str, list[str]]) -> tuple[list[str], dict[str, int]]:
    """Calculate the load order for tables based on foreign key dependencies.

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

"""Database source contract generation."""

import logging
from typing import Any

from sqlalchemy.exc import DatabaseError, NoSuchTableError, OperationalError

from core.models import SourceContract
from core.sources.database.introspection import analyze_database_query, analyze_database_table
from core.sources.database.relationships import calculate_load_order, detect_foreign_keys, list_database_tables

logger = logging.getLogger(__name__)


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
    """Generate a source contract from a database table or query.

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


def generate_database_multi_source_contracts(
    connection_string: str,
    database_type: str,
    schema: str | None = None,
    tables: list[str] | None = None,
    include_relationships: bool = True,
    sample_size: int = 1000,
    config: dict[str, Any] | None = None,
) -> list[SourceContract]:
    """Generate source contracts for multiple tables with relationship metadata.

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
    failed_tables = []

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

        except ValueError as e:
            # Expected validation errors
            logger.warning(f"Skipping table '{table_name}': {e}")
            failed_tables.append(table_name)
        except (NoSuchTableError, DatabaseError, OperationalError) as e:
            # Database errors - table may have been dropped, permissions issue, etc.
            logger.warning(f"Could not analyze table '{table_name}': {e}")
            failed_tables.append(table_name)
        except Exception as e:
            # Unexpected errors - log with full traceback
            logger.error(f"Unexpected error analyzing table '{table_name}': {e}", exc_info=True)
            failed_tables.append(table_name)

    if failed_tables:
        logger.info(
            f"Successfully generated {len(contracts)}/{len(sorted_tables)} contracts. "
            f"Failed tables: {', '.join(failed_tables)}"
        )

    return contracts

"""Database analysis and contract generation.

This package provides utilities for analyzing database tables and generating
source contracts from database schemas.
"""

from core.sources.database.contracts import (
    generate_database_multi_source_contracts,
    generate_database_source_contract,
)
from core.sources.database.engine import create_database_engine, sanitize_connection_string
from core.sources.database.introspection import (
    analyze_database_query,
    analyze_database_table,
    inspect_table_schema,
)
from core.sources.database.relationships import calculate_load_order, detect_foreign_keys, list_database_tables
from core.sources.database.type_mapping import map_database_type_to_contract_type

__all__ = [
    # Engine
    "create_database_engine",
    "sanitize_connection_string",
    # Type mapping
    "map_database_type_to_contract_type",
    # Introspection
    "analyze_database_table",
    "inspect_table_schema",
    "analyze_database_query",
    # Relationships
    "list_database_tables",
    "detect_foreign_keys",
    "calculate_load_order",
    # Contracts
    "generate_database_source_contract",
    "generate_database_multi_source_contracts",
]

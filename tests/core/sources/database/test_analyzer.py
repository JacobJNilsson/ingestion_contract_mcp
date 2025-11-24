"""Tests for database analyzer module"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from core.models import SourceContract
from core.sources.database import (
    calculate_load_order,
    detect_foreign_keys,
    generate_database_multi_source_contracts,
    generate_database_source_contract,
    list_database_tables,
    map_database_type_to_contract_type,
    sanitize_connection_string,
)


@pytest.fixture
def sqlite_db() -> str:  # type: ignore[misc]
    """Create a temporary SQLite database with test data"""
    # Create temporary database file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db") as temp_db:
        db_path = temp_db.name

    # Connect and create schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create test table with various data types
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            balance REAL,
            is_active INTEGER,
            created_at TEXT,
            metadata TEXT
        )
    """)

    # Insert test data
    cursor.executemany(
        """
        INSERT INTO users (username, email, age, balance, is_active, created_at, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        [
            ("john_doe", "john@example.com", 30, 1000.50, 1, "2023-01-01", '{"role": "admin"}'),
            ("jane_smith", "jane@example.com", 25, 2500.75, 1, "2023-02-15", '{"role": "user"}'),
            ("bob_jones", "bob@example.com", 35, 500.00, 0, "2023-03-20", '{"role": "user"}'),
        ],
    )

    # Create another table to test multi-table scenarios
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            status TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    cursor.executemany(
        "INSERT INTO orders (user_id, amount, status) VALUES (?, ?, ?)",
        [
            (1, 100.00, "completed"),
            (1, 50.00, "pending"),
            (2, 200.00, "completed"),
        ],
    )

    conn.commit()
    conn.close()

    yield f"sqlite:///{db_path}"

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def test_sanitize_connection_string() -> None:
    """Test connection string sanitization"""
    # PostgreSQL style
    conn_str = "postgresql://user:secret_password@localhost:5432/mydb"
    sanitized = sanitize_connection_string(conn_str)
    assert "secret_password" not in sanitized
    assert "***" in sanitized
    assert sanitized == "postgresql://user:***@localhost:5432/mydb"

    # MySQL style
    conn_str = "mysql://root:my_pass@localhost:3306/test"
    sanitized = sanitize_connection_string(conn_str)
    assert "my_pass" not in sanitized
    assert "***" in sanitized

    # Connection string without password
    conn_str = "sqlite:///path/to/database.db"
    sanitized = sanitize_connection_string(conn_str)
    assert sanitized == conn_str


def test_map_database_type_to_contract_type() -> None:
    """Test database type to contract type mapping"""
    # Integer types
    assert map_database_type_to_contract_type("INTEGER", "sqlite") == "integer"
    assert map_database_type_to_contract_type("BIGINT", "postgresql") == "bigint"
    assert map_database_type_to_contract_type("SMALLINT", "mysql") == "integer"

    # Text types
    assert map_database_type_to_contract_type("VARCHAR(255)", "postgresql") == "text"
    assert map_database_type_to_contract_type("TEXT", "sqlite") == "text"
    assert map_database_type_to_contract_type("CHAR(10)", "mysql") == "text"

    # Floating point types
    assert map_database_type_to_contract_type("REAL", "sqlite") == "float"
    assert map_database_type_to_contract_type("DOUBLE", "mysql") == "float"
    assert map_database_type_to_contract_type("FLOAT", "postgresql") == "float"

    # Decimal types
    assert map_database_type_to_contract_type("DECIMAL(10,2)", "postgresql") == "decimal"
    assert map_database_type_to_contract_type("NUMERIC", "sqlite") == "decimal"

    # Boolean types
    assert map_database_type_to_contract_type("BOOLEAN", "postgresql") == "boolean"
    assert map_database_type_to_contract_type("BOOL", "mysql") == "boolean"

    # Date/time types
    assert map_database_type_to_contract_type("TIMESTAMP", "postgresql") == "datetime"
    assert map_database_type_to_contract_type("DATETIME", "mysql") == "datetime"
    assert map_database_type_to_contract_type("DATE", "postgresql") == "date"
    assert map_database_type_to_contract_type("TIME", "mysql") == "time"

    # JSON types
    assert map_database_type_to_contract_type("JSON", "postgresql") == "json"
    assert map_database_type_to_contract_type("JSONB", "postgresql") == "json"

    # Binary types
    assert map_database_type_to_contract_type("BLOB", "sqlite") == "binary"
    assert map_database_type_to_contract_type("BYTEA", "postgresql") == "binary"

    # UUID types
    assert map_database_type_to_contract_type("UUID", "postgresql") == "uuid"

    # Array types (PostgreSQL)
    assert map_database_type_to_contract_type("INTEGER[]", "postgresql") == "array"

    # Unknown types default to text
    assert map_database_type_to_contract_type("CUSTOM_TYPE", "postgresql") == "text"


def test_generate_database_source_contract_from_table(sqlite_db: str) -> None:
    """Test generating source contract from a database table"""
    contract = generate_database_source_contract(
        source_id="test_users",
        connection_string=sqlite_db,
        database_type="sqlite",
        source_type="table",
        source_name="users",
    )

    assert isinstance(contract, SourceContract)
    assert contract.source_id == "test_users"
    assert contract.database_type == "sqlite"
    assert contract.source_type == "table"
    assert contract.source_name == "users"

    # Check schema
    schema = contract.data_schema
    assert "id" in schema.fields
    assert "username" in schema.fields
    assert "email" in schema.fields
    assert "age" in schema.fields
    assert "balance" in schema.fields

    # Check that types are mapped correctly
    assert len(schema.fields) == len(schema.data_types)

    # Check quality metrics
    assert contract.quality_metrics.total_rows == 3
    assert len(contract.quality_metrics.sample_data) <= 10

    # Check metadata
    assert contract.metadata.get("database_type") == "sqlite"
    assert contract.metadata.get("table_name") == "users"


def test_generate_database_source_contract_from_query(sqlite_db: str) -> None:
    """Test generating source contract from a SQL query"""
    query = """
        SELECT username, email, age
        FROM users
        WHERE is_active = 1
    """

    contract = generate_database_source_contract(
        source_id="active_users",
        connection_string=sqlite_db,
        database_type="sqlite",
        source_type="query",
        query=query,
    )

    assert isinstance(contract, SourceContract)
    assert contract.source_id == "active_users"
    assert contract.database_type == "sqlite"
    assert contract.source_type == "query"

    # Check schema - should only have selected columns
    schema = contract.data_schema
    assert set(schema.fields) == {"username", "email", "age"}

    # Check quality metrics - should only have active users
    assert contract.quality_metrics.total_rows == 2


def test_generate_database_source_contract_with_sample_size(sqlite_db: str) -> None:
    """Test that sample_size parameter is respected"""
    contract = generate_database_source_contract(
        source_id="test_users",
        connection_string=sqlite_db,
        database_type="sqlite",
        source_type="table",
        source_name="users",
        sample_size=1,
    )

    # Should limit sample data but not affect total count
    assert contract.quality_metrics.total_rows == 3
    assert len(contract.quality_metrics.sample_data) <= 10  # We always limit display to 10
    assert contract.metadata.get("sample_size") == 1


def test_generate_database_source_contract_with_config(sqlite_db: str) -> None:
    """Test that config parameter is included in metadata"""
    config = {"custom_field": "custom_value", "batch_size": 100}

    contract = generate_database_source_contract(
        source_id="test_users",
        connection_string=sqlite_db,
        database_type="sqlite",
        source_type="table",
        source_name="users",
        config=config,
    )

    assert contract.metadata.get("custom_field") == "custom_value"
    assert contract.metadata.get("batch_size") == 100


def test_generate_database_source_contract_invalid_table(sqlite_db: str) -> None:
    """Test error handling for non-existent table"""
    with pytest.raises(ValueError, match="Table 'nonexistent' not found"):
        generate_database_source_contract(
            source_id="test",
            connection_string=sqlite_db,
            database_type="sqlite",
            source_type="table",
            source_name="nonexistent",
        )


def test_generate_database_source_contract_missing_source_name(sqlite_db: str) -> None:
    """Test error when source_name is missing for table source_type"""
    with pytest.raises(ValueError, match="source_name is required"):
        generate_database_source_contract(
            source_id="test",
            connection_string=sqlite_db,
            database_type="sqlite",
            source_type="table",
        )


def test_generate_database_source_contract_missing_query(sqlite_db: str) -> None:
    """Test error when query is missing for query source_type"""
    with pytest.raises(ValueError, match="query is required"):
        generate_database_source_contract(
            source_id="test",
            connection_string=sqlite_db,
            database_type="sqlite",
            source_type="query",
        )


def test_generate_database_source_contract_invalid_source_type(sqlite_db: str) -> None:
    """Test error for invalid source_type"""
    with pytest.raises(ValueError, match="Invalid source_type"):
        generate_database_source_contract(
            source_id="test",
            connection_string=sqlite_db,
            database_type="sqlite",
            source_type="invalid",
            source_name="users",
        )


def test_generate_database_source_contract_unsupported_database(sqlite_db: str) -> None:
    """Test error for unsupported database type"""
    with pytest.raises(ValueError, match="Unsupported database_type"):
        generate_database_source_contract(
            source_id="test",
            connection_string=sqlite_db,
            database_type="oracle",  # Not supported in MVP
            source_type="table",
            source_name="users",
        )


def test_generate_database_source_contract_empty_query_result(sqlite_db: str) -> None:
    """Test handling of queries that return no results"""
    query = "SELECT * FROM users WHERE id = 999999"

    with pytest.raises(ValueError, match="Query returned no results"):
        generate_database_source_contract(
            source_id="empty_query",
            connection_string=sqlite_db,
            database_type="sqlite",
            source_type="query",
            query=query,
        )


def test_generate_database_source_contract_serialization(sqlite_db: str) -> None:
    """Test that the contract can be serialized to JSON"""
    contract = generate_database_source_contract(
        source_id="test_users",
        connection_string=sqlite_db,
        database_type="sqlite",
        source_type="table",
        source_name="users",
    )

    # Should be able to serialize to JSON
    json_str = contract.model_dump_json(by_alias=True)
    assert isinstance(json_str, str)
    assert "test_users" in json_str
    assert "sqlite" in json_str

    # Should use "schema" alias in JSON
    assert '"schema"' in json_str
    assert '"data_schema"' not in json_str


# ============================================================================
# List Database Tables Tests
# ============================================================================


def test_list_database_tables_basic(sqlite_db: str) -> None:
    """Test listing tables from a database"""
    tables = list_database_tables(
        connection_string=sqlite_db,
        database_type="sqlite",
    )

    assert isinstance(tables, list)
    assert len(tables) == 2  # users and orders

    # Find users table
    users_table = next((t for t in tables if t["table_name"] == "users"), None)
    assert users_table is not None
    assert users_table["type"] == "table"
    assert users_table["has_primary_key"] is True
    assert users_table["row_count"] == 3
    assert users_table["column_count"] == 8

    # Find orders table
    orders_table = next((t for t in tables if t["table_name"] == "orders"), None)
    assert orders_table is not None
    assert orders_table["type"] == "table"
    assert orders_table["has_primary_key"] is True


def test_list_database_tables_without_row_counts(sqlite_db: str) -> None:
    """Test listing tables without querying row counts"""
    tables = list_database_tables(
        connection_string=sqlite_db,
        database_type="sqlite",
        include_row_counts=False,
    )

    assert isinstance(tables, list)
    assert len(tables) == 2

    # Row counts should be None
    for table in tables:
        assert table["row_count"] is None
        assert table["column_count"] is not None


def test_list_database_tables_sorted(sqlite_db: str) -> None:
    """Test that tables are returned sorted by name"""
    tables = list_database_tables(
        connection_string=sqlite_db,
        database_type="sqlite",
    )

    table_names = [t["table_name"] for t in tables]
    assert table_names == sorted(table_names)


def test_list_database_tables_unsupported_database() -> None:
    """Test error for unsupported database type"""
    with pytest.raises(ValueError, match="Unsupported database_type"):
        list_database_tables(
            connection_string="oracle://user:pass@localhost/db",
            database_type="oracle",
        )


def test_list_database_tables_empty_database() -> None:
    """Test listing tables from an empty database"""
    # Create empty database
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db") as temp_db:
        db_path = temp_db.name

    try:
        # Just create the connection, no tables
        conn = sqlite3.connect(db_path)
        conn.close()

        tables = list_database_tables(
            connection_string=f"sqlite:///{db_path}",
            database_type="sqlite",
        )

        assert isinstance(tables, list)
        assert len(tables) == 0

    finally:
        Path(db_path).unlink(missing_ok=True)


def test_list_database_tables_primary_key_info(sqlite_db: str) -> None:
    """Test that primary key information is correctly detected"""
    tables = list_database_tables(
        connection_string=sqlite_db,
        database_type="sqlite",
    )

    users_table = next((t for t in tables if t["table_name"] == "users"), None)
    assert users_table is not None
    assert users_table["has_primary_key"] is True
    assert "primary_key_columns" in users_table
    assert "id" in users_table["primary_key_columns"]


def test_list_database_tables_metadata_structure(sqlite_db: str) -> None:
    """Test that each table has the expected metadata structure"""
    tables = list_database_tables(
        connection_string=sqlite_db,
        database_type="sqlite",
    )

    for table in tables:
        # Required fields
        assert "table_name" in table
        assert "schema" in table
        assert "type" in table
        assert "has_primary_key" in table
        assert "row_count" in table
        assert "column_count" in table

        # Type checks
        assert isinstance(table["table_name"], str)
        assert isinstance(table["type"], str)
        assert isinstance(table["has_primary_key"], bool)
        assert isinstance(table["column_count"], (int, type(None)))
        assert isinstance(table["row_count"], (int, type(None)))


# ============================================================================
# Multi-Table Analysis Tests
# ============================================================================


def test_detect_foreign_keys(sqlite_db: str) -> None:
    """Test detecting foreign key relationships"""
    # Orders table has a FK to users
    relationships = detect_foreign_keys(
        connection_string=sqlite_db,
        database_type="sqlite",
        table_name="orders",
    )

    assert isinstance(relationships, dict)
    assert "foreign_keys" in relationships
    assert "referenced_by" in relationships

    # Orders should have FK to users
    assert len(relationships["foreign_keys"]) == 1
    fk = relationships["foreign_keys"][0]
    assert fk["referred_table"] == "users"


def test_detect_foreign_keys_referenced_by(sqlite_db: str) -> None:
    """Test detecting tables that reference this table"""
    # Users table is referenced by orders
    relationships = detect_foreign_keys(
        connection_string=sqlite_db,
        database_type="sqlite",
        table_name="users",
    )

    assert len(relationships["referenced_by"]) == 1
    ref = relationships["referenced_by"][0]
    assert ref["table"] == "orders"


def test_calculate_load_order_simple() -> None:
    """Test calculating load order for simple dependencies"""
    # users -> orders -> order_items
    dependencies = {
        "users": [],
        "orders": ["users"],
        "order_items": ["orders"],
    }

    sorted_tables, load_order = calculate_load_order(dependencies)

    assert sorted_tables[0] == "users"
    assert sorted_tables[1] == "orders"
    assert sorted_tables[2] == "order_items"

    assert load_order["users"] == 1
    assert load_order["orders"] == 2
    assert load_order["order_items"] == 3


def test_calculate_load_order_no_dependencies() -> None:
    """Test calculating load order with no dependencies"""
    dependencies: dict[str, list[str]] = {
        "table_a": [],
        "table_b": [],
        "table_c": [],
    }

    sorted_tables, load_order = calculate_load_order(dependencies)

    # All should be at level 1
    assert all(load_order[t] == 1 for t in sorted_tables)


def test_calculate_load_order_complex() -> None:
    """Test calculating load order with complex dependencies"""
    dependencies = {
        "users": [],
        "products": [],
        "orders": ["users"],
        "order_items": ["orders", "products"],
    }

    sorted_tables, load_order = calculate_load_order(dependencies)

    # Users and products should be level 1
    assert load_order["users"] == 1
    assert load_order["products"] == 1

    # Orders should be level 2
    assert load_order["orders"] == 2

    # Order_items should be level 3
    assert load_order["order_items"] == 3


def test_generate_database_multi_source_contracts_all_tables(sqlite_db: str) -> None:
    """Test generating contracts for all tables"""
    contracts = generate_database_multi_source_contracts(
        connection_string=sqlite_db,
        database_type="sqlite",
        include_relationships=True,
    )

    assert len(contracts) == 2  # users and orders
    assert all(contract.database_type == "sqlite" for contract in contracts)

    # Check that we got both tables
    table_names = [contract.source_name for contract in contracts]
    assert "users" in table_names
    assert "orders" in table_names


def test_generate_database_multi_source_contracts_specific_tables(sqlite_db: str) -> None:
    """Test generating contracts for specific tables only"""
    contracts = generate_database_multi_source_contracts(
        connection_string=sqlite_db,
        database_type="sqlite",
        tables=["users"],
        include_relationships=False,
    )

    assert len(contracts) == 1
    assert contracts[0].source_name == "users"


def test_generate_database_multi_source_contracts_with_relationships(sqlite_db: str) -> None:
    """Test that relationship metadata is included"""
    contracts = generate_database_multi_source_contracts(
        connection_string=sqlite_db,
        database_type="sqlite",
        include_relationships=True,
    )

    # Find users and orders contracts
    users_contract = next((c for c in contracts if c.source_name == "users"), None)
    orders_contract = next((c for c in contracts if c.source_name == "orders"), None)

    assert users_contract is not None
    assert orders_contract is not None

    # Check relationship metadata
    assert "relationships" in users_contract.metadata
    assert "load_order" in users_contract.metadata
    assert "depends_on" in users_contract.metadata

    # Users should be referenced by orders
    assert len(users_contract.metadata["relationships"]["referenced_by"]) == 1

    # Orders should have FK to users
    assert len(orders_contract.metadata["relationships"]["foreign_keys"]) == 1

    # Users should load before orders
    assert users_contract.metadata["load_order"] < orders_contract.metadata["load_order"]


def test_generate_database_multi_source_contracts_load_order(sqlite_db: str) -> None:
    """Test that contracts are returned in correct load order"""
    contracts = generate_database_multi_source_contracts(
        connection_string=sqlite_db,
        database_type="sqlite",
        include_relationships=True,
    )

    # Users should come before orders in the list
    table_names = [c.source_name for c in contracts]
    users_idx = table_names.index("users")
    orders_idx = table_names.index("orders")

    assert users_idx < orders_idx


def test_generate_database_multi_source_contracts_without_relationships(sqlite_db: str) -> None:
    """Test generating contracts without relationship detection"""
    contracts = generate_database_multi_source_contracts(
        connection_string=sqlite_db,
        database_type="sqlite",
        include_relationships=False,
    )

    assert len(contracts) == 2

    # Should not have relationship metadata
    for contract in contracts:
        assert "relationships" not in contract.metadata
        assert "load_order" not in contract.metadata or contract.metadata["load_order"] == 1


def test_generate_database_multi_source_contracts_empty_database() -> None:
    """Test generating contracts from empty database"""
    # Create empty database
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db") as temp_db:
        db_path = temp_db.name

    try:
        conn = sqlite3.connect(db_path)
        conn.close()

        contracts = generate_database_multi_source_contracts(
            connection_string=f"sqlite:///{db_path}",
            database_type="sqlite",
        )

        assert len(contracts) == 0

    finally:
        Path(db_path).unlink(missing_ok=True)


def test_generate_database_multi_source_contracts_unsupported_database() -> None:
    """Test error for unsupported database type"""
    with pytest.raises(ValueError, match="Unsupported database_type"):
        generate_database_multi_source_contracts(
            connection_string="oracle://user:pass@localhost/db",
            database_type="oracle",
        )

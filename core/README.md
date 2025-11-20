# Contract Generator MCP Server

This MCP server provides tools for generating three types of contracts for data ingestion pipelines:

1. **Source Contracts** - Describe data sources (schema, format, quality)
2. **Destination Contracts** - Define data destinations (schema, constraints, validation)
3. **Transformation Contracts** - Map source to destination (field mappings, transformations, enrichment)

## Architecture

The three-contract architecture separates concerns:

- **Source Contract**: Automated analysis of source data files
- **Destination Contract**: Manual definition of target schema and rules
- **Transformation Contract**: References both, defines how to move data from source to destination

## Usage in Cursor

Add to `.cursor/mcp.json`:

```json
{
  "contract-generator": {
    "command": "python3",
    "args": ["/absolute/path/to/project/mcp_server/server.py"],
    "env": {}
  }
}
```

## Available Tools

### Contract Generation

1. **generate_source_contract** - Generate a source contract from a data file
   - Automatically analyzes file format, encoding, schema, and quality
   - Returns: JSON contract with `contract_type: "source"`

2. **generate_database_source_contract** - Generate a source contract from a database table or query
   - Supports PostgreSQL, MySQL, and SQLite
   - Analyzes schema, types, and samples data
   - Returns: JSON contract with `contract_type: "source"`

3. **generate_destination_contract** - Generate a destination contract
   - Define target schema, validation rules, and constraints
   - Returns: JSON contract with `contract_type: "destination"`

4. **generate_transformation_contract** - Generate a transformation contract
   - Maps source to destination with transformation rules
   - Returns: JSON contract with `contract_type: "transformation"`

### Database Discovery

5. **list_database_tables** - List all tables in a database with metadata
   - Returns table names, row counts, column counts, and primary key information
   - Helps discover available tables before generating contracts

6. **generate_database_multi_source_contracts** - Generate contracts for multiple tables with relationship analysis
   - Automatically detects foreign key relationships between tables
   - Calculates optimal load order using topological sort
   - Includes relationship metadata in contracts (dependencies, referenced-by)
   - Returns: List of JSON contracts with relationship information

### Analysis & Validation

7. **analyze_source** - Analyze a source file and return raw metadata
8. **validate_contract** - Validate any contract type (source, destination, or transformation)

## Example Workflows

### Discovering Database Tables

```python
# 1. List all tables in a database
tables = list_database_tables(
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    schema="public"
)

# Returns:
# {
#   "tables": [
#     {
#       "table_name": "users",
#       "schema": "public",
#       "type": "table",
#       "has_primary_key": true,
#       "primary_key_columns": ["id"],
#       "row_count": 10000,
#       "column_count": 8
#     },
#     {
#       "table_name": "orders",
#       "schema": "public",
#       "type": "table",
#       "has_primary_key": true,
#       "primary_key_columns": ["order_id"],
#       "row_count": 50000,
#       "column_count": 12
#     }
#   ],
#   "count": 2
# }

# 2. Then generate contracts for selected tables
contract = generate_database_source_contract(
    source_id="users_table",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    source_type="table",
    source_name="users",
    schema="public"
)
```

### File-Based Source

```python
# 1. Generate source contract from CSV file
source_contract = generate_source_contract(
    source_path="/path/to/data.csv",
    source_id="swedish_bank_csv"
)

# 2. Generate destination contract
dest_contract = generate_destination_contract(
    destination_id="dwh_transactions_table",
    schema={
        "fields": ["id", "date", "amount"],
        "types": ["int", "date", "decimal"]
    }
)

# 3. Generate transformation contract
transform_contract = generate_transformation_contract(
    transformation_id="swedish_to_dwh",
    source_ref="swedish_bank_csv",
    destination_ref="dwh_transactions_table"
)

# 4. LLM fills in field_mappings, transformations, enrichment in transform_contract
# 5. Validate and execute
```

### Database-Based Source

```python
# 1. Generate source contract from PostgreSQL table
source_contract = generate_database_source_contract(
    source_id="orders_table",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    source_type="table",
    source_name="orders",
    schema="public"
)

# 2. Generate source contract from SQL query
query_contract = generate_database_source_contract(
    source_id="active_users",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    source_type="query",
    query="SELECT user_id, email, created_at FROM users WHERE status = 'active'"
)

# 3. Continue with destination and transformation contracts as above
```

### Multi-Table Analysis with Relationships

```python
# Analyze multiple related tables at once
contracts = generate_database_multi_source_contracts(
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    schema="public",
    include_relationships=True  # Detect foreign keys and calculate load order
)

# Returns a list of contracts with relationship metadata:
# {
#   "contracts": [
#     {
#       "contract_type": "source",
#       "source_id": "users",
#       "source_name": "users",
#       "database_type": "postgresql",
#       "schema": {...},
#       "metadata": {
#         "relationships": {
#           "foreign_keys": [],  # Tables this table references
#           "referenced_by": [   # Tables that reference this table
#             {
#               "table": "orders",
#               "columns": ["user_id"],
#               "referred_columns": ["id"]
#             }
#           ]
#         },
#         "load_order": 1,       # Load this table first
#         "depends_on": []       # No dependencies
#       }
#     },
#     {
#       "contract_type": "source",
#       "source_id": "orders",
#       "source_name": "orders",
#       "database_type": "postgresql",
#       "schema": {...},
#       "metadata": {
#         "relationships": {
#           "foreign_keys": [    # This table references users
#             {
#               "columns": ["user_id"],
#               "referred_table": "users",
#               "referred_columns": ["id"]
#             }
#           ],
#           "referenced_by": []
#         },
#         "load_order": 2,       # Load after users
#         "depends_on": ["users"]
#       }
#     }
#   ],
#   "count": 2
# }

# Analyze specific tables only
contracts = generate_database_multi_source_contracts(
    connection_string="sqlite:///mydb.db",
    database_type="sqlite",
    tables=["users", "orders", "products"],  # Specific tables
    include_relationships=True
)

# Skip relationship detection for faster analysis
contracts = generate_database_multi_source_contracts(
    connection_string="mysql://user:pass@localhost:3306/mydb",
    database_type="mysql",
    include_relationships=False  # No FK detection or load order
)
```

**Use Cases:**
- **Database Migration**: Analyze entire schema and understand table dependencies
- **Data Warehouse ETL**: Generate contracts for all source tables with correct load order
- **Schema Documentation**: Document relationships and dependencies across tables
- **Incremental Loading**: Use load_order to load dependent tables after their parents

## Contract Types

### Source Contract
```json
{
  "contract_type": "source",
  "source_id": "...",
  "file_format": "csv",
  "schema": {...},
  "quality_metrics": {...}
}
```

### Destination Contract
```json
{
  "contract_type": "destination",
  "destination_id": "...",
  "schema": {...},
  "validation_rules": {...}
}
```

### Transformation Contract
```json
{
  "contract_type": "transformation",
  "source_ref": "source_id",
  "destination_ref": "destination_id",
  "field_mappings": {...},
  "transformations": {...},
  "enrichment": {...}
}
```

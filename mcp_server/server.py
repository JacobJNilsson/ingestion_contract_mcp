"""
MCP Server for Contract Generator
Provides tools for generating ingestion contracts
"""

import sys
import traceback
from pathlib import Path

# Add parent directory to path for imports when running as script
sys.path.insert(0, str(Path(__file__).parent.parent))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from mcp_server.handlers import ContractHandler

# Initialize MCP server
app: Server = Server("ingestion-contract-generator")

# Initialize contract handler (stateless)
contract_handler = ContractHandler()


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available MCP tools"""
    return [
        Tool(
            name="generate_source_contract",
            description=(
                "Generate a source contract that describes a data source. "
                "Automatically analyzes the source file and extracts schema, data types, "
                "encoding, format, and quality metrics. Returns the contract as JSON."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "Absolute path to the source data file",
                    },
                    "source_id": {
                        "type": "string",
                        "description": "Unique identifier for this source (e.g., 'swedish_bank_csv')",
                    },
                    "config": {
                        "type": "object",
                        "description": "Optional configuration/metadata dictionary",
                    },
                },
                "required": ["source_path", "source_id"],
            },
        ),
        Tool(
            name="generate_destination_contract",
            description=(
                "Generate a destination contract that describes where data should be written. "
                "Defines the target schema, validation rules, and constraints. "
                "Returns the contract as JSON."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "destination_id": {
                        "type": "string",
                        "description": "Unique identifier for destination (e.g., 'dwh_transactions_table')",
                    },
                    "schema": {
                        "type": "object",
                        "description": "Optional schema definition with fields, types, and constraints",
                    },
                    "config": {
                        "type": "object",
                        "description": "Optional configuration/metadata dictionary",
                    },
                },
                "required": ["destination_id"],
            },
        ),
        Tool(
            name="generate_transformation_contract",
            description=(
                "Generate a transformation contract that maps source to destination. "
                "Defines field mappings, transformations, enrichment rules, and execution plan. "
                "References existing source and destination contracts. Returns the contract as JSON."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "transformation_id": {
                        "type": "string",
                        "description": "Unique identifier for this transformation",
                    },
                    "source_ref": {
                        "type": "string",
                        "description": "Reference to source contract ID",
                    },
                    "destination_ref": {
                        "type": "string",
                        "description": "Reference to destination contract ID",
                    },
                    "config": {
                        "type": "object",
                        "description": "Optional configuration dict (batch_size, error_threshold, etc.)",
                    },
                },
                "required": ["transformation_id", "source_ref", "destination_ref"],
            },
        ),
        Tool(
            name="generate_database_source_contract",
            description=(
                "Generate a source contract from a database table or query. "
                "Analyzes database schema, extracts column types, and samples data for quality metrics. "
                "Supports PostgreSQL, MySQL, and SQLite. Returns the contract as JSON."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_id": {
                        "type": "string",
                        "description": "Unique identifier for this source (e.g., 'orders_table')",
                    },
                    "connection_string": {
                        "type": "string",
                        "description": "Database connection string (e.g., 'postgresql://user:pass@localhost:5432/mydb')",
                    },
                    "database_type": {
                        "type": "string",
                        "description": "Database type: 'postgresql', 'mysql', or 'sqlite'",
                    },
                    "source_type": {
                        "type": "string",
                        "description": "Source type: 'table', 'view', or 'query' (default: 'table')",
                    },
                    "source_name": {
                        "type": "string",
                        "description": "Table or view name (required if source_type is 'table' or 'view')",
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL query (required if source_type is 'query')",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Database schema name (optional, for databases that support schemas)",
                    },
                    "sample_size": {
                        "type": "integer",
                        "description": "Number of rows to sample for analysis (default: 1000)",
                    },
                    "config": {
                        "type": "object",
                        "description": "Optional configuration/metadata dictionary",
                    },
                },
                "required": ["source_id", "connection_string", "database_type"],
            },
        ),
        Tool(
            name="list_database_tables",
            description=(
                "List all tables in a database or schema with metadata. "
                "Helps discover available tables before generating contracts. "
                "Returns table names, row counts, column counts, and primary key information."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "connection_string": {
                        "type": "string",
                        "description": "Database connection string (e.g., 'postgresql://user:pass@localhost:5432/mydb')",
                    },
                    "database_type": {
                        "type": "string",
                        "description": "Database type: 'postgresql', 'mysql', or 'sqlite'",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Database schema name (optional, defaults to 'public' for PostgreSQL)",
                    },
                    "include_views": {
                        "type": "boolean",
                        "description": "Whether to include views in the results (default: false)",
                    },
                    "include_row_counts": {
                        "type": "boolean",
                        "description": "Whether to query row counts for each table (default: true, may be slow)",
                    },
                },
                "required": ["connection_string", "database_type"],
            },
        ),
        Tool(
            name="generate_database_multi_source_contracts",
            description=(
                "Generate source contracts for multiple tables with relationship analysis. "
                "Detects foreign key relationships, calculates load order, and generates "
                "contracts for all or selected tables. Returns multiple contracts with relationship metadata."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "connection_string": {
                        "type": "string",
                        "description": "Database connection string (e.g., 'postgresql://user:pass@localhost:5432/mydb')",
                    },
                    "database_type": {
                        "type": "string",
                        "description": "Database type: 'postgresql', 'mysql', or 'sqlite'",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Database schema name (optional, defaults to 'public' for PostgreSQL)",
                    },
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of specific table names to analyze (optional, defaults to all tables)",
                    },
                    "include_relationships": {
                        "type": "boolean",
                        "description": "Whether to detect foreign key relationships and calculate load order (default: true)",
                    },
                    "sample_size": {
                        "type": "integer",
                        "description": "Number of rows to sample per table (default: 1000)",
                    },
                },
                "required": ["connection_string", "database_type"],
            },
        ),
        Tool(
            name="analyze_source",
            description=(
                "Analyze a source file and return detailed metadata including file format, encoding, "
                "delimiter, data types, and quality assessment. Returns raw analysis as JSON."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "Absolute path to the source data file",
                    }
                },
                "required": ["source_path"],
            },
        ),
        Tool(
            name="validate_contract",
            description=(
                "Validate any contract JSON file (source, destination, or transformation) "
                "and return validation results as JSON."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "contract_path": {
                        "type": "string",
                        "description": "Absolute path to the contract JSON file",
                    }
                },
                "required": ["contract_path"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, object]) -> list[TextContent]:
    """Handle tool calls - routes to appropriate handler"""

    # Tool dispatch registry
    handlers = {
        "generate_source_contract": contract_handler.generate_source_contract,
        "generate_destination_contract": contract_handler.generate_destination_contract,
        "generate_transformation_contract": contract_handler.generate_transformation_contract,
        "generate_database_source_contract": contract_handler.generate_database_source_contract,
        "generate_database_multi_source_contracts": contract_handler.generate_database_multi_source_contracts,
        "list_database_tables": contract_handler.list_database_tables,
        "analyze_source": contract_handler.analyze_source,
        "validate_contract": contract_handler.validate_contract,
    }

    try:
        # Look up handler
        handler = handlers.get(name)
        if not handler:
            return [TextContent(type="text", text=f"Error: Unknown tool: {name}")]

        # Call handler with arguments - let Python handle argument validation
        result_text = handler(**arguments)  # type: ignore[operator]
        return [TextContent(type="text", text=result_text or "Error: No result generated")]

    except TypeError as e:
        # Handle missing/invalid arguments
        error_text = f"Error: Invalid arguments for tool '{name}': {e!s}"
        return [TextContent(type="text", text=error_text)]
    except (ValueError, ImportError, OSError, RuntimeError) as e:
        error_text = f"Error: {e!s}\n\n{traceback.format_exc()}"
        return [TextContent(type="text", text=error_text)]


async def main() -> None:
    """Run the MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

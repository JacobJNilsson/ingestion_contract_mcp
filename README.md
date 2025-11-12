# Data Ingestion Contract Generator

An MCP (Model Context Protocol) server that automatically generates and validates contracts for data ingestion pipelines. Designed to help AI agents and developers build reliable, type-safe data workflows with automated schema detection and quality assessment.

## Overview

Building data ingestion pipelines is complex and error-prone, especially when dealing with:
- Diverse file formats and encodings (CSV, JSON, etc.)
- International number formats (European vs US)
- Data quality issues (UTF-8 BOMs, sparse data, missing values)
- Schema mismatches between source and destination
- Transformation logic that's hard to track and maintain

This MCP server solves these problems with a **three-contract architecture** that separates concerns and makes data pipelines explicit, validated, and maintainable.

## Three-Contract Architecture

### 1. Source Contracts
**Automatically analyze source data files**

```bash
generate_source_contract(
    source_path="/path/to/transactions.csv",
    source_id="bank_transactions"
)
```

Returns a contract with:
- File format, encoding, and delimiter detection
- Automatic schema inference with data types
- Quality metrics (row counts, sample data, issues)
- UTF-8 BOM detection and handling
- Support for European number formats (1.234,56)

### 2. Destination Contracts
**Define target schemas and validation rules**

```bash
generate_destination_contract(
    destination_id="dwh_transactions",
    schema={
        "fields": ["transaction_id", "date", "amount"],
        "types": ["uuid", "date", "decimal"],
        "constraints": {"amount": "non_negative"}
    }
)
```

Returns a contract with:
- Target schema definition
- Data type specifications
- Validation rules and constraints

### 3. Transformation Contracts
**Map source to destination with transformation logic**

```bash
generate_transformation_contract(
    transformation_id="bank_to_dwh",
    source_ref="bank_transactions",
    destination_ref="dwh_transactions"
)
```

Returns a contract template for:
- Field mappings between source and destination
- Transformation rules (type conversions, formatting)
- Enrichment logic (derived fields, lookups)
- Execution plan (batch size, error handling)

## Quick Start

### Prerequisites

- Python 3.11 or newer (3.13 recommended)
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd ingestion_contract_mcp

# Install dependencies
uv sync

# Run tests to verify setup
make test
```

### Using with Cursor

Add to your `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "contract-generator": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/ingestion_contract_mcp",
        "run",
        "mcp_server/server.py"
      ]
    }
  }
}
```

Then restart Cursor, and the contract generation tools will be available to AI assistants.

### Command Line Usage

```bash
# Run the MCP server
uv run mcp_server/server.py

# Generate a contract from a CSV file
# (Use MCP tools via Cursor or other MCP clients)
```

## Example Workflow

```python
# 1. Analyze a data source
source_contract = generate_source_contract(
    source_path="/data/bank_export.csv",
    source_id="swedish_bank_2024"
)
# Auto-detects: CSV, UTF-8, semicolon delimiter, European numbers
# Returns: schema with 13 fields, 352 rows, quality assessment

# 2. Define destination
destination_contract = generate_destination_contract(
    destination_id="analytics_dwh",
    schema={
        "fields": ["id", "date", "amount", "currency"],
        "types": ["uuid", "date", "decimal", "varchar(3)"]
    }
)

# 3. Create transformation
transformation_contract = generate_transformation_contract(
    transformation_id="bank_to_analytics",
    source_ref="swedish_bank_2024",
    destination_ref="analytics_dwh"
)

# 4. AI or developer fills in field mappings and transformations
# 5. Validate contracts before execution
validate_contract("/path/to/contract.json")
```

## Development

### Setup Development Environment

```bash
# Install dev dependencies
uv sync --all-extras

# Install pre-commit hooks (optional)
pre-commit install
```

### Available Commands

```bash
# Code quality checks
make lint          # Run Ruff linter
make format        # Format code with Ruff
make mypy          # Run type checking
make check         # Run all checks (lint + format-check + mypy)

# Testing
make test          # Run pytest test suite

# Run everything
make check && make test
```

### Running Tests

The project has comprehensive test coverage:

```bash
# Run all tests
make test

# Run specific test file
uv run pytest tests/test_contract_generator.py

# Run with coverage
uv run pytest --cov=mcp_server --cov-report=html
```

**Test categories:**
- `test_contract_generator.py` - Core generation logic
- `test_contract_generator_data_quality.py` - Edge cases (BOMs, sparse data, encodings)
- `test_contract_handler.py` - Business logic and validation
- `test_server.py` - MCP server integration

### Code Quality Tools

- **Ruff** - Fast Python linter and formatter
- **mypy** - Static type checking with strict mode
- **pytest** - Test framework with async support
- **pre-commit** - Git hooks for automated checks

All code is fully typed with Python 3.13+ type hints and validated by mypy in strict mode.

## Project Structure

```
ingestion_contract_mcp/
├── mcp_server/              # Main package
│   ├── server.py            # MCP server entry point
│   ├── contract_handler.py  # Business logic layer
│   ├── contract_generator.py # Contract generation
│   ├── models.py            # Pydantic data models
│   ├── config.py            # Configuration
│   └── README.md            # Detailed API documentation
├── tests/                   # Test suite
│   ├── fixtures/            # Test data files
│   ├── test_server.py       # Server integration tests
│   ├── test_contract_handler.py
│   ├── test_contract_generator.py
│   └── test_contract_generator_data_quality.py
├── data/                    # Example data files
├── .cursor/                 # Cursor IDE configuration
│   └── rules/               # Project-specific AI rules
├── pyproject.toml           # Project metadata and deps
├── Makefile                 # Development commands
├── uv.lock                  # Locked dependencies
└── README.md                # This file
```

## Features

### Automated Source Analysis
- CSV, JSON, and other file format support
- Encoding detection (UTF-8, Latin-1, CP1252, etc.)
- Delimiter sniffing for CSV files
- UTF-8 BOM detection and removal
- Multi-row type inference for sparse data
- European and US number format support

### Type-Safe Contracts
- All contracts are Pydantic models with validation
- Full type hints using Python 3.13+ syntax
- Runtime validation on contract creation
- JSON serialization with aliases

### Stateless Design
- No global state or configuration
- All operations use absolute paths
- Clients control file persistence
- Easy to test and reason about

### Quality Assessment
- Row counting and sampling
- Data quality issue detection
- Format consistency checks
- Detailed error messages

## Documentation

- **[MCP Server API](mcp_server/README.md)** - Detailed tool documentation
- **[Project Architecture](.cursor/rules/project.mdc)** - Architecture decisions and conventions
- **[Git Workflow](.cursor/rules/git.mdc)** - Commit guidelines and workflow
- **[Python Guidelines](.cursor/rules/python.mdc)** - Coding standards

## Requirements

- **Python**: 3.13+
- **MCP**: 1.0.0+
- **Pydantic**: 2.0.0+

See `pyproject.toml` for complete dependency list.

## License

[Add your license information here]

## Contributing

[Add contributing guidelines here]

## Support

For issues, questions, or contributions, please [add contact information or repository link].


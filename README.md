# Data Ingestion Contract Generator

[![CI](https://github.com/JacobJNilsson/ingestion_contract_mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/JacobJNilsson/ingestion_contract_mcp/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)

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

- Python 3.13+
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
make check         # Run all checks (lint + format-check + mypy)
make test          # Run pytest test suite
make format        # Format code with Ruff
```

All code is fully typed with Python 3.13+ type hints. CI runs automatically on pull requests.

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

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

Describe where data comes from. Automatically analyzes and documents:

- **File sources**: CSV, JSON, and other file formats with encoding detection
- **Database sources**: PostgreSQL, MySQL, SQLite tables and queries
- Schema inference with data types
- Quality metrics and data profiling
- Format-specific handling (UTF-8 BOM, European numbers, etc.)

### 2. Destination Contracts

Define where data goes. Specifies:

- Target schema and data types
- Validation rules and constraints
- Required fields and uniqueness constraints
- Data quality requirements

### 3. Transformation Contracts

Map source to destination. Defines:

- Field mappings between source and destination
- Transformation logic (type conversions, formatting)
- Enrichment rules (derived fields, lookups)
- Execution configuration (batch size, error handling)

See the [API documentation](mcp_server/README.md) for detailed usage and examples.

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

## License

This project is licensed under **AGPL-3.0** for open source use. See [LICENSE](LICENSE) for details.

**Commercial licenses** are available if you want to use this software in a closed-source product. Contact [jacobjnilsson@gmail.com](mailto:jacobjnilsson@gmail.com) for inquiries.

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

2. **generate_destination_contract** - Generate a destination contract
   - Define target schema, validation rules, and constraints
   - Returns: JSON contract with `contract_type: "destination"`

3. **generate_transformation_contract** - Generate a transformation contract
   - Maps source to destination with transformation rules
   - Returns: JSON contract with `contract_type: "transformation"`

### Analysis & Validation

4. **analyze_source** - Analyze a source file and return raw metadata
5. **validate_contract** - Validate any contract type (source, destination, or transformation)

## Example Workflow

```python
# 1. Generate source contract
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

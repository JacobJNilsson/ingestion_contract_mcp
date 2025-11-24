"""Contract generation utilities for automated analysis

This module provides automated contract generation functionality.
It should only be used by the MCP server, not by ingestors directly.
"""

from pathlib import Path
from typing import Any

from core.models import (
    DestinationContract,
    DestinationSchema,
    ExecutionPlan,
    QualityMetrics,
    SourceContract,
    SourceSchema,
    TransformationContract,
)
from core.sources.csv import analyze_csv_file
from core.sources.json import analyze_json_file


def generate_source_analysis(source_path: str) -> dict[str, Any]:
    """Generate automated source data analysis

    Args:
        source_path: Path to source data file

    Returns:
        Dictionary with analysis results
    """
    source_file = Path(source_path)
    if not source_file.exists():
        msg = f"Source file not found: {source_path}"
        raise FileNotFoundError(msg)

    # Determine file type by extension
    suffix = source_file.suffix.lower()
    if suffix in [".json", ".jsonl", ".ndjson"]:
        return analyze_json_file(source_file)

    # Default to CSV analysis
    return analyze_csv_file(source_file)


def generate_source_contract(source_path: str, source_id: str, config: dict[str, Any] | None = None) -> SourceContract:
    """Generate a source contract describing a data source

    Args:
        source_path: Path to source data file
        source_id: Unique identifier for this source (e.g., 'swedish_bank_csv')
        config: Optional configuration dictionary

    Returns:
        Source contract model
    """
    source_analysis = generate_source_analysis(source_path)

    return SourceContract(
        source_id=source_id,
        source_path=str(source_path),
        file_format=source_analysis.get("file_type", "unknown"),
        encoding=source_analysis.get("encoding", "utf-8"),
        delimiter=source_analysis.get("delimiter"),
        has_header=source_analysis.get("has_header", True),
        schema=SourceSchema(
            fields=source_analysis.get("sample_fields", []),
            data_types=source_analysis.get("data_types", []),
        ),
        quality_metrics=QualityMetrics(
            total_rows=source_analysis.get("total_rows", 0),
            sample_data=source_analysis.get("sample_data", []),
            issues=source_analysis.get("issues", []),
        ),
        metadata=config or {},
    )


def generate_destination_contract(
    destination_id: str,
    schema: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    connection_string: str | None = None,
    table_name: str | None = None,
    database_type: str | None = None,
    database_schema: str | None = None,
) -> DestinationContract:
    """Generate a destination contract describing a data destination

    Args:
        destination_id: Unique identifier for destination (e.g., 'dwh_transactions_table')
        schema: Schema definition with fields and types
        config: Optional configuration dictionary
        connection_string: Database connection string (optional)
        table_name: Database table name (optional)
        database_type: Database type - postgresql, mysql, or sqlite (required if connection_string provided)
        database_schema: Database schema name (optional, for databases that support schemas)

    Returns:
        Destination contract model
    """
    # If database info is provided, inspect the table
    if connection_string and table_name:
        if not database_type:
            raise ValueError("database_type is required when connection_string is provided")

        from core.sources.database import inspect_table_schema

        try:
            db_schema = inspect_table_schema(
                connection_string=connection_string,
                database_type=database_type,
                table_name=table_name,
                schema=database_schema,
            )
            # Merge with provided schema if any (provided schema takes precedence)
            if schema:
                db_schema.update(schema)
            schema = db_schema
        except Exception as e:
            # If inspection fails, we might still want to proceed if a schema was manually provided
            # otherwise we re-raise
            if not schema:
                raise ValueError(f"Failed to inspect database table: {e}") from e

    # Parse schema if provided, otherwise use defaults
    if schema:
        dest_schema = DestinationSchema(
            fields=schema.get("fields", []),
            types=schema.get("types", []),
            constraints=schema.get("constraints", {}),
        )
    else:
        dest_schema = DestinationSchema()

    return DestinationContract(
        destination_id=destination_id,
        schema=dest_schema,
        metadata=config or {},
    )


def generate_transformation_contract(
    transformation_id: str,
    source_ref: str,
    destination_ref: str,
    config: dict[str, Any] | None = None,
) -> TransformationContract:
    """Generate a transformation contract mapping source to destination

    Args:
        transformation_id: Unique identifier for this transformation
        source_ref: Reference to source contract ID
        destination_ref: Reference to destination contract ID
        config: Optional configuration dictionary

    Returns:
        Transformation contract model
    """
    # Build execution plan from config
    exec_plan = ExecutionPlan(
        batch_size=config.get("batch_size", 100) if config else 100,
        error_threshold=config.get("error_threshold", 0.1) if config else 0.1,
    )

    return TransformationContract(
        transformation_id=transformation_id,
        source_ref=source_ref,
        destination_ref=destination_ref,
        execution_plan=exec_plan,
        metadata=config or {},
    )

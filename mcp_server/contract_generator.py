"""Contract generation utilities for automated analysis

This module provides automated contract generation functionality.
It should only be used by the MCP server, not by ingestors directly.
"""

import csv
from pathlib import Path
from typing import Any

from mcp_server.models import (
    DestinationContract,
    DestinationSchema,
    ExecutionPlan,
    QualityMetrics,
    SourceContract,
    SourceSchema,
    TransformationContract,
)

try:
    import pandas as pd  # noqa: F401

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def detect_file_encoding(file_path: str) -> str:
    """Detect file encoding"""
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]
    file_path_obj = Path(file_path)
    for encoding in encodings:
        try:
            with file_path_obj.open(encoding=encoding) as f:
                f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
        else:
            return encoding
    return "utf-8"  # Default fallback


def detect_delimiter(file_path: str, encoding: str) -> str:
    """Detect CSV delimiter"""
    file_path_obj = Path(file_path)
    with file_path_obj.open(encoding=encoding) as f:
        sample = f.read(1024)
        sniffer = csv.Sniffer()
        try:
            return sniffer.sniff(sample).delimiter
        except csv.Error:
            return ","  # Default fallback


def analyze_numeric_format(sample_value: str) -> dict[str, Any]:
    """Analyze numeric format (e.g., European vs US format)"""
    has_comma_decimal = "," in sample_value.replace(",", "") and "." not in sample_value
    has_thousands_sep = "," in sample_value and "." in sample_value

    return {
        "has_comma_decimal": has_comma_decimal,
        "has_thousands_sep": has_thousands_sep,
        "format": "european" if has_comma_decimal else "us",
    }


def is_numeric(value: str) -> bool:
    """Check if a value is numeric (handles US and European formats)

    Handles:
    - US format: 1234.56
    - European format: 1234,56
    - Negative numbers: -1234.56
    """
    value = value.strip()
    if not value:
        return False

    # Remove leading minus sign for negative numbers
    test_value = value.lstrip("-")

    # Check US format (dot as decimal separator)
    if "." in test_value and "," not in test_value:
        return test_value.replace(".", "").isdigit()

    # Check European format (comma as decimal separator)
    if "," in test_value and "." not in test_value:
        return test_value.replace(",", "").isdigit()

    # Check mixed format (e.g., 1,234.56 or 1.234,56)
    # Remove thousands separator and check decimal part
    if "," in test_value and "." in test_value:
        # Could be US (1,234.56) or European (1.234,56)
        # Try US format first
        us_format = test_value.replace(",", "")
        if us_format.replace(".", "").isdigit():
            return True
        # Try European format
        euro_format = test_value.replace(".", "")
        if euro_format.replace(",", "").isdigit():
            return True

    # Simple integer
    return test_value.isdigit()


def is_date(value: str) -> bool:
    """Check if a value is a date

    Handles:
    - ISO format: YYYY-MM-DD (2025-10-25)
    - Slash format: DD/MM/YYYY (25/10/2025)
    - US format: MM/DD/YYYY (10/25/2025)
    """
    value = value.strip()
    if not value:
        return False

    # Check ISO format: YYYY-MM-DD
    if "-" in value:
        parts = value.split("-")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            # Check if it looks like a valid date (rough check)
            year, month, day = parts
            if len(year) == 4 and 1 <= len(month) <= 2 and 1 <= len(day) <= 2:
                return True

    # Check slash format: DD/MM/YYYY or MM/DD/YYYY
    if "/" in value:
        parts = value.split("/")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            return True

    return False


def detect_data_types(sample_row: list[str]) -> list[str]:
    """Detect data types from sample row"""
    types = []
    for raw_value in sample_row:
        value = raw_value.strip()
        if not value:
            types.append("empty")
        elif is_date(value):
            types.append("date")
        elif is_numeric(value):
            types.append("numeric")
        else:
            types.append("text")
    return types


def detect_data_types_from_multiple_rows(data_rows: list[list[str]], num_columns: int) -> list[str]:
    """Detect data types by scanning multiple rows

    This handles sparse data where early rows may have empty values.
    Returns the most specific non-empty type found for each column.

    Priority: date > numeric > text > empty
    """
    # Initialize with "empty" for all columns
    final_types = ["empty"] * num_columns

    # Scan all available rows
    for row in data_rows:
        # Ensure row has enough columns (pad with empty strings if needed)
        padded_row = row + [""] * (num_columns - len(row))

        # Detect types for this row
        row_types = detect_data_types(padded_row[:num_columns])

        # Update final types with more specific types
        for col_idx, row_type in enumerate(row_types):
            if row_type == "empty":
                # Skip empty values
                continue
            elif final_types[col_idx] == "empty":
                # First non-empty value sets the type
                final_types[col_idx] = row_type
            elif final_types[col_idx] == "text":
                # Text is least specific, keep it
                continue
            elif row_type == "text":
                # If we see text but had numeric/date, downgrade to text
                final_types[col_idx] = "text"
            elif final_types[col_idx] == "numeric" and row_type == "date":
                # Date is more specific than numeric for date-like values
                final_types[col_idx] = "date"
            elif final_types[col_idx] == "date" and row_type == "numeric":
                # Keep date as it's more specific
                continue

    return final_types


def generate_source_analysis(source_path: str) -> dict[str, Any]:
    """Generate automated source data analysis

    Args:
        source_path: Path to source CSV file

    Returns:
        Dictionary with analysis results
    """
    source_file = Path(source_path)
    if not source_file.exists():
        msg = f"Source file not found: {source_path}"
        raise FileNotFoundError(msg)

    encoding = detect_file_encoding(str(source_file))
    delimiter = detect_delimiter(str(source_file), encoding)

    # Read sample rows
    sample_rows = []
    with source_file.open(encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)
        for i, row in enumerate(reader):
            if i >= 10:  # Sample first 10 rows
                break
            sample_rows.append(row)

    if not sample_rows:
        return {
            "file_type": "csv",
            "delimiter": delimiter,
            "encoding": encoding,
            "has_header": False,
            "total_rows": 0,
            "sample_fields": [],
            "data_types": [],
            "issues": ["File is empty"],
        }

    # Detect header (first row might be header)
    has_header = any(not cell.replace(".", "").replace("-", "").isdigit() for cell in sample_rows[0])

    if has_header:
        sample_fields = sample_rows[0]
        data_rows = sample_rows[1:]
    else:
        sample_fields = [f"column_{i + 1}" for i in range(len(sample_rows[0]))]
        data_rows = sample_rows

    # Detect and strip BOM from first field if present
    has_bom = bool(sample_fields and sample_fields[0].startswith("\ufeff"))
    if has_bom:
        sample_fields[0] = sample_fields[0].lstrip("\ufeff")

    # Detect data types by scanning multiple rows for better accuracy
    num_columns = len(sample_fields)
    data_types = detect_data_types_from_multiple_rows(data_rows, num_columns) if data_rows else []

    # Count total rows (approximate)
    with source_file.open(encoding=encoding) as f:
        total_rows = sum(1 for _ in f) - (1 if has_header else 0)

    # Collect issues and warnings about the data source
    issues = []
    if has_bom:
        issues.append(
            "File contains UTF-8 BOM (Byte Order Mark) that was automatically "
            "stripped from field names. Ensure downstream processors handle the "
            "original file encoding correctly."
        )

    return {
        "file_type": "csv",
        "delimiter": delimiter,
        "encoding": encoding,
        "has_header": has_header,
        "total_rows": max(0, total_rows),
        "sample_fields": sample_fields,
        "sample_data": data_rows[:5] if data_rows else [],
        "data_types": data_types,
        "issues": issues,
    }


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
        data_schema=SourceSchema(
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
) -> DestinationContract:
    """Generate a destination contract describing a data destination

    Args:
        destination_id: Unique identifier for destination (e.g., 'dwh_transactions_table')
        schema: Schema definition with fields and types
        config: Optional configuration dictionary

    Returns:
        Destination contract model
    """
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
        data_schema=dest_schema,
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

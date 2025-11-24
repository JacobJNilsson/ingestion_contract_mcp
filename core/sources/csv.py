"""CSV file analysis."""

import csv
from pathlib import Path
from typing import Any

from core.sources.utils import detect_data_types_from_multiple_rows, detect_file_encoding


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


def analyze_csv_file(source_file: Path) -> dict[str, Any]:
    """Analyze CSV file content."""
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

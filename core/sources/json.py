"""JSON file analysis."""

import json
from pathlib import Path
from typing import Any

from core.sources.utils import detect_data_types_from_multiple_rows, detect_file_encoding


def analyze_json_file(source_file: Path) -> dict[str, Any]:
    """Analyze JSON or NDJSON file content."""
    encoding = detect_file_encoding(str(source_file))
    issues = []

    # Try to determine if it's NDJSON or standard JSON
    is_ndjson = False
    data_objects = []
    total_rows = 0

    try:
        with source_file.open(encoding=encoding) as f:
            first_char = f.read(1).strip()
            f.seek(0)

            if first_char == "[":
                # Standard JSON array
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        data_objects = data[:10]  # Sample first 10
                        total_rows = len(data)
                    else:
                        issues.append("JSON root is not a list")
                except json.JSONDecodeError:
                    issues.append("Invalid JSON format")
            else:
                # Assume NDJSON
                is_ndjson = True
                for i, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    if i < 10:
                        try:
                            data_objects.append(json.loads(line))
                        except json.JSONDecodeError:
                            issues.append(f"Invalid JSON on line {i + 1}")
                    total_rows += 1

    except Exception as e:
        issues.append(f"Error reading file: {str(e)}")

    if not data_objects:
        if not issues:
            issues.append("File is empty or contains no valid objects")
        return {
            "file_type": "ndjson" if is_ndjson else "json",
            "encoding": encoding,
            "total_rows": 0,
            "sample_fields": [],
            "data_types": [],
            "issues": issues,
        }

    # Extract fields from all sampled objects to get a complete schema
    all_fields: set[str] = set()
    for obj in data_objects:
        if isinstance(obj, dict):
            all_fields.update(obj.keys())

    sample_fields = sorted(all_fields)

    # Convert objects to rows for type detection
    data_rows = []
    for obj in data_objects:
        if isinstance(obj, dict):
            row = [str(obj.get(field, "")) for field in sample_fields]
            data_rows.append(row)

    # Detect data types
    num_columns = len(sample_fields)
    data_types = detect_data_types_from_multiple_rows(data_rows, num_columns) if data_rows else []

    return {
        "file_type": "ndjson" if is_ndjson else "json",
        "encoding": encoding,
        "has_header": False,  # JSON doesn't have a header row like CSV
        "total_rows": total_rows,
        "sample_fields": sample_fields,
        "sample_data": data_rows[:5],
        "data_types": data_types,
        "issues": issues,
    }

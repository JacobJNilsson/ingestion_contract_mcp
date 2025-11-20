"""Tests for JSON source support."""

import json
import tempfile
from pathlib import Path

from typer.testing import CliRunner

from cli.commands.source import app
from core.contract_generator import generate_source_analysis

runner = CliRunner()


def test_analyze_standard_json() -> None:
    """Test analyzing a standard JSON array file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "data.json"
        data = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False},
        ]
        json_path.write_text(json.dumps(data))

        analysis = generate_source_analysis(str(json_path))

        assert analysis["file_type"] == "json"
        assert analysis["total_rows"] == 2
        assert analysis["sample_fields"] == ["active", "id", "name"]
        # Note: data types might vary based on detection logic, but we expect:
        # active -> text (or bool if supported, currently text/numeric/date/empty) -> actually bools are often detected as text unless specifically handled
        # id -> numeric
        # name -> text
        assert "numeric" in analysis["data_types"]
        assert "text" in analysis["data_types"]


def test_analyze_ndjson() -> None:
    """Test analyzing an NDJSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ndjson_path = Path(tmpdir) / "data.ndjson"
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 80.0},
            {"id": 3, "name": "Charlie", "score": 99.9},
        ]
        content = "\n".join(json.dumps(row) for row in data)
        ndjson_path.write_text(content)

        analysis = generate_source_analysis(str(ndjson_path))

        assert analysis["file_type"] == "ndjson"
        assert analysis["total_rows"] == 3
        assert analysis["sample_fields"] == ["id", "name", "score"]


def test_analyze_empty_json() -> None:
    """Test analyzing an empty JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "empty.json"
        json_path.write_text("[]")

        analysis = generate_source_analysis(str(json_path))

        assert analysis["total_rows"] == 0
        assert analysis["issues"] == ["File is empty or contains no valid objects"]


def test_analyze_invalid_json() -> None:
    """Test analyzing an invalid JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "invalid.json"
        json_path.write_text("{invalid json}")

        analysis = generate_source_analysis(str(json_path))

        assert analysis["total_rows"] == 0
        assert any("Invalid JSON" in issue for issue in analysis["issues"])


def test_cli_source_json() -> None:
    """Test the source json CLI command."""
    with tempfile.TemporaryDirectory() as tmpdir:
        json_path = Path(tmpdir) / "users.json"
        data = [{"id": 1, "username": "user1"}]
        json_path.write_text(json.dumps(data))

        output_path = Path(tmpdir) / "contract.json"

        result = runner.invoke(app, ["json", str(json_path), "--id", "users", "--output", str(output_path)])

        assert result.exit_code == 0
        assert output_path.exists()

        contract = json.loads(output_path.read_text())
        assert contract["source_id"] == "users"
        assert contract["file_format"] == "json"
        assert contract["schema"]["fields"] == ["id", "username"]

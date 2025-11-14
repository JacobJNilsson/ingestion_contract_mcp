"""Tests for CLI commands."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cli.main import app
from mcp_server.models import DestinationContract, SourceContract

runner = CliRunner()


def test_cli_help() -> None:
    """Test CLI help command."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Generate and validate data ingestion contracts" in result.stdout


def test_cli_version() -> None:
    """Test CLI version command."""
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "contract-gen version" in result.stdout


def test_source_csv_help() -> None:
    """Test source csv help command."""
    result = runner.invoke(app, ["source", "csv", "--help"])
    assert result.exit_code == 0
    assert "Generate source contract from CSV file" in result.stdout


def test_source_csv_to_stdout(sample_csv_path: Path) -> None:
    """Test generating source contract to stdout."""
    result = runner.invoke(app, ["source", "csv", str(sample_csv_path), "--id", "test_source"])
    assert result.exit_code == 0
    # Rich output includes ANSI codes, so we just check for key content
    assert "test_source" in result.stdout
    assert "source" in result.stdout


def test_source_csv_to_file(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test generating source contract to file."""
    output_file = tmp_path / "source_contract.json"
    result = runner.invoke(
        app, ["source", "csv", str(sample_csv_path), "--id", "test_source", "--output", str(output_file)]
    )
    assert result.exit_code == 0
    assert output_file.exists()

    # Validate the generated contract
    contract_data = json.loads(output_file.read_text())
    contract = SourceContract.model_validate(contract_data)
    assert contract.source_id == "test_source"
    assert contract.file_format == "csv"


def test_source_csv_pretty_output(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test pretty-printed output."""
    # Write to file to avoid Rich formatting issues
    output_file = tmp_path / "source.json"
    result = runner.invoke(
        app, ["source", "csv", str(sample_csv_path), "--id", "test_source", "--output", str(output_file), "--pretty"]
    )
    assert result.exit_code == 0
    # Pretty output should have indentation
    content = output_file.read_text()
    assert "  " in content


def test_source_csv_with_delimiter(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test specifying delimiter."""
    output_file = tmp_path / "source.json"
    result = runner.invoke(
        app,
        [
            "source",
            "csv",
            str(sample_csv_path),
            "--id",
            "test_source",
            "--delimiter",
            ",",
            "--output",
            str(output_file),
        ],
    )
    assert result.exit_code == 0
    output_data = json.loads(output_file.read_text())
    assert output_data["delimiter"] == ","


def test_source_csv_nonexistent_file() -> None:
    """Test error handling for nonexistent file."""
    result = runner.invoke(app, ["source", "csv", "/nonexistent/file.csv", "--id", "test_source"])
    assert result.exit_code == 2  # Typer returns 2 for parameter errors


def test_destination_csv_help() -> None:
    """Test destination csv help command."""
    result = runner.invoke(app, ["destination", "csv", "--help"])
    assert result.exit_code == 0
    assert "Generate destination contract" in result.stdout


def test_destination_csv_to_stdout(tmp_path: Path) -> None:
    """Test generating destination contract to stdout."""
    output_file = tmp_path / "dest.json"
    result = runner.invoke(app, ["destination", "csv", "--id", "test_destination", "--output", str(output_file)])
    assert result.exit_code == 0
    # Validate the output
    contract_data = json.loads(output_file.read_text())
    assert contract_data["destination_id"] == "test_destination"
    assert contract_data["contract_type"] == "destination"


def test_destination_csv_to_file(tmp_path: Path) -> None:
    """Test generating destination contract to file."""
    output_file = tmp_path / "destination_contract.json"
    result = runner.invoke(app, ["destination", "csv", "--id", "test_destination", "--output", str(output_file)])
    assert result.exit_code == 0
    assert output_file.exists()

    # Validate the generated contract
    contract_data = json.loads(output_file.read_text())
    contract = DestinationContract.model_validate(contract_data)
    assert contract.destination_id == "test_destination"


def test_validate_help() -> None:
    """Test validate help command."""
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    assert "Validate contract" in result.stdout


def test_validate_source_contract(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test validating a source contract."""
    # First generate a contract
    contract_file = tmp_path / "source_contract.json"
    result = runner.invoke(
        app, ["source", "csv", str(sample_csv_path), "--id", "test_source", "--output", str(contract_file)]
    )
    assert result.exit_code == 0

    # Now validate it
    result = runner.invoke(app, ["validate", str(contract_file)])
    assert result.exit_code == 0
    assert "Valid source contract" in result.stdout


def test_validate_directory(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test validating all contracts in a directory."""
    # Generate multiple contracts
    source_file = tmp_path / "source.json"
    dest_file = tmp_path / "destination.json"

    result1 = runner.invoke(
        app, ["source", "csv", str(sample_csv_path), "--id", "test_source", "--output", str(source_file)]
    )
    assert result1.exit_code == 0

    result2 = runner.invoke(app, ["destination", "csv", "--id", "test_dest", "--output", str(dest_file)])
    assert result2.exit_code == 0

    # Validate directory
    result = runner.invoke(app, ["validate", str(tmp_path)])
    assert result.exit_code == 0
    # Should have both contracts validated
    assert "2 contract(s) are valid" in result.stdout


def test_validate_invalid_json(tmp_path: Path) -> None:
    """Test validating invalid JSON."""
    invalid_file = tmp_path / "invalid.json"
    invalid_file.write_text("{invalid json")

    result = runner.invoke(app, ["validate", str(invalid_file)])
    # Should fail (exit code 1)
    assert result.exit_code == 1


def test_validate_invalid_contract(tmp_path: Path) -> None:
    """Test validating contract with missing required fields."""
    invalid_contract = tmp_path / "invalid_contract.json"
    invalid_contract.write_text(json.dumps({"contract_type": "source"}))  # Missing required fields

    result = runner.invoke(app, ["validate", str(invalid_contract)])
    assert result.exit_code == 1


def test_validate_recursive(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test recursive validation."""
    # Create nested directory structure
    subdir = tmp_path / "subdir"
    subdir.mkdir()

    source_file = tmp_path / "source.json"
    nested_file = subdir / "nested.json"

    result1 = runner.invoke(app, ["source", "csv", str(sample_csv_path), "--id", "test1", "--output", str(source_file)])
    result2 = runner.invoke(app, ["source", "csv", str(sample_csv_path), "--id", "test2", "--output", str(nested_file)])
    assert result1.exit_code == 0
    assert result2.exit_code == 0

    # Validate with recursive flag
    result = runner.invoke(app, ["validate", str(tmp_path), "--recursive"])
    assert result.exit_code == 0
    assert "2 contract(s) are valid" in result.stdout


def test_validate_empty_directory(tmp_path: Path) -> None:
    """Test validating empty directory."""
    result = runner.invoke(app, ["validate", str(tmp_path)])
    assert result.exit_code == 1

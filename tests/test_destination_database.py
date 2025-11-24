"""Tests for destination database command."""

import json
from pathlib import Path

from sqlalchemy import create_engine, text
from typer.testing import CliRunner

from cli.commands.destination import app

runner = CliRunner()


def test_destination_database_cli(tmp_path: Path) -> None:
    """Test generating destination contract from a database table via CLI."""
    # Setup SQLite DB
    db_path = tmp_path / "test.db"
    connection_string = f"sqlite:///{db_path}"

    engine = create_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    price REAL
                )
                """
            )
        )
        conn.commit()

    output_file = tmp_path / "contract.json"

    result = runner.invoke(
        app,
        [
            "database",
            connection_string,
            "products",
            "--id",
            "products_dest",
            "--type",
            "sqlite",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0
    assert output_file.exists()

    contract = json.loads(output_file.read_text())
    assert contract["destination_id"] == "products_dest"
    assert contract["schema"]["fields"] == ["id", "name", "price"]
    assert contract["schema"]["types"] == ["INTEGER", "TEXT", "REAL"]
    assert contract["schema"]["constraints"]["id"] == ["PRIMARY KEY"]
    assert contract["schema"]["constraints"]["name"] == ["NOT NULL"]


def test_destination_database_cli_table_not_found(tmp_path: Path) -> None:
    """Test CLI error when table is not found."""
    db_path = tmp_path / "test.db"
    connection_string = f"sqlite:///{db_path}"
    create_engine(connection_string)  # Create empty DB

    result = runner.invoke(
        app,
        [
            "database",
            connection_string,
            "missing_table",
            "--id",
            "dest",
            "--type",
            "sqlite",
        ],
    )

    assert result.exit_code == 1
    # Error message is printed to stderr
    assert "Table 'missing_table' not found" in result.stderr

"""Destination contract generation commands."""

from pathlib import Path

import typer

from cli.output import error_message, output_contract
from mcp_server.contract_generator import generate_destination_contract

app = typer.Typer(help="Generate destination contracts")


@app.command("csv")
def destination_csv(
    destination_id: str = typer.Option(..., "--id", help="Unique identifier for this destination"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format: json or yaml"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate destination contract for CSV output.

    Example:
        contract-gen destination csv --id output_transactions --output contracts/destination.json --pretty
    """
    try:
        # Generate contract
        contract = generate_destination_contract(destination_id=destination_id, config=None)

        # Output
        contract_json = contract.model_dump_json(by_alias=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check the parameters")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate destination contract: {e}")
        raise typer.Exit(1) from e


@app.command("database")
def destination_database(
    connection_string: str = typer.Argument(
        ..., help="Database connection string (e.g. postgresql://user:pass@host/db)"
    ),
    table_name: str = typer.Argument(..., help="Table name to inspect"),
    destination_id: str = typer.Option(..., "--id", help="Unique identifier for this destination"),
    database_type: str = typer.Option(..., "--type", help="Database type: postgresql, mysql, or sqlite"),
    database_schema: str | None = typer.Option(
        None, "--schema", help="Database schema name (optional, defaults to 'public' for PostgreSQL)"
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate destination contract from a database table.

    Example:
        contract-gen destination database postgresql://user:pass@localhost/db my_table --id my_dest --type postgresql
    """
    try:
        # Load config for defaults
        from cli.config import get_output_defaults

        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Generate contract
        contract = generate_destination_contract(
            destination_id=destination_id,
            connection_string=connection_string,
            table_name=table_name,
            database_type=database_type,
            database_schema=database_schema,
        )

        # Output
        contract_json = contract.model_dump_json(by_alias=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check your connection string and table name")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate destination contract: {e}")
        raise typer.Exit(1) from e

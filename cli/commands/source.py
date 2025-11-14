"""Source contract generation commands."""

from pathlib import Path

import typer

from cli.output import error_message, output_contract
from mcp_server.contract_generator import generate_source_contract

app = typer.Typer(help="Generate source contracts from data sources")


@app.command("csv")
def source_csv(
    path: Path = typer.Argument(..., help="Path to CSV file", exists=True, dir_okay=False, resolve_path=True),
    source_id: str = typer.Option(..., "--id", help="Unique identifier for this source"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format: json or yaml"),
    delimiter: str | None = typer.Option(None, "--delimiter", help="CSV delimiter (default: auto-detect)"),
    encoding: str | None = typer.Option(None, "--encoding", help="File encoding (default: auto-detect)"),
    sample_size: int = typer.Option(1000, "--sample-size", help="Number of rows to sample for analysis"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate source contract from CSV file.

    Example:
        contract-gen source csv data/transactions.csv --id transactions --output contracts/source.json --pretty
    """
    try:
        # Build config
        config: dict[str, str | int] = {"sample_size": sample_size}
        if delimiter:
            config["delimiter"] = delimiter
        if encoding:
            config["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_path=str(path.absolute()), source_id=source_id, config=config)

        # Output
        contract_json = contract.model_dump_json(by_alias=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except FileNotFoundError as e:
        error_message(f"File not found: {path}", hint="Check the file path and try again")
        raise typer.Exit(1) from e
    except ValueError as e:
        error_message(str(e), hint="Check the file format and parameters")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate source contract: {e}")
        raise typer.Exit(1) from e

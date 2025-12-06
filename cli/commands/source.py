"""Source contract generation commands."""

from pathlib import Path

import typer

from cli.output import error_message, output_contract
from core.contract_generator import generate_source_contract
from core.sources.database.introspection import extract_table_list

app = typer.Typer(help="Generate source contracts from data sources")
database_app = typer.Typer(help="Generate source contracts from databases")
app.add_typer(database_app, name="database")


@app.command("csv")
def source_csv(
    path: Path = typer.Argument(..., help="Path to CSV file", exists=True, dir_okay=False, resolve_path=True),
    source_id: str = typer.Option(..., "--id", help="Unique identifier for this source"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    delimiter: str | None = typer.Option(None, "--delimiter", help="CSV delimiter (default: auto-detect)"),
    encoding: str | None = typer.Option(None, "--encoding", help="File encoding (default: auto-detect)"),
    sample_size: int | None = typer.Option(None, "--sample-size", help="Number of rows to sample for analysis"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate source contract from CSV file.

    Example:
        contract-gen source csv data/transactions.csv --id transactions --output contracts/source.json --pretty
    """
    try:
        # Load config for defaults
        from cli.config import get_csv_defaults, get_output_defaults

        csv_defaults = get_csv_defaults()
        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if delimiter is None:
            delimiter = csv_defaults.delimiter
        if encoding is None:
            encoding = csv_defaults.encoding
        if sample_size is None:
            sample_size = csv_defaults.sample_size
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Build config
        config_dict: dict[str, str | int] = {"sample_size": sample_size}
        if delimiter:
            config_dict["delimiter"] = delimiter
        if encoding:
            config_dict["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_path=str(path.absolute()), source_id=source_id, config=config_dict)

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


@app.command("json")
def source_json(
    path: Path = typer.Argument(..., help="Path to JSON/NDJSON file", exists=True, dir_okay=False, resolve_path=True),
    source_id: str = typer.Option(..., "--id", help="Unique identifier for this source"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    encoding: str | None = typer.Option(None, "--encoding", help="File encoding (default: auto-detect)"),
    sample_size: int | None = typer.Option(None, "--sample-size", help="Number of rows to sample for analysis"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate source contract from JSON or NDJSON file.

    Example:
        contract-gen source json data/users.json --id users --output contracts/source.json
    """
    try:
        # Load config for defaults
        from cli.config import get_json_defaults, get_output_defaults

        json_defaults = get_json_defaults()
        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if encoding is None:
            encoding = json_defaults.encoding
        if sample_size is None:
            sample_size = json_defaults.sample_size
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Build config
        config_dict: dict[str, str | int] = {"sample_size": sample_size}
        if encoding:
            config_dict["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_path=str(path.absolute()), source_id=source_id, config=config_dict)

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


@database_app.command("list")
def source_database_list(
    connection_string: str = typer.Argument(
        ..., help="Database connection string (e.g. postgresql://user:pass@host/db)"
    ),
    database_type: str = typer.Option(..., "--type", help="Database type: postgresql, mysql, or sqlite"),
    schema: str | None = typer.Option(
        None, "--schema", help="Database schema name (optional, defaults to 'public' for PostgreSQL)"
    ),
    with_fields: bool = typer.Option(False, "--with-fields", help="Include column details"),
    output_format: str = typer.Option("text", "--format", "-f", help="Output format: text or json"),
) -> None:
    """List tables in a database.

    Example:
        contract-gen source database list postgresql://user:pass@host/db --type postgresql
    """
    try:
        tables = extract_table_list(
            connection_string=connection_string,
            database_type=database_type,
            schema=schema,
            with_fields=with_fields,
        )

        if output_format == "json":
            import json

            typer.echo(json.dumps(tables, indent=2))
        else:
            if not tables:
                typer.echo("No tables found.")
                return

            schema_msg = f" in schema '{schema}'" if schema else ""
            typer.echo(f"Tables{schema_msg} ({len(tables)} total):")

            for table in tables:
                col_count = table.get("column_count", 0)
                typer.echo(f"  {table['name']} ({col_count} columns)")

                if with_fields and "columns" in table:
                    for col in table["columns"]:
                        pk = ", PRIMARY KEY" if False else ""  # Logic for PK display if available
                        nullable = ", NOT NULL" if not col["nullable"] else ""
                        typer.echo(f"    - {col['name']} ({col['type']}{nullable}{pk})")
                    typer.echo("")

    except Exception as e:
        error_message(f"Failed to list tables: {e}")
        raise typer.Exit(1) from e

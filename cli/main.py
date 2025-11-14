"""Main entry point for contract-gen CLI tool."""

import typer

from cli import __version__
from cli.commands import destination, source
from cli.commands.validate import validate

# Create main app
app = typer.Typer(
    name="contract-gen",
    help="Generate and validate data ingestion contracts",
    no_args_is_help=True,
    add_completion=False,
)

# Add subcommands
app.add_typer(source.app, name="source")
app.add_typer(destination.app, name="destination")
app.command(name="validate")(validate)


def version_callback(show_version: bool) -> None:
    """Show version and exit.

    Args:
        show_version: Whether to show version
    """
    if show_version:
        typer.echo(f"contract-gen version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version and exit",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """Generate and validate data ingestion contracts.

    Examples:

        # Generate source contract from CSV
        contract-gen source csv data/transactions.csv --id transactions --output contracts/source.json

        # Generate destination contract
        contract-gen destination csv --id output_transactions --output contracts/destination.json

        # Validate contracts
        contract-gen validate contracts/source.json
        contract-gen validate contracts/ --recursive

    For detailed help on each command:
        contract-gen source --help
        contract-gen destination --help
        contract-gen validate --help
    """
    pass


if __name__ == "__main__":
    app()

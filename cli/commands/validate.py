"""Contract validation commands."""

from pathlib import Path

import typer
from pydantic import ValidationError

from cli.output import error_message, success_message
from core.models import DestinationContract, SourceContract, TransformationContract


def validate_contract_file(contract_path: Path) -> bool:
    """Validate a single contract file.

    Args:
        contract_path: Path to contract file

    Returns:
        True if valid, False otherwise
    """
    try:
        # Read contract
        contract_json = contract_path.read_text(encoding="utf-8")

        # Try to parse as different contract types
        import json

        contract_data = json.loads(contract_json)
        contract_type = contract_data.get("contract_type")

        match contract_type:
            case "source":
                SourceContract.model_validate_json(contract_json)
            case "destination":
                DestinationContract.model_validate_json(contract_json)
            case "transformation":
                TransformationContract.model_validate_json(contract_json)
            case _:
                error_message(
                    f"Unknown contract type: {contract_type}",
                    hint="Contract must have contract_type: source, destination, or transformation",
                )
                return False

        success_message(f"Valid {contract_type} contract: {contract_path.name}")
        return True

    except ValidationError as e:
        error_message(f"Validation failed for {contract_path.name}:")
        for error in e.errors():
            location = " -> ".join(str(loc) for loc in error["loc"])
            typer.secho(f"  • {location}: {error['msg']}", fg=typer.colors.RED, err=True)
        return False
    except json.JSONDecodeError as e:
        error_message(f"Invalid JSON in {contract_path.name}: {e}", hint="Check the JSON syntax")
        return False
    except Exception as e:
        error_message(f"Failed to validate {contract_path.name}: {e}")
        return False


def validate(
    path: Path = typer.Argument(
        ...,
        help="Path to contract file or directory",
        exists=True,
        resolve_path=True,
    ),
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Recursively validate all contracts in directory"),
) -> None:
    """Validate contract files.

    Examples:
        # Validate single contract
        contract-gen validate contracts/source.json

        # Validate all contracts in directory
        contract-gen validate contracts/ --recursive
    """
    try:
        if path.is_file():
            # Validate single file
            if not validate_contract_file(path):
                raise typer.Exit(1)
        elif path.is_dir():
            # Validate directory
            pattern = "**/*.json" if recursive else "*.json"
            contract_files = list(path.glob(pattern))

            if not contract_files:
                error_message(f"No contract files found in {path}", hint="Use --recursive to search subdirectories")
                raise typer.Exit(1)

            typer.echo(f"Validating {len(contract_files)} contract(s)...\n")

            valid_count = 0
            invalid_count = 0

            for contract_file in contract_files:
                if validate_contract_file(contract_file):
                    valid_count += 1
                else:
                    invalid_count += 1
                typer.echo()  # Blank line between files

            # Summary
            typer.secho("─" * 50, dim=True)
            if invalid_count == 0:
                success_message(f"All {valid_count} contract(s) are valid")
            else:
                error_message(f"{invalid_count} of {valid_count + invalid_count} contract(s) failed validation")
                raise typer.Exit(1)
        else:
            error_message(f"Path is neither file nor directory: {path}")
            raise typer.Exit(1)

    except typer.Exit:
        raise
    except Exception as e:
        error_message(f"Validation failed: {e}")
        raise typer.Exit(1) from e

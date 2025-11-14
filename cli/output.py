"""Output formatting utilities for CLI."""

import json
from pathlib import Path
from typing import Any

import typer
import yaml
from rich.console import Console
from rich.syntax import Syntax

console = Console()


def format_json(data: dict[str, Any], pretty: bool = False) -> str:
    """Format data as JSON.

    Args:
        data: Data to format
        pretty: Whether to pretty-print with indentation

    Returns:
        JSON string
    """
    if pretty:
        return json.dumps(data, indent=2, ensure_ascii=False)
    return json.dumps(data, ensure_ascii=False)


def format_yaml(data: dict[str, Any]) -> str:
    """Format data as YAML.

    Args:
        data: Data to format

    Returns:
        YAML string
    """
    result = yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
    return str(result) if result is not None else ""


def output_contract(
    contract_json: str,
    output_path: Path | None = None,
    output_format: str = "json",
    pretty: bool = False,
) -> None:
    """Output contract to file or stdout.

    Args:
        contract_json: Contract as JSON string
        output_path: Output file path (None = stdout)
        output_format: Output format (json or yaml)
        pretty: Whether to pretty-print JSON
    """
    # Parse JSON
    contract_data = json.loads(contract_json)

    # Format based on output format
    match output_format:
        case "yaml":
            output_str = format_yaml(contract_data)
        case "json":
            output_str = format_json(contract_data, pretty=pretty)
        case _:
            typer.secho(f"✗ Error: Unknown output format: {output_format}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)

    # Write to file or stdout
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_str, encoding="utf-8")
        typer.secho(f"✓ Contract written to {output_path}", fg=typer.colors.GREEN)
    else:
        # Pretty print to terminal with syntax highlighting
        match output_format:
            case "json":
                syntax = Syntax(output_str, "json", theme="monokai", line_numbers=False)
            case _:
                syntax = Syntax(output_str, "yaml", theme="monokai", line_numbers=False)
        console.print(syntax)


def error_message(message: str, hint: str | None = None) -> None:
    """Print error message and exit.

    Args:
        message: Error message
        hint: Optional hint for user
    """
    typer.secho(f"✗ Error: {message}", fg=typer.colors.RED, err=True)
    if hint:
        typer.secho(f"  Hint: {hint}", fg=typer.colors.YELLOW, err=True)


def success_message(message: str) -> None:
    """Print success message.

    Args:
        message: Success message
    """
    typer.secho(f"✓ {message}", fg=typer.colors.GREEN)

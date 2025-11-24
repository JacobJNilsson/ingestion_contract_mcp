"""Tests for destination API command with OpenAPI schemas."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cli.commands.destination import app

runner = CliRunner()


def test_destination_api_cli_with_openapi_schema(tmp_path: Path) -> None:
    """Test generating destination contract from OpenAPI schema file."""
    # Create a sample OpenAPI schema
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {
            "/users": {
                "post": {
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["name", "email"],
                                    "properties": {
                                        "name": {"type": "string", "minLength": 1, "maxLength": 100},
                                        "email": {"type": "string", "format": "email"},
                                        "age": {"type": "integer", "minimum": 0, "maximum": 150},
                                        "active": {"type": "boolean"},
                                    },
                                }
                            }
                        },
                    }
                }
            }
        },
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    output_file = tmp_path / "contract.json"

    result = runner.invoke(
        app,
        [
            "api",
            str(schema_file),
            "/users",
            "--id",
            "users_api",
            "--method",
            "POST",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, f"Command failed: {result.stderr}"
    assert output_file.exists()

    contract = json.loads(output_file.read_text())
    assert contract["destination_id"] == "users_api"
    assert contract["metadata"]["destination_type"] == "api"
    assert contract["metadata"]["endpoint"] == "/users"
    assert contract["metadata"]["http_method"] == "POST"

    # Check schema extraction
    assert "name" in contract["schema"]["fields"]
    assert "email" in contract["schema"]["fields"]
    assert "age" in contract["schema"]["fields"]
    assert "active" in contract["schema"]["fields"]

    # Check types
    assert contract["schema"]["types"][contract["schema"]["fields"].index("name")] == "text"
    assert contract["schema"]["types"][contract["schema"]["fields"].index("email")] == "email"
    assert contract["schema"]["types"][contract["schema"]["fields"].index("age")] == "integer"
    assert contract["schema"]["types"][contract["schema"]["fields"].index("active")] == "boolean"

    # Check constraints
    assert "REQUIRED" in contract["schema"]["constraints"]["name"]
    assert "REQUIRED" in contract["schema"]["constraints"]["email"]
    assert any("MIN_LENGTH: 1" in c for c in contract["schema"]["constraints"]["name"])
    assert any("MIN: 0" in c for c in contract["schema"]["constraints"]["age"])


def test_destination_api_cli_with_yaml_schema(tmp_path: Path) -> None:
    """Test generating destination contract from YAML OpenAPI schema."""
    # Create a sample OpenAPI schema in YAML
    yaml_content = """
openapi: 3.0.0
info:
  title: API
  version: 1.0.0
paths:
  /data:
    post:
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                id:
                  type: string
                  format: uuid
                value:
                  type: number
"""

    schema_file = tmp_path / "openapi.yaml"
    schema_file.write_text(yaml_content)

    output_file = tmp_path / "contract.json"

    result = runner.invoke(
        app,
        [
            "api",
            str(schema_file),
            "/data",
            "--id",
            "data_api",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, f"Command failed: {result.stderr}"
    assert output_file.exists()

    contract = json.loads(output_file.read_text())
    assert contract["destination_id"] == "data_api"
    assert "id" in contract["schema"]["fields"]
    assert "value" in contract["schema"]["fields"]

    # Check UUID format mapping
    assert contract["schema"]["types"][contract["schema"]["fields"].index("id")] == "uuid"
    assert contract["schema"]["types"][contract["schema"]["fields"].index("value")] == "float"


def test_destination_api_cli_endpoint_not_found(tmp_path: Path) -> None:
    """Test error handling when endpoint is not found in schema."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {"/users": {"get": {}}},
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(
        app,
        [
            "api",
            str(schema_file),
            "/missing",
            "--id",
            "test_api",
        ],
    )

    assert result.exit_code == 1
    assert "not found in schema" in result.stderr


def test_destination_api_cli_method_not_found(tmp_path: Path) -> None:
    """Test error handling when method is not found for endpoint."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {"/users": {"get": {}}},
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(
        app,
        [
            "api",
            str(schema_file),
            "/users",
            "--id",
            "test_api",
            "--method",
            "POST",
        ],
    )

    assert result.exit_code == 1
    assert "not found for endpoint" in result.stderr

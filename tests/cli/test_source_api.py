import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from cli.main import app

runner = CliRunner()


@pytest.fixture
def openapi_3_schema(tmp_path: Path) -> Path:
    """Create a sample OpenAPI 3.0 schema file."""
    schema = {
        "openapi": "3.0.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/users": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Successful response",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string", "format": "uuid"},
                                            "name": {"type": "string"},
                                            "email": {"type": "string", "format": "email"},
                                            "age": {"type": "integer"},
                                        },
                                        "required": ["id", "name"],
                                    }
                                }
                            },
                        }
                    }
                }
            }
        },
    }
    schema_file = tmp_path / "openapi.json"
    with open(schema_file, "w") as f:
        json.dump(schema, f)
    return schema_file


@pytest.fixture
def swagger_2_schema(tmp_path: Path) -> Path:
    """Create a sample Swagger 2.0 schema file."""
    schema = {
        "swagger": "2.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/products": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "Successful response",
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer"},
                                    "name": {"type": "string"},
                                    "price": {"type": "number"},
                                },
                                "required": ["id", "name"],
                            },
                        }
                    }
                }
            }
        },
    }
    schema_file = tmp_path / "swagger.yaml"
    with open(schema_file, "w") as f:
        yaml.dump(schema, f)
    return schema_file


def test_source_api_openapi_3(openapi_3_schema: Path, tmp_path: Path) -> None:
    """Test generating source contract from OpenAPI 3.0 schema."""
    output_file = tmp_path / "contract.json"
    result = runner.invoke(
        app,
        [
            "source",
            "api",
            str(openapi_3_schema),
            "/users",
            "--id",
            "users_api",
            "--method",
            "GET",
            "--output",
            str(output_file),
        ],
    )
    print(f"Stdout: {result.stdout}")
    print(f"Stderr: {result.stderr}")
    assert result.exit_code == 0

    contract = json.loads(output_file.read_text())
    assert contract["source_id"] == "users_api"
    assert contract["metadata"]["source_type"] == "api"
    assert contract["metadata"]["endpoint"] == "/users"
    assert contract["metadata"]["http_method"] == "GET"

    fields = contract["schema"]["fields"]
    types = contract["schema"]["data_types"]

    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "age" in fields

    assert types[fields.index("id")] == "text"  # uuid -> text
    assert types[fields.index("name")] == "text"
    assert types[fields.index("email")] == "email"
    assert types[fields.index("age")] == "integer"


def test_source_api_swagger_2(swagger_2_schema: Path, tmp_path: Path) -> None:
    """Test generating source contract from Swagger 2.0 schema."""
    output_file = tmp_path / "contract.json"
    result = runner.invoke(
        app,
        [
            "source",
            "api",
            str(swagger_2_schema),
            "/products",
            "--id",
            "products_api",
            "--method",
            "GET",
            "--output",
            str(output_file),
        ],
    )
    assert result.exit_code == 0

    contract = json.loads(output_file.read_text())
    assert contract["source_id"] == "products_api"

    fields = contract["schema"]["fields"]
    types = contract["schema"]["data_types"]

    assert "id" in fields
    assert "name" in fields
    assert "price" in fields

    assert types[fields.index("id")] == "integer"
    assert types[fields.index("name")] == "text"
    assert types[fields.index("price")] == "float"


def test_source_api_missing_endpoint(openapi_3_schema: Path) -> None:
    """Test error when endpoint is missing in schema."""
    result = runner.invoke(
        app,
        [
            "source",
            "api",
            str(openapi_3_schema),
            "/nonexistent",
            "--id",
            "test",
        ],
    )
    assert result.exit_code == 1
    assert "Endpoint '/nonexistent' not found" in result.stderr


def test_source_api_missing_method(openapi_3_schema: Path) -> None:
    """Test error when method is missing for endpoint."""
    result = runner.invoke(
        app,
        [
            "source",
            "api",
            str(openapi_3_schema),
            "/users",
            "--id",
            "test",
            "--method",
            "POST",  # Schema only has GET
        ],
    )
    assert result.exit_code == 1
    assert "Method 'POST' not found" in result.stderr

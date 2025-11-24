"""Tests for CLI config functionality."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from cli.config import (
    Config,
    CSVDefaults,
    Defaults,
    OutputDefaults,
    get_config_path,
    get_connection,
    get_csv_defaults,
    get_output_defaults,
    init_config,
    load_config,
    resolve_connection,
    save_config,
    validate_config,
)


def test_default_config_path() -> None:
    """Test that default config path is in home directory."""
    assert get_config_path() == Path.home() / ".contract-gen.yaml"


def test_custom_config_path() -> None:
    """Test that custom config path is used when env var is set."""
    with patch.dict("os.environ", {"CONTRACT_GEN_CONFIG": "/tmp/custom.yaml"}):
        assert get_config_path() == Path("/tmp/custom.yaml")


def test_load_config_missing_file() -> None:
    """Test loading config when file doesn't exist returns defaults."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "nonexistent.yaml"
        with patch("cli.config.get_config_path", return_value=config_path):
            config = load_config()
            assert isinstance(config, Config)
            assert config.version == "1.0"
            assert config.connections == {}
            assert config.defaults.csv.delimiter == ","


def test_save_and_load_config() -> None:
    """Test saving and loading config file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "test.yaml"
        test_config = Config(connections={"test_db": "postgresql://localhost/test"})

        with patch("cli.config.get_config_path", return_value=config_path):
            save_config(test_config)
            loaded_config = load_config()

            assert loaded_config.version == "1.0"
            assert "test_db" in loaded_config.connections
            assert loaded_config.connections["test_db"] == "postgresql://localhost/test"


def test_init_config() -> None:
    """Test initializing config file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "init.yaml"

        with patch("cli.config.get_config_path", return_value=config_path):
            result_path = init_config()

            assert result_path == config_path
            assert config_path.exists()

            # Check content
            with config_path.open() as f:
                content = yaml.safe_load(f)
                assert content["version"] == "1.0"
                assert "connections" in content
                assert "defaults" in content


def test_init_config_exists_without_force() -> None:
    """Test initializing config file when it already exists without force."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "existing.yaml"
        config_path.write_text("existing: content")

        with patch("cli.config.get_config_path", return_value=config_path), pytest.raises(FileExistsError):
            init_config(force=False)


def test_init_config_exists_with_force() -> None:
    """Test initializing config file when it already exists with force."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "existing.yaml"
        config_path.write_text("existing: content")

        with patch("cli.config.get_config_path", return_value=config_path):
            result_path = init_config(force=True)
            assert result_path == config_path

            # Should be overwritten with default config
            with config_path.open() as f:
                content = yaml.safe_load(f)
                assert content["version"] == "1.0"


def test_get_connection() -> None:
    """Test getting named connection from config."""
    config = Config(
        connections={
            "prod_db": "postgresql://localhost:5432/prod",
            "staging_db": "postgresql://localhost:5432/staging",
        }
    )

    assert get_connection("prod_db", config) == "postgresql://localhost:5432/prod"
    assert get_connection("staging_db", config) == "postgresql://localhost:5432/staging"


def test_get_connection_missing() -> None:
    """Test getting missing connection raises error."""
    config = Config()

    with pytest.raises(KeyError, match="Connection 'missing' not found"):
        get_connection("missing", config)


def test_resolve_connection_with_reference() -> None:
    """Test resolving connection string with @ reference."""
    config = Config(connections={"prod_db": "postgresql://localhost:5432/prod"})

    assert resolve_connection("@prod_db", config) == "postgresql://localhost:5432/prod"


def test_resolve_connection_without_reference() -> None:
    """Test resolving connection string without @ reference."""
    assert resolve_connection("postgresql://localhost:5432/prod") == "postgresql://localhost:5432/prod"


def test_get_csv_defaults() -> None:
    """Test getting CSV defaults from config."""
    config = Config(defaults=Defaults(csv=CSVDefaults(delimiter="|", encoding="latin-1", sample_size=500)))

    csv_defaults = get_csv_defaults(config)
    assert csv_defaults.delimiter == "|"
    assert csv_defaults.encoding == "latin-1"
    assert csv_defaults.sample_size == 500


def test_get_output_defaults() -> None:
    """Test getting output defaults from config."""
    config = Config(defaults=Defaults(output=OutputDefaults(format="yaml", pretty=True)))

    output_defaults = get_output_defaults(config)
    assert output_defaults.format == "yaml"
    assert output_defaults.pretty is True


def test_get_csv_defaults_uses_config_defaults() -> None:
    """Test getting CSV defaults without explicit config."""
    csv_defaults = get_csv_defaults()
    assert csv_defaults.delimiter == ","
    assert csv_defaults.encoding == "utf-8"
    assert csv_defaults.sample_size == 1000


def test_get_output_defaults_uses_config_defaults() -> None:
    """Test getting output defaults without explicit config."""
    output_defaults = get_output_defaults()
    assert output_defaults.format == "json"
    assert output_defaults.pretty is False


def test_validate_config_valid() -> None:
    """Test validating a valid config."""
    config = Config(
        connections={"db": "postgresql://localhost/db"},
        defaults=Defaults(csv=CSVDefaults(sample_size=1000), output=OutputDefaults(format="json", pretty=True)),
    )

    errors = validate_config(config)
    assert errors == []


def test_validate_config_invalid_output_format() -> None:
    """Test validating config with invalid output format."""
    config = Config(defaults=Defaults(output=OutputDefaults(format="xml")))

    errors = validate_config(config)
    assert "'defaults.output.format' must be 'json' or 'yaml'" in errors


def test_load_config_invalid_yaml() -> None:
    """Test loading config with invalid YAML."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "invalid.yaml"
        config_path.write_text("invalid: yaml: content: [")

        with (
            patch("cli.config.get_config_path", return_value=config_path),
            pytest.raises(ValueError, match="Invalid YAML"),
        ):
            load_config()


def test_pydantic_validation_on_load() -> None:
    """Test that Pydantic validates on load."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "invalid_type.yaml"
        # Write config with invalid sample_size type
        config_path.write_text("version: '1.0'\ndefaults:\n  csv:\n    sample_size: 'not_an_int'\n")

        with patch("cli.config.get_config_path", return_value=config_path), pytest.raises(ValueError):
            load_config()

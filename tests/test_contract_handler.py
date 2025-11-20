"""Tests for contract_handler module"""

import json
from pathlib import Path

import pytest

from core.models import DestinationContract, SourceContract, TransformationContract
from mcp_server.handlers import ContractHandler, load_contract, save_contract, validate_contract


class TestContractFileOperations:
    """Tests for contract file loading and saving"""

    def test_save_and_load_contract(self, tmp_path: Path, sample_source_contract: SourceContract) -> None:
        """Test saving and loading a contract"""
        contract_path = tmp_path / "test_contract.json"

        # Save contract
        success = save_contract(sample_source_contract, str(contract_path))
        assert success is True
        assert contract_path.exists()

        # Load contract
        loaded = load_contract(str(contract_path))
        assert loaded is not None
        assert loaded.contract_type == "source"
        assert isinstance(loaded, SourceContract)
        assert loaded.source_id == "test_source"

    def test_load_nonexistent_contract(self) -> None:
        """Test loading a nonexistent contract returns None"""
        result = load_contract("/nonexistent/contract.json")
        assert result is None

    def test_load_invalid_json(self, tmp_path: Path) -> None:
        """Test loading invalid JSON returns None"""
        contract_path = tmp_path / "invalid.json"
        contract_path.write_text("{invalid json")

        result = load_contract(str(contract_path))
        assert result is None

    def test_save_contract_creates_directories(self, tmp_path: Path, sample_source_contract: SourceContract) -> None:
        """Test that save_contract creates parent directories"""
        contract_path = tmp_path / "nested" / "dir" / "contract.json"

        success = save_contract(sample_source_contract, str(contract_path))
        assert success is True
        assert contract_path.exists()


class TestContractValidation:
    """Tests for contract validation"""

    def test_validate_source_contract_valid(self, sample_source_contract: SourceContract) -> None:
        """Test validation of a valid source contract"""
        issues = validate_contract(sample_source_contract)
        assert issues == []

    def test_validate_destination_contract_valid(self, sample_destination_contract: DestinationContract) -> None:
        """Test validation of a valid destination contract"""
        issues = validate_contract(sample_destination_contract)
        assert issues == []

    def test_validate_transformation_contract_valid(
        self, sample_transformation_contract: TransformationContract
    ) -> None:
        """Test validation of a valid transformation contract"""
        issues = validate_contract(sample_transformation_contract)
        assert issues == []

    # Note: With Pydantic, validation happens at construction time
    # Invalid contracts cannot be created, so we test validation errors differently

    def test_pydantic_validates_required_fields(self) -> None:
        """Test that Pydantic catches missing required fields at construction"""
        from pydantic import ValidationError

        # Missing required field
        with pytest.raises(ValidationError):
            SourceContract(
                # source_id is missing - should raise ValidationError
                source_path="/path/to/file.csv",
                file_format="csv",
            )

    def test_pydantic_validates_field_types(self) -> None:
        """Test that Pydantic catches invalid field types"""
        from pydantic import ValidationError

        from core.models import QualityMetrics, SourceSchema

        # Invalid type for total_rows (should be int >= 0)
        with pytest.raises(ValidationError):
            SourceContract(
                source_id="test",
                source_path="/path/to/file.csv",
                file_format="csv",
                data_schema=SourceSchema(fields=[], data_types=[]),
                quality_metrics=QualityMetrics(total_rows=-1),  # Invalid: negative
            )


class TestContractHandler:
    """Tests for ContractHandler class"""

    @pytest.fixture
    def handler(self) -> ContractHandler:
        """Create a ContractHandler instance"""
        return ContractHandler()

    def test_generate_source_contract(self, handler: ContractHandler, sample_csv_path: Path) -> None:
        """Test source contract generation via handler"""
        result = handler.generate_source_contract(source_path=str(sample_csv_path), source_id="test_source")

        # Result should be valid JSON
        contract = json.loads(result)
        assert contract["contract_type"] == "source"
        assert contract["source_id"] == "test_source"
        assert contract["file_format"] == "csv"

    def test_generate_source_contract_relative_path(self, handler: ContractHandler) -> None:
        """Test source contract generation with relative path returns error"""
        result = handler.generate_source_contract(source_path="relative/path.csv", source_id="test_source")

        error = json.loads(result)
        assert "error" in error
        assert "absolute path" in error["error"]

    def test_generate_source_contract_missing_file(self, handler: ContractHandler) -> None:
        """Test source contract generation with missing file returns error"""
        result = handler.generate_source_contract(source_path="/nonexistent/file.csv", source_id="test_source")

        error = json.loads(result)
        assert "error" in error
        assert "not found" in error["error"]

    def test_generate_destination_contract(self, handler: ContractHandler) -> None:
        """Test destination contract generation via handler"""
        schema: dict[str, object] = {"fields": ["id", "name"], "types": ["int", "string"]}
        result = handler.generate_destination_contract(destination_id="test_dest", schema=schema)

        contract = json.loads(result)
        assert contract["contract_type"] == "destination"
        assert contract["destination_id"] == "test_dest"
        assert contract["schema"]["fields"] == ["id", "name"]
        assert contract["schema"]["types"] == ["int", "string"]

    def test_generate_transformation_contract(self, handler: ContractHandler) -> None:
        """Test transformation contract generation via handler"""
        result = handler.generate_transformation_contract(
            transformation_id="test_transform", source_ref="source_1", destination_ref="dest_1"
        )

        contract = json.loads(result)
        assert contract["contract_type"] == "transformation"
        assert contract["transformation_id"] == "test_transform"
        assert contract["source_ref"] == "source_1"
        assert contract["destination_ref"] == "dest_1"

    def test_analyze_source(self, handler: ContractHandler, sample_csv_path: Path) -> None:
        """Test source analysis via handler"""
        result = handler.analyze_source(source_path=str(sample_csv_path))

        analysis = json.loads(result)
        assert analysis["file_type"] == "csv"
        assert analysis["total_rows"] == 11  # Includes blank line at end of file
        assert "sample_fields" in analysis

    def test_analyze_source_relative_path(self, handler: ContractHandler) -> None:
        """Test source analysis with relative path returns error"""
        result = handler.analyze_source(source_path="relative/path.csv")

        error = json.loads(result)
        assert "error" in error
        assert "absolute path" in error["error"]

    def test_validate_contract_valid(self, handler: ContractHandler, saved_source_contract: Path) -> None:
        """Test contract validation via handler for valid contract"""
        result = handler.validate_contract(contract_path=str(saved_source_contract))

        validation = json.loads(result)
        assert validation["valid"] is True
        assert validation["contract_type"] == "source"
        assert validation["source_id"] == "test_source"
        assert "issues" not in validation

    def test_validate_contract_invalid(self, handler: ContractHandler, tmp_path: Path) -> None:
        """Test contract validation via handler for invalid contract"""
        # Create invalid contract (missing required fields)
        # With Pydantic, this will fail to load (not just validate)
        invalid_contract = {"contract_version": "1.0"}
        contract_path = tmp_path / "invalid.json"
        with contract_path.open("w") as f:
            json.dump(invalid_contract, f)

        result = handler.validate_contract(contract_path=str(contract_path))

        # When contract fails to load/validate, an error is returned
        validation = json.loads(result)
        assert "error" in validation
        assert "Failed to load or validate" in validation["error"]

    def test_validate_contract_relative_path(self, handler: ContractHandler) -> None:
        """Test contract validation with relative path returns error"""
        result = handler.validate_contract(contract_path="relative/contract.json")

        error = json.loads(result)
        assert "error" in error
        assert "absolute path" in error["error"]

    def test_validate_contract_missing_file(self, handler: ContractHandler) -> None:
        """Test contract validation with missing file returns error"""
        result = handler.validate_contract(contract_path="/nonexistent/contract.json")

        error = json.loads(result)
        assert "error" in error
        assert "not found" in error["error"]

    def test_validate_contract_malformed_json(self, handler: ContractHandler, tmp_path: Path) -> None:
        """Test contract validation with malformed JSON returns error"""
        contract_path = tmp_path / "malformed.json"
        contract_path.write_text("{invalid json")

        result = handler.validate_contract(contract_path=str(contract_path))

        error = json.loads(result)
        assert "error" in error
        assert "Failed to load" in error["error"]

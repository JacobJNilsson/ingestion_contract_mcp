"""Tests for MCP server"""

from pathlib import Path

from mcp_server.server import app, contract_handler


class TestServerInitialization:
    """Tests for server initialization"""

    def test_server_name(self) -> None:
        """Test that server is initialized with correct name"""
        assert app.name == "ingestion-contract-generator"

    def test_handler_initialized(self) -> None:
        """Test that contract handler is initialized"""
        assert contract_handler is not None


class TestHandlerIntegration:
    """Tests for handler integration (not MCP protocol)"""

    def test_handler_has_all_methods(self) -> None:
        """Test that handler has all required methods"""
        assert hasattr(contract_handler, "generate_source_contract")
        assert hasattr(contract_handler, "generate_destination_contract")
        assert hasattr(contract_handler, "generate_transformation_contract")
        assert hasattr(contract_handler, "analyze_source")
        assert hasattr(contract_handler, "validate_contract")

    def test_handler_methods_are_callable(self) -> None:
        """Test that all handler methods are callable"""
        assert callable(contract_handler.generate_source_contract)
        assert callable(contract_handler.generate_destination_contract)
        assert callable(contract_handler.generate_transformation_contract)
        assert callable(contract_handler.analyze_source)
        assert callable(contract_handler.validate_contract)

    def test_generate_source_contract_returns_json(self, sample_csv_path: Path) -> None:
        """Test that generate_source_contract returns valid JSON"""
        import json

        result = contract_handler.generate_source_contract(source_path=str(sample_csv_path), source_id="test_source")
        # Should be valid JSON
        contract = json.loads(result)
        assert "contract_type" in contract

    def test_generate_destination_contract_returns_json(self) -> None:
        """Test that generate_destination_contract returns valid JSON"""
        import json

        result = contract_handler.generate_destination_contract(destination_id="test_dest")
        # Should be valid JSON
        contract = json.loads(result)
        assert "contract_type" in contract

    def test_generate_transformation_contract_returns_json(self) -> None:
        """Test that generate_transformation_contract returns valid JSON"""
        import json

        result = contract_handler.generate_transformation_contract(
            transformation_id="test_transform", source_ref="source_1", destination_ref="dest_1"
        )
        # Should be valid JSON
        contract = json.loads(result)
        assert "contract_type" in contract

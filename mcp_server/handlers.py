"""Contract generation and validation handlers"""

import json
from pathlib import Path

from pydantic import ValidationError

from core.contract_generator import (
    generate_destination_contract,
    generate_source_analysis,
    generate_source_contract,
    generate_transformation_contract,
)
from core.models import Contract, DestinationContract, SourceContract, TransformationContract
from core.sources.database import (
    generate_database_multi_source_contracts,
    generate_database_source_contract,
    list_database_tables,
    sanitize_connection_string,
)


def load_contract(contract_path: str) -> Contract | None:
    """Load and validate a contract JSON file

    Args:
        contract_path: Path to the contract JSON file

    Returns:
        Contract model or None if loading/validation fails
    """
    try:
        with Path(contract_path).open(encoding="utf-8") as file:
            data = json.load(file)
            if not isinstance(data, dict):
                return None

            # Determine contract type and parse accordingly
            contract_type = data.get("contract_type")
            if contract_type == "source":
                return SourceContract.model_validate(data)
            elif contract_type == "destination":
                return DestinationContract.model_validate(data)
            elif contract_type == "transformation":
                return TransformationContract.model_validate(data)
            else:
                return None
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValidationError):
        return None


def save_contract(contract: Contract, contract_path: str) -> bool:
    """Save a contract to a JSON file

    Args:
        contract: Contract model to save
        contract_path: Path where to save the contract

    Returns:
        True if successful, False otherwise
    """
    try:
        path = Path(contract_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as file:
            file.write(contract.model_dump_json(indent=2, exclude_none=False, by_alias=True))
            return True
    except (OSError, TypeError):
        return False


def validate_contract(contract: Contract) -> list[str]:
    """Validate a contract model

    Args:
        contract: Contract model to validate

    Returns:
        List of validation issue messages (empty if valid, since Pydantic validates on construction)
    """
    # With Pydantic, if we have a Contract object, it's already validated
    # We could add additional business logic validation here if needed
    return []


class ContractHandler:
    """Handles all contract-related operations"""

    def generate_source_contract(
        self,
        source_path: str,
        source_id: str,
        config: dict[str, object] | None = None,
    ) -> str:
        """Generate a source contract describing a data source

        Args:
            source_path: Absolute path to the source data file
            source_id: Unique identifier for this source (e.g., 'swedish_bank_csv')
            config: Optional configuration dictionary

        Returns:
            JSON string of the generated source contract
        """
        source_full_path = Path(source_path)
        if not source_full_path.is_absolute():
            return json.dumps({"error": "source_path must be an absolute path", "provided_path": source_path}, indent=2)

        if not source_full_path.exists():
            return json.dumps({"error": "Source file not found", "path": source_path}, indent=2)

        try:
            contract = generate_source_contract(str(source_full_path), source_id, config)
            return contract.model_dump_json(indent=2, exclude_none=False, by_alias=True)
        except (ValueError, OSError, ValidationError) as e:
            return json.dumps({"error": f"Failed to generate source contract: {e!s}"}, indent=2)

    def generate_destination_contract(
        self,
        destination_id: str,
        schema: dict[str, object] | None = None,
        config: dict[str, object] | None = None,
    ) -> str:
        """Generate a destination contract describing a data destination

        Args:
            destination_id: Unique identifier for destination (e.g., 'dwh_transactions_table')
            schema: Schema definition with fields and types
            config: Optional configuration dictionary

        Returns:
            JSON string of the generated destination contract
        """
        try:
            contract = generate_destination_contract(destination_id, schema, config)
            return contract.model_dump_json(indent=2, exclude_none=False, by_alias=True)
        except (ValueError, TypeError, ValidationError) as e:
            return json.dumps({"error": f"Failed to generate destination contract: {e!s}"}, indent=2)

    def generate_transformation_contract(
        self,
        transformation_id: str,
        source_ref: str,
        destination_ref: str,
        config: dict[str, object] | None = None,
    ) -> str:
        """Generate a transformation contract mapping source to destination

        Args:
            transformation_id: Unique identifier for this transformation
            source_ref: Reference to source contract ID
            destination_ref: Reference to destination contract ID
            config: Optional configuration dictionary

        Returns:
            JSON string of the generated transformation contract
        """
        try:
            contract = generate_transformation_contract(transformation_id, source_ref, destination_ref, config)
            return contract.model_dump_json(indent=2, exclude_none=False, by_alias=True)
        except (ValueError, TypeError, ValidationError) as e:
            return json.dumps({"error": f"Failed to generate transformation contract: {e!s}"}, indent=2)

    def generate_database_source_contract(
        self,
        source_id: str,
        connection_string: str,
        database_type: str,
        source_type: str = "table",
        source_name: str | None = None,
        query: str | None = None,
        schema: str | None = None,
        sample_size: int = 1000,
        config: dict[str, object] | None = None,
    ) -> str:
        """Generate a source contract from a database table or query

        Args:
            source_id: Unique identifier for this source
            connection_string: Database connection string
            database_type: Database type (postgresql, mysql, sqlite)
            source_type: Type of source ('table', 'view', or 'query')
            source_name: Table or view name (required if source_type is 'table' or 'view')
            query: SQL query (required if source_type is 'query')
            schema: Database schema name (optional)
            sample_size: Number of rows to sample for analysis
            config: Optional configuration dictionary

        Returns:
            JSON string of the generated source contract
        """
        try:
            # Sanitize connection string for logging
            sanitized_conn = sanitize_connection_string(connection_string)

            # Generate contract
            contract = generate_database_source_contract(
                source_id=source_id,
                connection_string=connection_string,
                database_type=database_type,
                source_type=source_type,
                source_name=source_name,
                query=query,
                schema=schema,
                sample_size=sample_size,
                config=config,
            )

            # Note: We don't include the connection string in the contract for security
            # It should be managed externally
            return contract.model_dump_json(indent=2, exclude_none=False, by_alias=True)

        except ValueError as e:
            return json.dumps({"error": f"Validation error: {e!s}"}, indent=2)
        except Exception as e:
            # Log with sanitized connection string
            sanitized_conn = sanitize_connection_string(connection_string)
            error_msg = f"Failed to generate database source contract for {sanitized_conn}: {e!s}"
            return json.dumps({"error": error_msg}, indent=2)

    def list_database_tables(
        self,
        connection_string: str,
        database_type: str,
        schema: str | None = None,
        include_views: bool = False,
        include_row_counts: bool = True,
    ) -> str:
        """List all tables in a database or schema with metadata

        Args:
            connection_string: Database connection string
            database_type: Database type (postgresql, mysql, sqlite)
            schema: Database schema name (optional)
            include_views: Whether to include views in the results
            include_row_counts: Whether to query row counts

        Returns:
            JSON string containing list of tables with metadata
        """
        try:
            # List tables
            tables = list_database_tables(
                connection_string=connection_string,
                database_type=database_type,
                schema=schema,
                include_views=include_views,
                include_row_counts=include_row_counts,
            )

            return json.dumps({"tables": tables, "count": len(tables)}, indent=2)

        except ValueError as e:
            return json.dumps({"error": f"Validation error: {e!s}"}, indent=2)
        except Exception as e:
            # Log with sanitized connection string
            sanitized_conn = sanitize_connection_string(connection_string)
            error_msg = f"Failed to list tables for {sanitized_conn}: {e!s}"
            return json.dumps({"error": error_msg}, indent=2)

    def generate_database_multi_source_contracts(
        self,
        connection_string: str,
        database_type: str,
        schema: str | None = None,
        tables: list[str] | None = None,
        include_relationships: bool = True,
        sample_size: int = 1000,
    ) -> str:
        """Generate source contracts for multiple tables with relationships

        Args:
            connection_string: Database connection string
            database_type: Database type (postgresql, mysql, sqlite)
            schema: Database schema name (optional)
            tables: List of specific tables (None = all tables)
            include_relationships: Whether to detect FK relationships
            sample_size: Number of rows to sample per table

        Returns:
            JSON string containing list of contracts with relationship metadata
        """
        try:
            # Generate contracts
            contracts = generate_database_multi_source_contracts(
                connection_string=connection_string,
                database_type=database_type,
                schema=schema,
                tables=tables,
                include_relationships=include_relationships,
                sample_size=sample_size,
            )

            # Convert to JSON-serializable format
            contracts_data = [
                contract.model_dump(mode="json", by_alias=True, exclude_none=False) for contract in contracts
            ]

            return json.dumps({"contracts": contracts_data, "count": len(contracts)}, indent=2)

        except ValueError as e:
            return json.dumps({"error": f"Validation error: {e!s}"}, indent=2)
        except Exception as e:
            # Log with sanitized connection string
            sanitized_conn = sanitize_connection_string(connection_string)
            error_msg = f"Failed to generate multi-table contracts for {sanitized_conn}: {e!s}"
            return json.dumps({"error": error_msg}, indent=2)

    def analyze_source(self, source_path: str) -> str:
        """Analyze a source file and return metadata

        Args:
            source_path: Absolute path to the source data file

        Returns:
            JSON string containing the source analysis
        """
        source_full_path = Path(source_path)
        if not source_full_path.is_absolute():
            return json.dumps({"error": "source_path must be an absolute path", "provided_path": source_path}, indent=2)

        if not source_full_path.exists():
            return json.dumps({"error": "Source file not found", "path": source_path}, indent=2)

        try:
            analysis = generate_source_analysis(str(source_full_path))
            return json.dumps(analysis, indent=2)
        except (ValueError, OSError) as e:
            return json.dumps({"error": f"Failed to analyze source: {e!s}"}, indent=2)

    def validate_contract(self, contract_path: str) -> str:
        """Validate a contract JSON file

        Args:
            contract_path: Absolute path to the contract JSON file

        Returns:
            JSON string containing validation results
        """
        contract_full_path = Path(contract_path)
        if not contract_full_path.is_absolute():
            return json.dumps(
                {"error": "contract_path must be an absolute path", "provided_path": contract_path}, indent=2
            )

        if not contract_full_path.exists():
            return json.dumps({"error": "Contract file not found", "path": contract_path}, indent=2)

        contract = load_contract(str(contract_full_path))
        if not contract:
            return json.dumps({"error": "Failed to load or validate contract file"}, indent=2)

        issues = validate_contract(contract)

        result = {
            "valid": len(issues) == 0,
            "contract_path": contract_path,
            "contract_version": contract.contract_version,
            "contract_type": contract.contract_type,
        }

        # Add type-specific fields
        if isinstance(contract, SourceContract):
            result["source_id"] = contract.source_id
        elif isinstance(contract, DestinationContract):
            result["destination_id"] = contract.destination_id
        elif isinstance(contract, TransformationContract):
            result["transformation_id"] = contract.transformation_id
            result["source_ref"] = contract.source_ref
            result["destination_ref"] = contract.destination_ref

        if issues:
            result["issues"] = issues

        return json.dumps(result, indent=2)

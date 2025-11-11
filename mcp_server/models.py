"""Pydantic models for ingestion contracts"""

from typing import Any, Literal

from pydantic import BaseModel, Field

# ============================================================================
# Source Contract Models
# ============================================================================


class SourceSchema(BaseModel):
    """Schema definition for a data source"""

    fields: list[str] = Field(description="List of field/column names")
    data_types: list[str] = Field(description="Detected data types for each field")


class QualityMetrics(BaseModel):
    """Quality metrics for a data source"""

    total_rows: int = Field(ge=0, description="Total number of rows in the source")
    sample_data: list[list[str]] = Field(default_factory=list, description="Sample data rows")
    issues: list[str] = Field(default_factory=list, description="List of quality issues detected")


class SourceContract(BaseModel):
    """Contract describing a data source"""

    contract_version: str = Field(default="1.0", description="Version of the contract schema")
    contract_type: Literal["source"] = Field(default="source", description="Type of contract")
    source_id: str = Field(description="Unique identifier for this source")
    source_path: str = Field(description="Path to the source data file")
    file_format: str = Field(description="File format (csv, json, parquet, etc.)")
    encoding: str = Field(default="utf-8", description="File encoding")
    delimiter: str | None = Field(default=None, description="Delimiter for CSV files")
    has_header: bool = Field(default=True, description="Whether the file has a header row")
    data_schema: SourceSchema = Field(description="Schema information", alias="schema")
    quality_metrics: QualityMetrics = Field(description="Quality assessment")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = {"populate_by_name": True}


# ============================================================================
# Destination Contract Models
# ============================================================================


class DestinationSchema(BaseModel):
    """Schema definition for a data destination"""

    fields: list[str] = Field(default_factory=list, description="List of field names")
    types: list[str] = Field(default_factory=list, description="Data types for each field")
    constraints: dict[str, Any] = Field(default_factory=dict, description="Field constraints")


class ValidationRules(BaseModel):
    """Validation rules for data"""

    required_fields: list[str] = Field(default_factory=list, description="Fields that must be present")
    unique_constraints: list[str] = Field(default_factory=list, description="Fields that must be unique")
    data_range_checks: dict[str, Any] = Field(default_factory=dict, description="Range checks for numeric fields")
    format_validation: dict[str, Any] = Field(default_factory=dict, description="Format validation patterns")


class DestinationContract(BaseModel):
    """Contract describing a data destination"""

    contract_version: str = Field(default="1.0", description="Version of the contract schema")
    contract_type: Literal["destination"] = Field(default="destination", description="Type of contract")
    destination_id: str = Field(description="Unique identifier for this destination")
    data_schema: DestinationSchema = Field(description="Schema definition", alias="schema")
    validation_rules: ValidationRules = Field(default_factory=ValidationRules, description="Validation rules to apply")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = {"populate_by_name": True}


# ============================================================================
# Transformation Contract Models
# ============================================================================


class ExecutionPlan(BaseModel):
    """Execution plan for data transformation"""

    batch_size: int = Field(default=100, ge=1, description="Number of records to process per batch")
    error_threshold: float = Field(default=0.1, ge=0.0, le=1.0, description="Maximum allowed error rate")
    validation_enabled: bool = Field(default=True, description="Whether to validate data")
    rollback_on_error: bool = Field(default=False, description="Whether to rollback on errors")


class TransformationContract(BaseModel):
    """Contract describing a data transformation from source to destination"""

    contract_version: str = Field(default="1.0", description="Version of the contract schema")
    contract_type: Literal["transformation"] = Field(default="transformation", description="Type of contract")
    transformation_id: str = Field(description="Unique identifier for this transformation")
    source_ref: str = Field(description="Reference to source contract ID")
    destination_ref: str = Field(description="Reference to destination contract ID")
    field_mappings: dict[str, str] = Field(
        default_factory=dict, description="Mapping from destination fields to source fields"
    )
    transformations: dict[str, Any] = Field(default_factory=dict, description="Transformations to apply to fields")
    enrichment: dict[str, Any] = Field(default_factory=dict, description="Enrichment rules")
    business_rules: list[Any] = Field(default_factory=list, description="Business rules to apply")
    execution_plan: ExecutionPlan = Field(default_factory=ExecutionPlan, description="Execution configuration")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


# ============================================================================
# Type Alias for Any Contract
# ============================================================================

Contract = SourceContract | DestinationContract | TransformationContract

"""API endpoint schema introspection."""

from typing import Any


def extract_endpoint_schema(
    openapi_spec: dict[str, Any],
    endpoint: str,
    method: str = "POST",
) -> dict[str, Any]:
    """Extract schema for a specific API endpoint.

    Args:
        openapi_spec: Parsed OpenAPI specification
        endpoint: API endpoint path (e.g., '/users', '/data')
        method: HTTP method (GET, POST, PUT, PATCH, DELETE)

    Returns:
        Dictionary containing fields, types, and constraints

    Raises:
        ValueError: If endpoint or schema not found
    """
    method = method.upper()

    # Get paths from OpenAPI spec
    paths = openapi_spec.get("paths", {})
    if endpoint not in paths:
        available = list(paths.keys())
        raise ValueError(f"Endpoint '{endpoint}' not found in schema. Available endpoints: {available}")

    # Get method from endpoint
    endpoint_spec = paths[endpoint]
    if method.lower() not in endpoint_spec:
        available_methods = [m.upper() for m in endpoint_spec if m != "parameters"]
        raise ValueError(
            f"Method '{method}' not found for endpoint '{endpoint}'. Available methods: {available_methods}"
        )

    # Get request body schema
    operation = endpoint_spec[method.lower()]

    # Handle Swagger 2.0 format (parameters with in: body)
    parameters = operation.get("parameters", [])
    body_param = None
    for param in parameters:
        if isinstance(param, dict) and param.get("in") == "body":
            body_param = param
            break

    if body_param:
        # Swagger 2.0 format
        schema = body_param.get("schema", {})
        body_required = body_param.get("required", False)
    else:
        # OpenAPI 3.0 format
        request_body = operation.get("requestBody", {})

        if not request_body:
            # No request body, return empty schema
            return {
                "fields": [],
                "types": [],
                "constraints": {},
            }

        # Get content schema (typically application/json)
        content = request_body.get("content", {})
        json_content = content.get("application/json", content.get("application/x-www-form-urlencoded", {}))

        if not json_content:
            return {
                "fields": [],
                "types": [],
                "constraints": {},
            }

        schema = json_content.get("schema", {})
        body_required = request_body.get("required", False)

    if not schema:
        return {
            "fields": [],
            "types": [],
            "constraints": {},
        }

    # Handle $ref if present
    if "$ref" in schema:
        schema = _resolve_ref(openapi_spec, schema["$ref"])

    # Extract fields and types from schema
    return _extract_fields_from_schema(schema, body_required)


def _resolve_ref(openapi_spec: dict[str, Any], ref: str) -> dict[str, Any]:
    """Resolve a $ref pointer in the OpenAPI spec.

    Args:
        openapi_spec: Full OpenAPI specification
        ref: Reference string (e.g., '#/components/schemas/User')

    Returns:
        Resolved schema object
    """
    if not ref.startswith("#/"):
        raise ValueError(f"Only internal references are supported: {ref}")

    parts = ref[2:].split("/")
    current = openapi_spec

    for part in parts:
        if part not in current:
            raise ValueError(f"Reference not found: {ref}")
        current = current[part]

    return current


def _extract_fields_from_schema(schema: dict[str, Any], body_required: bool = False) -> dict[str, Any]:
    """Extract fields, types, and constraints from a JSON schema.

    Args:
        schema: JSON schema object
        body_required: Whether the request body itself is required

    Returns:
        Dictionary with fields, types, and constraints
    """
    fields = []
    types = []
    constraints: dict[str, list[str]] = {}

    # Get required fields
    required_fields = set(schema.get("required", []))

    # Extract properties
    properties = schema.get("properties", {})

    for field_name, field_schema in properties.items():
        fields.append(field_name)

        # Map JSON schema type to contract type
        field_type = field_schema.get("type", "string")
        field_format = field_schema.get("format")

        # Map type
        contract_type = _map_json_type_to_contract_type(field_type, field_format)
        types.append(contract_type)

        # Extract constraints
        field_constraints = []

        if field_name in required_fields or body_required:
            field_constraints.append("REQUIRED")

        if "enum" in field_schema:
            enum_values = field_schema["enum"]
            field_constraints.append(f"ENUM: {', '.join(map(str, enum_values))}")

        if field_type == "string":
            if "minLength" in field_schema:
                field_constraints.append(f"MIN_LENGTH: {field_schema['minLength']}")
            if "maxLength" in field_schema:
                field_constraints.append(f"MAX_LENGTH: {field_schema['maxLength']}")
            if "pattern" in field_schema:
                field_constraints.append(f"PATTERN: {field_schema['pattern']}")

        if field_type in ["integer", "number"]:
            if "minimum" in field_schema:
                field_constraints.append(f"MIN: {field_schema['minimum']}")
            if "maximum" in field_schema:
                field_constraints.append(f"MAX: {field_schema['maximum']}")

        if field_constraints:
            constraints[field_name] = field_constraints

    return {
        "fields": fields,
        "types": types,
        "constraints": constraints,
    }


def _map_json_type_to_contract_type(json_type: str, format_type: str | None = None) -> str:
    """Map JSON schema type to contract type.

    Args:
        json_type: JSON schema type
        format_type: JSON schema format (e.g., 'date-time', 'email')

    Returns:
        Contract type string
    """
    if format_type:
        format_mapping = {
            "date-time": "datetime",
            "date": "date",
            "time": "time",
            "email": "email",
            "uri": "url",
            "uuid": "uuid",
            "int32": "integer",
            "int64": "bigint",
            "float": "float",
            "double": "double",
        }
        if format_type in format_mapping:
            return format_mapping[format_type]

    type_mapping = {
        "string": "text",
        "integer": "integer",
        "number": "float",
        "boolean": "boolean",
        "array": "array",
        "object": "json",
        "null": "null",
    }

    return type_mapping.get(json_type, "text")


def extract_endpoint_list(
    openapi_spec: dict[str, Any],
    with_fields: bool = False,
    method: str | None = None,
) -> list[dict[str, Any]]:
    """List API endpoints with optional field details.

    Args:
        openapi_spec: Parsed OpenAPI specification
        with_fields: Whether to include field details
        method: Filter by HTTP method (optional)

    Returns:
        List of endpoint details
    """
    paths = openapi_spec.get("paths", {})
    results = []

    if method:
        method = method.upper()

    for path, path_item in paths.items():
        if not path.startswith("/"):
            continue

        for op_method, operation in path_item.items():
            if op_method.lower() in ["parameters", "$ref", "summary", "description"]:
                continue

            op_method_upper = op_method.upper()

            if method and op_method_upper != method:
                continue

            endpoint_info: dict[str, Any] = {
                "method": op_method_upper,
                "path": path,
                "summary": operation.get("summary", ""),
            }

            if with_fields:
                # Reuse extract_endpoint_schema logic partly or simplify
                try:
                    # We can use the existing function but we need to handle errors gracefully
                    # and we don't want to fail if one endpoint is bad.
                    # Also extract_endpoint_schema does full extraction.
                    schema = extract_endpoint_schema(openapi_spec, path, op_method_upper)
                    endpoint_info["fields"] = schema["fields"]
                    endpoint_info["types"] = schema["types"]
                    endpoint_info["constraints"] = schema["constraints"]
                except Exception:
                    endpoint_info["error"] = "Failed to extract schema"

            # Count fields if not detailed
            if not with_fields:
                # Estimate count? Or leave it. The requirement says "names and basic info".
                pass

            results.append(endpoint_info)

    return results

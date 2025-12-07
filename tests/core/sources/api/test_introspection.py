from core.sources.api.introspection import extract_endpoint_list


def test_extract_endpoint_list() -> None:
    spec = {
        "paths": {
            "/users": {
                "get": {"summary": "Get users"},
                "post": {
                    "summary": "Create user",
                    "requestBody": {
                        "content": {"application/json": {"schema": {"properties": {"name": {"type": "string"}}}}}
                    },
                },
            },
            "/users/{id}": {"get": {"summary": "Get user"}},
        }
    }

    # Test basic
    result = extract_endpoint_list(spec)
    assert len(result) == 3
    methods = sorted([(r["path"], r["method"]) for r in result])
    assert methods == [
        ("/users", "GET"),
        ("/users", "POST"),
        ("/users/{id}", "GET"),
    ]

    # Test filter
    result = extract_endpoint_list(spec, method="POST")
    assert len(result) == 1
    assert result[0]["method"] == "POST"

    # Test with fields
    # Note: To test with_fields=True, we rely on extract_endpoint_schema working for this spec.
    # We mocked the spec enough for extract_endpoint_list to call it, but extract_endpoint_schema checks 'requestBody' etc
    result = extract_endpoint_list(spec, with_fields=True, method="POST")
    assert len(result) == 1
    assert "fields" in result[0]
    assert "name" in result[0]["fields"]

from typing import Any
from unittest.mock import MagicMock, patch

from core.sources.database.introspection import extract_table_list


@patch("core.sources.database.introspection.create_database_engine")
@patch("core.sources.database.introspection.inspect")
def test_extract_table_list(mock_inspect: MagicMock, mock_create_engine: MagicMock) -> None:
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    mock_inspector = MagicMock()
    mock_inspect.return_value = mock_inspector

    mock_inspector.get_table_names.return_value = ["table1", "table2"]

    # Mock columns
    def get_columns_side_effect(table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        if table_name == "table1":
            return [{"name": "id", "type": "INTEGER", "nullable": False}]
        return [{"name": "name", "type": "VARCHAR", "nullable": True}]

    mock_inspector.get_columns.side_effect = get_columns_side_effect

    # Test basic
    result = extract_table_list("connection", "postgresql")
    assert len(result) == 2
    assert result[0]["name"] == "table1"
    # Basic view should have column count
    assert result[0]["column_count"] == 1
    assert "columns" not in result[0]

    # Test with fields
    result = extract_table_list("connection", "postgresql", with_fields=True)
    assert len(result) == 2
    assert "columns" in result[0]
    assert result[0]["columns"][0]["name"] == "id"

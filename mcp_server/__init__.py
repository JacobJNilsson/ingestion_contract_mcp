"""MCP Server for Contract Generator

This package provides MCP tools for generating ingestion contracts.
"""

from mcp_server.config import PROJECT_ROOT, get_project_path
from mcp_server.contract_handler import ContractHandler

__all__ = [
    "PROJECT_ROOT",
    "ContractHandler",
    "get_project_path",
]

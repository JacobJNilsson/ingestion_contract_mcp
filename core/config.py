"""Project configuration and path utilities for MCP server"""

from pathlib import Path

# Project root directory (for resolving relative paths)
PROJECT_ROOT = Path(__file__).parent.parent


def get_project_path(relative_path: str) -> Path:
    """Get absolute path from relative path"""
    return PROJECT_ROOT / relative_path

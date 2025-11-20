"""Database connection and engine management."""

import re
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def sanitize_connection_string(connection_string: str) -> str:
    """Sanitize a database connection string by removing passwords for logging.

    Args:
        connection_string: The database connection string

    Returns:
        Sanitized connection string with password replaced by ***
    """
    try:
        parsed = urlparse(connection_string)
        if parsed.password:
            sanitized = connection_string.replace(parsed.password, "***")
            return sanitized
    except Exception:
        # If parsing fails, use regex as fallback
        pass

    # Fallback: Use regex to find and replace password patterns
    # Matches :password@ or :password/ patterns
    sanitized = re.sub(r"://([^:]+):([^@/]+)@", r"://\1:***@", connection_string)
    return sanitized


def create_database_engine(connection_string: str, database_type: str) -> Engine:
    """Create a SQLAlchemy engine for the specified database.

    Args:
        connection_string: The database connection string
        database_type: The database type (postgresql, mysql, sqlite)

    Returns:
        SQLAlchemy Engine instance
    """
    # Add driver-specific connection arguments if needed
    connect_args: dict[str, Any] = {}

    if database_type == "sqlite":
        # SQLite-specific arguments
        connect_args = {"check_same_thread": False}

    engine = create_engine(connection_string, connect_args=connect_args, echo=False)
    return engine

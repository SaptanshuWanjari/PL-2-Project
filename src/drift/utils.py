"""Utility functions for drift analyzer."""

from pathlib import Path
from typing import Optional


def resolve_path(file_path: str) -> Path:
    """Resolve a file path to an absolute Path object.

    Args:
        file_path: Input file path (can be relative or absolute)

    Returns:
        Resolved absolute Path object

    Raises:
        FileNotFoundError: If the file does not exist
    """
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
    return path


def format_number(n: int) -> str:
    """Format a number with thousands separators.

    Args:
        n: Number to format

    Returns:
        Formatted string with commas
    """
    return f"{n:,}"


def calculate_percentage_change(old: int, new: int) -> str:
    """Calculate percentage change between two numbers.

    Args:
        old: Original value
        new: New value

    Returns:
        Formatted percentage change string (e.g., "+15%", "-20%")
    """
    if old == 0:
        if new == 0:
            return "0%"
        return "+100%" if new > 0 else "-100%"

    change = ((new - old) / old) * 100
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.1f}%"


def truncate_string(s: str, max_length: int = 50) -> str:
    """Truncate a string to a maximum length with ellipsis.

    Args:
        s: String to truncate
        max_length: Maximum length before truncation

    Returns:
        Truncated string with ellipsis if needed
    """
    if len(s) <= max_length:
        return s
    return s[: max_length - 3] + "..."


def suggest_key_column(columns: list[str]) -> Optional[str]:
    """Suggest a key column based on naming conventions.

    Args:
        columns: List of column names

    Returns:
        Suggested key column name or None
    """
    # Priority order for key column suggestions
    key_patterns = ["id", "key", "uuid", "guid"]

    for pattern in key_patterns:
        # Check for exact match
        for col in columns:
            if col.lower() == pattern:
                return col

        # Check for suffix match (e.g., user_id, order_id)
        for col in columns:
            if col.lower().endswith(f"_{pattern}"):
                return col

    return None

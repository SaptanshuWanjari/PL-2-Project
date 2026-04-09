from pathlib import Path
from typing import Optional


def resolve_path(file_path: str) -> Path:
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
    return path


def format_number(n: int) -> str:
    return f"{n:,}"


def calculate_percentage_change(old: int, new: int) -> str:
    if old == 0:
        if new == 0:
            return "0%"
        return "+100%" if new > 0 else "-100%"

    change = ((new - old) / old) * 100
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.1f}%"


def truncate_string(s: str, max_length: int = 50) -> str:
    if len(s) <= max_length:
        return s
    return s[: max_length - 3] + "..."


def suggest_key_column(columns: list[str]) -> Optional[str]:
    key_patterns = ["id", "key", "uuid", "guid"]

    for pattern in key_patterns:
        for col in columns:
            if col.lower() == pattern:
                return col

        for col in columns:
            if col.lower().endswith(f"_{pattern}"):
                return col

    return None

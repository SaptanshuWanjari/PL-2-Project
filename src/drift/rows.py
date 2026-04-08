"""Row comparison functionality for drift analysis."""

import pandas as pd
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ColumnChange:
    """Represents a change in a single column value."""

    column: str
    old_value: Any
    new_value: Any

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "column": self.column,
            "old_value": self.old_value,
            "new_value": self.new_value,
        }


@dataclass
class RowChange:
    """Represents changes in a row identified by a key."""

    key: Any
    changes: list[ColumnChange] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "key": self.key,
            "changes": [change.to_dict() for change in self.changes],
        }


@dataclass
class ComparisonResult:
    """Result of comparing rows between two DataFrames."""

    missing_rows: list[Any] = field(default_factory=list)
    new_rows: list[Any] = field(default_factory=list)
    changed_rows: list[RowChange] = field(default_factory=list)
    unchanged_rows: int = 0
    total_old: int = 0
    total_new: int = 0
    samples: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "missing_rows": self.missing_rows,
            "new_rows": self.new_rows,
            "changed_rows": [row.to_dict() for row in self.changed_rows],
            "unchanged_rows": self.unchanged_rows,
            "total_old": self.total_old,
            "total_new": self.total_new,
            "samples": self.samples,
        }


class RowComparator:
    """Compares rows between two DataFrames to detect drift.

    This class is responsible for:
    - Identifying missing rows (present in old but not in new)
    - Identifying new rows (present in new but not in old)
    - Detecting value changes in matching rows
    - Sampling mismatched values for reporting

    Example:
        >>> comparator = RowComparator()
        >>> result = comparator.compare_rows(old_df, new_df, "id")
        >>> print(f"Found {len(result.changed_rows)} changed rows")
    """

    def __init__(self, ignore_columns: Optional[list[str]] = None):
        """Initialize the row comparator.

        Args:
            ignore_columns: List of columns to ignore during comparison
                           (e.g., timestamps, auto-generated fields)
        """
        self.ignore_columns = ignore_columns or []

    def compare_rows(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
        max_samples: int = 10,
    ) -> ComparisonResult:
        """Compare rows between two DataFrames using a key column.

        Args:
            old_df: The older DataFrame version
            new_df: The newer DataFrame version
            key_column: Column name to use as the row identifier
            max_samples: Maximum number of sample changes to include

        Returns:
            ComparisonResult containing all detected differences

        Raises:
            KeyError: If key_column is not present in both DataFrames
        """
        # Validate key column exists in both DataFrames
        if key_column not in old_df.columns:
            raise KeyError(f"Key column '{key_column}' not found in old DataFrame")
        if key_column not in new_df.columns:
            raise KeyError(f"Key column '{key_column}' not found in new DataFrame")

        # Get the keys from both DataFrames
        old_keys = set(old_df[key_column].dropna().unique())
        new_keys = set(new_df[key_column].dropna().unique())

        # Find missing, new, and common keys
        missing_keys = sorted(old_keys - new_keys)
        new_keys_list = sorted(new_keys - old_keys)
        common_keys = old_keys & new_keys

        # Detect changes in common rows
        changed_rows = self._detect_changed_rows(
            old_df, new_df, key_column, common_keys
        )

        # Generate samples
        samples = self._generate_samples(changed_rows, max_samples)

        # Count unchanged rows
        unchanged_count = len(common_keys) - len(changed_rows)

        return ComparisonResult(
            missing_rows=missing_keys,
            new_rows=new_keys_list,
            changed_rows=changed_rows,
            unchanged_rows=unchanged_count,
            total_old=len(old_keys),
            total_new=len(new_keys),
            samples=samples,
        )

    def get_row_changes(
        self,
        old_row: pd.Series,
        new_row: pd.Series,
        key_column: Optional[str] = None,
    ) -> list[ColumnChange]:
        """Detect changes between two rows.

        Args:
            old_row: Series representing the old row
            new_row: Series representing the new row
            key_column: Optional key column to exclude from comparison

        Returns:
            List of ColumnChange objects for columns with differences
        """
        changes = []

        # Get common columns (excluding ignored ones and key column)
        common_columns = set(old_row.index) & set(new_row.index)
        columns_to_compare = [
            col
            for col in common_columns
            if col not in self.ignore_columns and col != key_column
        ]

        for column in columns_to_compare:
            old_val = old_row.get(column)
            new_val = new_row.get(column)

            # Handle NaN comparison
            if pd.isna(old_val) and pd.isna(new_val):
                continue

            # Check if values differ
            if not self._values_equal(old_val, new_val):
                changes.append(
                    ColumnChange(
                        column=column,
                        old_value=self._serialize_value(old_val),
                        new_value=self._serialize_value(new_val),
                    )
                )

        return changes

    def sample_changes(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
        max_samples: int = 10,
    ) -> list[dict]:
        """Get sample changes between two DataFrames.

        This is a convenience method that returns a sample of row changes
        without performing the full comparison.

        Args:
            old_df: The older DataFrame version
            new_df: The newer DataFrame version
            key_column: Column name to use as the row identifier
            max_samples: Maximum number of samples to return

        Returns:
            List of sample change dictionaries
        """
        # Validate key column
        if key_column not in old_df.columns or key_column not in new_df.columns:
            return []

        # Get common keys
        old_keys = set(old_df[key_column].dropna().unique())
        new_keys = set(new_df[key_column].dropna().unique())
        common_keys = list(old_keys & new_keys)

        samples = []

        # Sample from common keys
        for key_val in common_keys[: max_samples * 2]:
            if len(samples) >= max_samples:
                break

            old_row = old_df[old_df[key_column] == key_val].iloc[0]
            new_row = new_df[new_df[key_column] == key_val].iloc[0]

            changes = self.get_row_changes(old_row, new_row, key_column)

            if changes:
                samples.append(
                    {
                        "key": key_val,
                        "changes": [change.to_dict() for change in changes],
                    }
                )

        return samples

    def _detect_changed_rows(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
        common_keys: set,
    ) -> list[RowChange]:
        """Detect rows with value changes among common keys.

        Args:
            old_df: The older DataFrame
            new_df: The newer DataFrame
            key_column: Column name used as key
            common_keys: Set of keys present in both DataFrames

        Returns:
            List of RowChange objects for rows with changes
        """
        changed_rows = []

        for key_val in common_keys:
            old_rows = old_df[old_df[key_column] == key_val]
            new_rows = new_df[new_df[key_column] == key_val]

            # Skip if multiple matches (shouldn't happen with unique keys)
            if len(old_rows) != 1 or len(new_rows) != 1:
                continue

            old_row = old_rows.iloc[0]
            new_row = new_rows.iloc[0]

            changes = self.get_row_changes(old_row, new_row, key_column)

            if changes:
                changed_rows.append(
                    RowChange(
                        key=key_val,
                        changes=changes,
                    )
                )

        return changed_rows

    def _generate_samples(
        self,
        changed_rows: list[RowChange],
        max_samples: int,
    ) -> list[dict]:
        """Generate sample change dictionaries from changed rows.

        Args:
            changed_rows: List of RowChange objects
            max_samples: Maximum number of samples to include

        Returns:
            List of sample dictionaries
        """
        samples = []

        for row_change in changed_rows[:max_samples]:
            samples.append(row_change.to_dict())

        return samples

    def _values_equal(self, old_val: Any, new_val: Any) -> bool:
        """Check if two values are equal, handling NaN and type differences.

        Args:
            old_val: The old value
            new_val: The new value

        Returns:
            True if values are considered equal, False otherwise
        """
        # Handle NaN cases
        if pd.isna(old_val) and pd.isna(new_val):
            return True

        if pd.isna(old_val) or pd.isna(new_val):
            return False

        # Handle numeric comparisons (int vs float)
        if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
            # Use tolerance for float comparison
            try:
                return abs(float(old_val) - float(new_val)) < 1e-9
            except (ValueError, TypeError):
                pass

        # String comparison (strip whitespace)
        if isinstance(old_val, str) and isinstance(new_val, str):
            return old_val.strip() == new_val.strip()

        # Direct comparison for other types
        try:
            return old_val == new_val
        except Exception:
            return False

    def _serialize_value(self, value: Any) -> Any:
        """Serialize a value for JSON compatibility.

        Args:
            value: The value to serialize

        Returns:
            A JSON-serializable version of the value
        """
        if pd.isna(value):
            return None

        if isinstance(value, (int, float, str, bool)):
            # Handle special float values
            if isinstance(value, float):
                if pd.isna(value):
                    return None
                if value == float("inf"):
                    return "Infinity"
                if value == float("-inf"):
                    return "-Infinity"
            return value

        # Convert other types to string
        return str(value)

    def get_row_summary(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
    ) -> dict:
        """Get a quick summary of row differences without full comparison.

        This is useful for large datasets where you want quick stats
        without computing all the detailed changes.

        Args:
            old_df: The older DataFrame version
            new_df: The newer DataFrame version
            key_column: Column name to use as the row identifier

        Returns:
            Dictionary with summary statistics
        """
        if key_column not in old_df.columns or key_column not in new_df.columns:
            return {
                "error": f"Key column '{key_column}' not found",
                "total_old": len(old_df),
                "total_new": len(new_df),
            }

        old_keys = set(old_df[key_column].dropna().unique())
        new_keys = set(new_df[key_column].dropna().unique())

        return {
            "total_old": len(old_keys),
            "total_new": len(new_keys),
            "missing_count": len(old_keys - new_keys),
            "new_count": len(new_keys - old_keys),
            "common_count": len(old_keys & new_keys),
            "change_ratio": len(new_keys - old_keys) / len(old_keys)
            if old_keys
            else 0,
        }
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd


@dataclass
class ColumnChange:
    column: str
    old_value: Any
    new_value: Any

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "old_value": self.old_value,
            "new_value": self.new_value,
        }


@dataclass
class RowChange:
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
    missing_rows: list[Any] = field(default_factory=list)
    new_rows: list[Any] = field(default_factory=list)
    changed_rows: list[RowChange] = field(default_factory=list)
    unchanged_rows: int = 0
    total_old: int = 0
    total_new: int = 0
    samples: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
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
    def __init__(self, ignore_columns: Optional[list[str]] = None):
        self.ignore_columns = ignore_columns or []

    def compare_rows(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
        max_samples: int = 10,
    ) -> ComparisonResult:

        if key_column not in old_df.columns:
            raise KeyError(f"Key column '{key_column}' not found in old DataFrame")
        if key_column not in new_df.columns:
            raise KeyError(f"Key column '{key_column}' not found in new DataFrame")

        old_keys = set(old_df[key_column].dropna().unique())
        new_keys = set(new_df[key_column].dropna().unique())

        missing_keys = sorted(old_keys - new_keys)
        new_keys_list = sorted(new_keys - old_keys)
        common_keys = old_keys & new_keys

        changed_rows = self._detect_changed_rows(
            old_df, new_df, key_column, common_keys
        )

        samples = self._generate_samples(changed_rows, max_samples)

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
        changes = []

        common_columns = set(old_row.index) & set(new_row.index)
        columns_to_compare = [
            col
            for col in common_columns
            if col not in self.ignore_columns and col != key_column
        ]

        for column in columns_to_compare:
            old_val = old_row.get(column)
            new_val = new_row.get(column)

            if pd.isna(old_val) and pd.isna(new_val):
                continue

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

        if key_column not in old_df.columns or key_column not in new_df.columns:
            return []

        old_keys = set(old_df[key_column].dropna().unique())
        new_keys = set(new_df[key_column].dropna().unique())
        common_keys = list(old_keys & new_keys)

        samples = []

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
        changed_rows = []

        for key_val in common_keys:
            old_rows = old_df[old_df[key_column] == key_val]
            new_rows = new_df[new_df[key_column] == key_val]

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
        samples = []

        for row_change in changed_rows[:max_samples]:
            samples.append(row_change.to_dict())

        return samples

    def _values_equal(self, old_val: Any, new_val: Any) -> bool:

        if pd.isna(old_val) and pd.isna(new_val):
            return True

        if pd.isna(old_val) or pd.isna(new_val):
            return False

        if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
            try:
                return abs(float(old_val) - float(new_val)) < 1e-9
            except (ValueError, TypeError):
                pass

        if isinstance(old_val, str) and isinstance(new_val, str):
            return old_val.strip() == new_val.strip()

        try:
            return old_val == new_val
        except Exception:
            return False

    def _serialize_value(self, value: Any) -> Any:
        if pd.isna(value):
            return None

        if isinstance(value, (int, float, str, bool)):
            if isinstance(value, float):
                if pd.isna(value):
                    return None
                if value == float("inf"):
                    return "Infinity"
                if value == float("-inf"):
                    return "-Infinity"
            return value

        return str(value)

    def get_row_summary(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
    ) -> dict:
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
            "change_ratio": len(new_keys - old_keys) / len(old_keys) if old_keys else 0,
        }

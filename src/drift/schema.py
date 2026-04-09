from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional

import pandas as pd


@dataclass
class ColumnRename:
    old_name: str
    new_name: str
    similarity: float

    def to_dict(self) -> dict:
        return {
            "old_name": self.old_name,
            "new_name": self.new_name,
            "similarity": self.similarity,
        }


@dataclass
class ColumnReorder:
    column: str
    old_index: int
    new_index: int

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "old_index": self.old_index,
            "new_index": self.new_index,
        }


@dataclass
class SchemaDiff:
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    reordered: list[ColumnReorder] = field(default_factory=list)
    renames: list[ColumnRename] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "added": self.added,
            "removed": self.removed,
            "reordered": [r.to_dict() for r in self.reordered],
            "renames": [r.to_dict() for r in self.renames],
        }

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.removed or self.reordered or self.renames)

    @property
    def total_changes(self) -> int:
        return (
            len(self.added)
            + len(self.removed)
            + len(self.reordered)
            + len(self.renames)
        )


class SchemaAnalyzer:
    DEFAULT_SIMILARITY_THRESHOLD = 0.6

    def __init__(self, similarity_threshold: Optional[float] = None):
        self.similarity_threshold = (
            similarity_threshold or self.DEFAULT_SIMILARITY_THRESHOLD
        )

    def compare_schemas(
        self, old_columns: list[str], new_columns: list[str]
    ) -> SchemaDiff:
        old_set = set(old_columns)
        new_set = set(new_columns)

        added = [col for col in new_columns if col not in old_set]
        removed = [col for col in old_columns if col not in new_set]

        common_columns = old_set & new_set
        reordered = []

        for col in common_columns:
            old_idx = old_columns.index(col)
            new_idx = new_columns.index(col)
            if old_idx != new_idx:
                reordered.append(
                    ColumnReorder(column=col, old_index=old_idx, new_index=new_idx)
                )

        reordered.sort(key=lambda x: x.old_index)

        return SchemaDiff(
            added=added,
            removed=removed,
            reordered=reordered,
        )

    def detect_renames(
        self,
        old_columns: list[str],
        new_columns: list[str],
        threshold: Optional[float] = None,
    ) -> list[ColumnRename]:
        if threshold is None:
            threshold = self.similarity_threshold

        old_set = set(old_columns)
        new_set = set(new_columns)

        removed_candidates = [col for col in old_columns if col not in new_set]
        added_candidates = [col for col in new_columns if col not in old_set]

        renames = []
        matched_old = set()
        matched_new = set()

        candidates = []
        for old_col in removed_candidates:
            for new_col in added_candidates:
                similarity = SequenceMatcher(
                    None, old_col.lower(), new_col.lower()
                ).ratio()
                if similarity >= threshold:
                    candidates.append((old_col, new_col, similarity))

        candidates.sort(key=lambda x: x[2], reverse=True)

        for old_col, new_col, similarity in candidates:
            if old_col not in matched_old and new_col not in matched_new:
                renames.append(
                    ColumnRename(
                        old_name=old_col,
                        new_name=new_col,
                        similarity=similarity,
                    )
                )
                matched_old.add(old_col)
                matched_new.add(new_col)

        return renames

    def analyze(self, old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
        old_columns = list(old_df.columns)
        new_columns = list(new_df.columns)

        diff = self.compare_schemas(old_columns, new_columns)

        renames = self.detect_renames(old_columns, new_columns)

        diff.renames = renames

        return diff.to_dict()

    def get_schema_summary(self, old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
        diff = self.analyze(old_df, new_df)

        return {
            "old_column_count": len(old_df.columns),
            "new_column_count": len(new_df.columns),
            "columns_added": len(diff["added"]),
            "columns_removed": len(diff["removed"]),
            "columns_reordered": len(diff["reordered"]),
            "potential_renames": len(diff["renames"]),
            "added": diff["added"],
            "removed": diff["removed"],
            "reordered": diff["reordered"],
            "renames": diff["renames"],
        }

    def explain_diff(self, old_columns: list[str], new_columns: list[str]) -> list[str]:
        diff = self.compare_schemas(old_columns, new_columns)
        renames = self.detect_renames(old_columns, new_columns)

        explanations = []

        if diff.added:
            if len(diff.added) == 1:
                explanations.append(f"Added column: {diff.added[0]}")
            else:
                explanations.append(
                    f"Added {len(diff.added)} columns: {', '.join(diff.added)}"
                )

        if diff.removed:
            if len(diff.removed) == 1:
                explanations.append(f"Removed column: {diff.removed[0]}")
            else:
                explanations.append(
                    f"Removed {len(diff.removed)} columns: {', '.join(diff.removed)}"
                )

        if diff.reordered:
            for reorder in diff.reordered:
                explanations.append(
                    f"Column '{reorder.column}' moved from position "
                    f"{reorder.old_index + 1} to {reorder.new_index + 1}"
                )

        if renames:
            for rename in renames:
                explanations.append(
                    f"Potential rename: '{rename.old_name}' -> '{rename.new_name}' "
                    f"(similarity: {rename.similarity:.2%})"
                )

        if not explanations:
            explanations.append("No schema changes detected.")

        return explanations

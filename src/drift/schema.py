"""Schema analyzer for detecting drift between CSV schemas."""

import pandas as pd
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional


@dataclass
class ColumnRename:
    """Represents a potential column rename.

    Attributes:
        old_name: The original column name.
        new_name: The new column name.
        similarity: Similarity score between 0 and 1.
    """

    old_name: str
    new_name: str
    similarity: float

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "old_name": self.old_name,
            "new_name": self.new_name,
            "similarity": self.similarity,
        }


@dataclass
class ColumnReorder:
    """Represents a column position change.

    Attributes:
        column: The column name that was reordered.
        old_index: The original index position.
        new_index: The new index position.
    """

    column: str
    old_index: int
    new_index: int

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "column": self.column,
            "old_index": self.old_index,
            "new_index": self.new_index,
        }


@dataclass
class SchemaDiff:
    """Complete schema difference between two datasets.

    Attributes:
        added: List of new column names added in the new schema.
        removed: List of column names removed from the old schema.
        reordered: List of ColumnReorder objects for reordered columns.
        renames: List of ColumnRename objects for potentially renamed columns.
    """

    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    reordered: list[ColumnReorder] = field(default_factory=list)
    renames: list[ColumnRename] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "added": self.added,
            "removed": self.removed,
            "reordered": [r.to_dict() for r in self.reordered],
            "renames": [r.to_dict() for r in self.renames],
        }

    @property
    def has_changes(self) -> bool:
        """Check if there are any schema changes."""
        return bool(self.added or self.removed or self.reordered or self.renames)

    @property
    def total_changes(self) -> int:
        """Get total number of schema changes."""
        return (
            len(self.added)
            + len(self.removed)
            + len(self.reordered)
            + len(self.renames)
        )


class SchemaAnalyzer:
    """Analyzes and compares schemas between two DataFrames.

    This class detects various types of schema drift:
    - Added columns (new columns in the new schema)
    - Removed columns (columns removed from the old schema)
    - Reordered columns (columns that changed position)
    - Renamed columns (detected via fuzzy matching)

    Example:
        >>> analyzer = SchemaAnalyzer()
        >>> old_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        >>> new_df = pd.DataFrame({"id": [1], "full_name": ["Alice"]})
        >>> diff = analyzer.analyze(old_df, new_df)
        >>> print(diff.renames[0].old_name)  # "name"
        >>> print(diff.renames[0].new_name)  # "full_name"
    """

    DEFAULT_SIMILARITY_THRESHOLD = 0.6

    def __init__(self, similarity_threshold: Optional[float] = None):
        """Initialize the schema analyzer.

        Args:
            similarity_threshold: Threshold for fuzzy rename detection (0-1).
                Defaults to 0.6. Higher values require more similarity.
        """
        self.similarity_threshold = (
            similarity_threshold or self.DEFAULT_SIMILARITY_THRESHOLD
        )

    def compare_schemas(
        self, old_columns: list[str], new_columns: list[str]
    ) -> SchemaDiff:
        """Compare two column lists and detect schema differences.

        This method identifies added, removed, and reordered columns.
        It does not detect renames (use detect_renames for that).

        Args:
            old_columns: List of column names from the old schema.
            new_columns: List of column names from the new schema.

        Returns:
            SchemaDiff object with added, removed, and reordered columns.
        """
        old_set = set(old_columns)
        new_set = set(new_columns)

        # Find added and removed columns
        added = [col for col in new_columns if col not in old_set]
        removed = [col for col in old_columns if col not in new_set]

        # Find reordered columns (columns present in both but at different positions)
        common_columns = old_set & new_set
        reordered = []

        for col in common_columns:
            old_idx = old_columns.index(col)
            new_idx = new_columns.index(col)
            if old_idx != new_idx:
                reordered.append(
                    ColumnReorder(column=col, old_index=old_idx, new_index=new_idx)
                )

        # Sort reordered by old index for consistent output
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
        """Detect potential column renames using fuzzy matching.

        This method uses difflib.SequenceMatcher to find columns that
        may have been renamed based on string similarity.

        Args:
            old_columns: List of column names from the old schema.
            new_columns: List of column names from the new schema.
            threshold: Similarity threshold (0-1). Uses instance default if not provided.

        Returns:
            List of ColumnRename objects for potential renames, sorted by
            similarity score in descending order.
        """
        if threshold is None:
            threshold = self.similarity_threshold

        old_set = set(old_columns)
        new_set = set(new_columns)

        # Only consider columns that exist in one schema (not both)
        removed_candidates = [col for col in old_columns if col not in new_set]
        added_candidates = [col for col in new_columns if col not in old_set]

        renames = []
        matched_old = set()
        matched_new = set()

        # Find all potential matches above threshold
        candidates = []
        for old_col in removed_candidates:
            for new_col in added_candidates:
                similarity = SequenceMatcher(
                    None, old_col.lower(), new_col.lower()
                ).ratio()
                if similarity >= threshold:
                    candidates.append((old_col, new_col, similarity))

        # Sort by similarity (descending) to prefer best matches
        candidates.sort(key=lambda x: x[2], reverse=True)

        # Greedily assign matches (each column can only be matched once)
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
        """Perform full schema analysis between two DataFrames.

        This method combines compare_schemas and detect_renames to provide
        a complete analysis of schema drift.

        Args:
            old_df: DataFrame representing the old schema.
            new_df: DataFrame representing the new schema.

        Returns:
            Dictionary containing:
                - added: list of new column names
                - removed: list of removed column names
                - reordered: list of {column, old_index, new_index} dicts
                - renames: list of {old_name, new_name, similarity} dicts
        """
        old_columns = list(old_df.columns)
        new_columns = list(new_df.columns)

        # Get basic schema differences
        diff = self.compare_schemas(old_columns, new_columns)

        # Detect potential renames
        renames = self.detect_renames(old_columns, new_columns)

        # Combine results
        diff.renames = renames

        return diff.to_dict()

    def get_schema_summary(self, old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
        """Get a summary of schema changes.

        Args:
            old_df: DataFrame representing the old schema.
            new_df: DataFrame representing the new schema.

        Returns:
            Dictionary with summary statistics and details.
        """
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

    def explain_diff(
        self, old_columns: list[str], new_columns: list[str]
    ) -> list[str]:
        """Generate human-readable explanations of schema changes.

        Args:
            old_columns: List of column names from the old schema.
            new_columns: List of column names from the new schema.

        Returns:
            List of human-readable explanation strings.
        """
        diff = self.compare_schemas(old_columns, new_columns)
        renames = self.detect_renames(old_columns, new_columns)

        explanations = []

        if diff.added:
            if len(diff.added) == 1:
                explanations.append(f"Added column: {diff.added[0]}")
            else:
                explanations.append(f"Added {len(diff.added)} columns: {', '.join(diff.added)}")

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
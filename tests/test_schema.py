"""Tests for SchemaAnalyzer class."""

import pytest
import pandas as pd

from drift.schema import SchemaAnalyzer, SchemaDiff, ColumnRename, ColumnReorder


class TestSchemaAnalyzer:
    """Tests for SchemaAnalyzer functionality."""

    def test_compare_schemas_added_columns(self):
        """Test detection of added columns."""
        analyzer = SchemaAnalyzer()
        old_cols = ["id", "name", "email"]
        new_cols = ["id", "name", "email", "phone", "address"]

        diff = analyzer.compare_schemas(old_cols, new_cols)

        assert "phone" in diff.added
        assert "address" in diff.added
        assert len(diff.added) == 2
        assert len(diff.removed) == 0

    def test_compare_schemas_removed_columns(self):
        """Test detection of removed columns."""
        analyzer = SchemaAnalyzer()
        old_cols = ["id", "name", "email", "phone"]
        new_cols = ["id", "name", "email"]

        diff = analyzer.compare_schemas(old_cols, new_cols)

        assert "phone" in diff.removed
        assert len(diff.removed) == 1
        assert len(diff.added) == 0

    def test_compare_schemas_reordered_columns(self):
        """Test detection of reordered columns."""
        analyzer = SchemaAnalyzer()
        old_cols = ["id", "name", "email", "phone"]
        new_cols = ["id", "email", "name", "phone"]

        diff = analyzer.compare_schemas(old_cols, new_cols)

        # name moved from position 1 to 2, email moved from position 2 to 1
        reordered_cols = [r.column for r in diff.reordered]
        assert "name" in reordered_cols or "email" in reordered_cols

    def test_detect_renames(self):
        """Test fuzzy rename detection."""
        analyzer = SchemaAnalyzer()
        old_cols = ["id", "name", "email_address"]
        new_cols = ["id", "name", "email_address_new"]

        renames = analyzer.detect_renames(old_cols, new_cols)

        # Should detect email_address -> email_address_new as potential rename
        # (similarity > 0.6)
        assert len(renames) == 1
        assert renames[0].old_name == "email_address"
        assert renames[0].new_name == "email_address_new"

    def test_detect_renames_threshold(self):
        """Test rename detection with custom threshold."""
        analyzer = SchemaAnalyzer(similarity_threshold=0.9)
        old_cols = ["id", "name", "email_addr"]
        new_cols = ["id", "name", "email"]

        renames = analyzer.detect_renames(old_cols, new_cols)

        # With high threshold, should not match
        assert len(renames) == 0

    def test_analyze_method(self):
        """Test full analysis with analyze method."""
        analyzer = SchemaAnalyzer()
        old_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email_address": ["a@ex.com", "b@ex.com"]
        })
        new_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "email": ["a@ex.com", "b@ex.com"],
            "phone": ["123", "456"]
        })

        result = analyzer.analyze(old_df, new_df)

        assert isinstance(result, dict)
        assert "added" in result
        assert "removed" in result
        assert "renames" in result
        assert "reordered" in result
        assert "phone" in result["added"]

    def test_schema_diff_to_dict(self):
        """Test SchemaDiff serialization."""
        diff = SchemaDiff(
            added=["new_col"],
            removed=["old_col"],
            reordered=[ColumnReorder(column="moved", old_index=0, new_index=2)],
            renames=[ColumnRename(old_name="old", new_name="new", similarity=0.85)]
        )

        result = diff.to_dict()

        assert result["added"] == ["new_col"]
        assert result["removed"] == ["old_col"]
        assert len(result["reordered"]) == 1
        assert len(result["renames"]) == 1

    def test_has_changes(self):
        """Test has_changes property."""
        diff_empty = SchemaDiff()
        assert diff_empty.has_changes is False

        diff_with_changes = SchemaDiff(added=["new_col"])
        assert diff_with_changes.has_changes is True

    def test_total_changes(self):
        """Test total_changes property."""
        diff = SchemaDiff(
            added=["col1", "col2"],
            removed=["col3"],
            reordered=[ColumnReorder("col4", 0, 1)],
            renames=[ColumnRename("a", "b", 0.9)]
        )

        assert diff.total_changes == 5  # 2 + 1 + 1 + 1
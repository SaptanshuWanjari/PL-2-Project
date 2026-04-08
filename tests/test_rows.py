"""Tests for RowComparator class."""

import pytest
import pandas as pd

from drift.rows import RowComparator, RowChange, ColumnChange, ComparisonResult


class TestRowComparator:
    """Tests for RowComparator functionality."""

    def test_compare_identical_rows(self):
        """Test comparison of identical rows."""
        comparator = RowComparator()
        old_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"]
        })
        new_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"]
        })

        result = comparator.compare_rows(old_df, new_df, "id")

        assert len(result.missing_rows) == 0
        assert len(result.new_rows) == 0
        assert len(result.changed_rows) == 0
        assert result.unchanged_rows == 3

    def test_compare_missing_rows(self):
        """Test detection of missing rows."""
        comparator = RowComparator()
        old_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"]
        })
        new_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"]
        })

        result = comparator.compare_rows(old_df, new_df, "id")

        assert len(result.missing_rows) == 1
        assert 3 in result.missing_rows
        assert len(result.new_rows) == 0

    def test_compare_new_rows(self):
        """Test detection of new rows."""
        comparator = RowComparator()
        old_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"]
        })
        new_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"]
        })

        result = comparator.compare_rows(old_df, new_df, "id")

        assert len(result.new_rows) == 1
        assert 3 in result.new_rows
        assert len(result.missing_rows) == 0

    def test_compare_changed_rows(self):
        """Test detection of changed rows."""
        comparator = RowComparator()
        old_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"]
        })
        new_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bobby", "Carol"]
        })

        result = comparator.compare_rows(old_df, new_df, "id")

        assert len(result.changed_rows) == 1
        assert result.changed_rows[0].key == 2

    def test_compare_multiple_changes(self):
        """Test detection of multiple row changes."""
        comparator = RowComparator()
        old_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35]
        })
        new_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Caroline"],
            "age": [30, 26, 35]
        })

        result = comparator.compare_rows(old_df, new_df, "id")

        assert len(result.changed_rows) == 2

    def test_get_row_changes(self):
        """Test row change detection."""
        comparator = RowComparator()
        old_row = pd.Series({"id": 1, "name": "Alice", "age": 30})
        new_row = pd.Series({"id": 1, "name": "Alice", "age": 31})

        changes = comparator.get_row_changes(old_row, new_row, "id")

        assert len(changes) == 1
        assert changes[0].column == "age"

    def test_comparison_result_to_dict(self):
        """Test ComparisonResult serialization."""
        result = ComparisonResult(
            missing_rows=[3],
            new_rows=[4],
            changed_rows=[
                RowChange(key=2, changes=[ColumnChange(column="name", old_value="Bob", new_value="Bobby")])
            ],
            unchanged_rows=2,
            total_old=3,
            total_new=3
        )

        result_dict = result.to_dict()

        assert result_dict["missing_rows"] == [3]
        assert result_dict["new_rows"] == [4]
        assert len(result_dict["changed_rows"]) == 1
        assert result_dict["total_old"] == 3

    def test_ignore_columns(self):
        """Test ignoring specific columns during comparison."""
        comparator = RowComparator(ignore_columns=["updated_at"])
        old_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "updated_at": ["2023-01-01", "2023-01-02"]
        })
        new_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "updated_at": ["2023-02-01", "2023-02-02"]
        })

        result = comparator.compare_rows(old_df, new_df, "id")

        # Should not count updated_at as a change
        assert len(result.changed_rows) == 0

    def test_empty_dataframes(self):
        """Test comparison with empty DataFrames."""
        comparator = RowComparator()
        old_df = pd.DataFrame(columns=["id", "name"])
        new_df = pd.DataFrame(columns=["id", "name"])

        result = comparator.compare_rows(old_df, new_df, "id")

        assert result.total_old == 0
        assert result.total_new == 0
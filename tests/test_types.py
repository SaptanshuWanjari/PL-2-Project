"""Tests for TypeChecker class."""

import pytest
import pandas as pd
import numpy as np

from drift.types import TypeChecker, TypeChange


class TestTypeChecker:
    """Tests for TypeChecker functionality."""

    def test_infer_type_int(self):
        """Test integer type inference."""
        checker = TypeChecker()
        series = pd.Series([1, 2, 3, 4, 5])

        assert checker.infer_type(series) == "int"

    def test_infer_type_float(self):
        """Test float type inference."""
        checker = TypeChecker()
        series = pd.Series([1.1, 2.2, 3.3])

        assert checker.infer_type(series) == "float"

    def test_infer_type_string(self):
        """Test string type inference."""
        checker = TypeChecker()
        series = pd.Series(["hello", "world", "test"])

        assert checker.infer_type(series) == "string"

    def test_infer_type_bool(self):
        """Test boolean type inference."""
        checker = TypeChecker()
        series = pd.Series([True, False, True])

        assert checker.infer_type(series) == "bool"

    def test_infer_type_with_nulls(self):
        """Test type inference with null values."""
        checker = TypeChecker()
        series = pd.Series([1, 2, None, 4, 5])

        assert checker.infer_type(series) == "int"

    def test_compare_types_no_change(self):
        """Test type comparison with no changes."""
        checker = TypeChecker()
        old_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        new_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        changes = checker.compare_types(old_df, new_df, ["id", "name"])

        assert len(changes) == 0

    def test_compare_types_with_changes(self):
        """Test type comparison with changes."""
        checker = TypeChecker()
        old_df = pd.DataFrame({"id": [1, 2], "value": [100, 200]})
        new_df = pd.DataFrame({"id": [1, 2], "value": ["100", "200"]})

        changes = checker.compare_types(old_df, new_df, ["id", "value"])

        assert len(changes) == 1
        assert changes[0]["column"] == "value"
        assert changes[0]["old_type"] == "int"
        assert changes[0]["new_type"] == "string"

    def test_assess_risk_high(self):
        """Test high risk assessment for type changes."""
        checker = TypeChecker()
        changes = [
            {"column": "value", "old_type": "int", "new_type": "string", "risk": "high"}
        ]

        assessed = checker.assess_risk(changes)

        assert assessed[0]["risk"] == "high"

    def test_assess_risk_low(self):
        """Test low risk assessment for type changes."""
        checker = TypeChecker()
        changes = [
            {"column": "name", "old_type": "string", "new_type": "int", "risk": "low"}
        ]

        assessed = checker.assess_risk(changes)

        assert assessed[0]["risk"] == "low"

    def test_get_type_summary(self):
        """Test type summary generation."""
        checker = TypeChecker()
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "score": [1.5, 2.5, 3.5]
        })

        summary = checker.get_type_summary(df)

        assert summary["id"] == "int"
        assert summary["name"] == "string"
        assert summary["score"] == "float"

    def test_type_change_dataclass(self):
        """Test TypeChange dataclass."""
        change = TypeChange(
            column="value",
            old_type="int",
            new_type="string",
            risk="high",
            sample_values={"old": [1, 2], "new": ["1", "2"]}
        )

        result = change.to_dict()

        assert result["column"] == "value"
        assert result["old_type"] == "int"
        assert result["new_type"] == "string"
        assert result["risk"] == "high"
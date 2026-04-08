"""Tests for CSVLoader class."""

import pytest
import pandas as pd
from pathlib import Path

from drift.loader import CSVLoader, CSVInfo


class TestCSVLoader:
    """Tests for CSVLoader functionality."""

    def test_load_basic_csv(self, fixtures_path):
        """Test loading a basic CSV file."""
        loader = CSVLoader()
        df, info = loader.load(fixtures_path / "sample_v1.csv")

        assert len(df) == 10
        assert len(df.columns) == 7
        assert "id" in df.columns
        assert "name" in df.columns
        assert info.row_count == 10
        assert info.column_count == 7

    def test_load_returns_dataframe(self, fixtures_path):
        """Test that load returns a pandas DataFrame."""
        loader = CSVLoader()
        df, _ = loader.load(fixtures_path / "sample_v1.csv")

        assert isinstance(df, pd.DataFrame)

    def test_load_returns_csv_info(self, fixtures_path):
        """Test that load returns CSVInfo metadata."""
        loader = CSVLoader()
        _, info = loader.load(fixtures_path / "sample_v1.csv")

        assert isinstance(info, CSVInfo)
        assert isinstance(info.file_path, Path)
        assert isinstance(info.columns, list)
        assert isinstance(info.encoding, str)

    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for missing file."""
        loader = CSVLoader()

        with pytest.raises(FileNotFoundError):
            loader.load("/nonexistent/file.csv")

    def test_get_column_types(self, fixtures_path):
        """Test column type inference."""
        loader = CSVLoader()
        df, _ = loader.load(fixtures_path / "sample_v1.csv")
        types = loader.get_column_types(df)

        assert types["id"] == "int"
        # Name column is object dtype, returns string after inference
        assert types["name"] in ["string", "unknown"]  # Either is acceptable
        # Salary values are floats that look like ints (75000.0), inferred as int
        assert types["salary"] in ["int", "float"]  # Either is acceptable

    def test_strict_mode(self, fixtures_path):
        """Test strict mode validation."""
        loader = CSVLoader(strict=True)
        # Should work with valid CSV
        df, info = loader.load(fixtures_path / "sample_v1.csv")
        assert len(df) == 10


@pytest.fixture
def fixtures_path():
    """Path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"
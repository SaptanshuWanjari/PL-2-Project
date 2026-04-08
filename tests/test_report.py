"""Tests for report path display formatting."""

from pathlib import Path

from drift.report import ReportConfig, ReportGenerator


def test_pretty_header_shows_cwd_relative_paths():
    """Pretty report header should show paths relative to CWD."""
    cwd = Path.cwd().resolve()
    old_abs = str((cwd / "examples" / "a_old.csv").resolve())
    new_abs = str((cwd / "examples" / "a_new.csv").resolve())

    generator = ReportGenerator(ReportConfig(format="pretty", no_color=True))
    report = generator.generate(
        results={
            "severity": "Low",
            "schema": {},
            "type_changes": [],
            "row_diff": {},
            "explanations": [],
            "impact": "",
        },
        old_info={"file_path": old_abs, "row_count": 1, "column_count": 1},
        new_info={"file_path": new_abs, "row_count": 1, "column_count": 1},
    )

    assert "examples/a_old.csv" in report
    assert "examples/a_new.csv" in report
    assert old_abs not in report
    assert new_abs not in report


def test_build_schema_tables_creates_expected_sections():
    """Schema renderables should include section tables for present categories."""
    generator = ReportGenerator(ReportConfig(format="pretty", no_color=True))

    tables = generator._build_schema_tables(
        {
            "added": ["status"],
            "removed": ["legacy"],
            "renames": [
                {"old_name": "name", "new_name": "full_name", "similarity": 0.8}
            ],
            "reordered": [{"column": "id", "old_index": 1, "new_index": 0}],
        }
    )

    titles = [str(table.title) for table in tables]
    assert "Added Columns" in titles
    assert "Removed Columns" in titles
    assert "Possible Renames" in titles
    assert "Reordered Columns" in titles

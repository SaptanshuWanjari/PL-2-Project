# Drift Analyzer

Explainable Schema Drift Analyzer for CSV Datasets

A Python CLI tool that compares two versions of a CSV dataset and generates an explainable report describing schema-level and content-level drift.

## Installation

```bash
uv pip install -e .
```

Interactive mode requires `fzf` installed and available in your PATH.

## Usage

```bash
# Open interactive mode (fzf-based command + file picker)
drift

# Compare two CSV files
drift analyze old.csv new.csv

# With key column for row comparison
drift analyze old.csv new.csv --key id

# Different output formats
drift analyze old.csv new.csv --format json
drift analyze old.csv new.csv --format markdown --output report.md

# Summary only
drift analyze old.csv new.csv --summary-only

# Schema comparison only
drift schema old.csv new.csv

# Type comparison only
drift types old.csv new.csv

# File info
drift info file.csv
```

## Features

- **Schema Drift Detection**: Added, removed, renamed, and reordered columns
- **Type Change Detection**: Data type changes with risk assessment
- **Row Comparison**: Find missing, new, and changed rows using a key column
- **Severity Scoring**: Low/Medium/High/Critical classification
- **Explainable Output**: Human-readable insights about drift impact
- **Multiple Formats**: Pretty terminal output, JSON, Markdown, plain text

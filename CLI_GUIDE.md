# CLI-Only Rich Terminal Tool Implementation Guide

## Overview
This document defines how to implement a **rich, command-driven CLI tool** (similar to Gemini CLI) for the *Explainable Schema Drift Analyzer*. The design intentionally avoids any full-screen TUI (like Textual) and focuses entirely on a **high-quality, single-command terminal experience** with structured, readable, and interactive output.

The CLI should feel:
- Fast
- Minimal friction
- Highly readable
- Script-friendly
- Explainable (not just raw diffs)

---

## Core Philosophy

### 1. Single-command-first UX
Instead of many subcommands, prefer a **single primary command**:

```
drift old.csv new.csv
```

Everything else is optional flags.

This mirrors tools like Gemini CLI, where:
- The default action is intelligent
- The tool “just works” without requiring command discovery

### 2. Output is the UI

Since there is no TUI, the entire experience is driven by:
- Structured terminal output
- Sections
- Colors
- Tables
- Progressive detail

The CLI itself *is* the interface.

### 3. Explain > Display

The tool should not just show differences. It must:
- Interpret changes
- Explain impact
- Suggest possible causes

This directly aligns with the project goal of **explainable drift analysis**.

---

## CLI Design

### Primary Command

```
drift <old.csv> <new.csv>
```

### Optional Flags

```
--key <column>
--format [pretty|json|markdown|text]
--output <file>
--summary-only
--verbose
--no-color
--detect-renames
--strict
```

### Behavior Rules

- No flags → full pretty output
- Piped output → auto-switch to plain/text
- `--summary-only` → minimal output
- `--verbose` → include deep details

---

## Output Structure

The CLI output should always follow a consistent structure:

### 1. Header Section

- Tool name
- File paths
- Timestamp

### 2. Summary Panel

- Row count comparison
- Column count comparison
- Severity level (Low / Medium / High / Critical)
- One-line explanation

### 3. Drift Overview

Key metrics:
- Columns added
- Columns removed
- Type changes
- Row differences

### 4. Schema Changes

Grouped clearly:
- Added columns
- Removed columns
- Reordered columns
- Possible renames

### 5. Type Changes

- Column name
- Old type → New type
- Risk level

### 6. Row-Level Changes (if key provided)

- Number of changed rows
- Sample mismatches
- Missing keys

### 7. Explanation Section

Human-readable insights:
- “Removed columns may break downstream systems”
- “Type change from int → string suggests parsing or ingestion change”

### 8. Export Info

If output file is generated:
- File path
- Format

---

## UX Design Principles

### 1. Progressive Disclosure

Default output should be concise.

Detailed output should only appear when:
- `--verbose` is used
- or explicitly requested

### 2. Color Semantics

- Green → safe / unchanged
- Yellow → warning / non-breaking change
- Red → breaking change
- Blue/Cyan → informational

### 3. Section Separation

Use clear visual grouping:
- spacing
- separators
- headings

### 4. Stability

The structure should never change unpredictably.
This ensures:
- script compatibility
- user familiarity

---

## Output Modes

### Pretty (default)

- Colored
- Structured
- Human-readable

### JSON

- Machine-readable
- Stable schema
- No formatting

### Markdown

- Suitable for reports
- Can be pasted into docs or PRs

### Text

- Minimal formatting
- CI-friendly

---

## Error Handling UX

Errors should be:
- Short
- Clear
- Actionable

Examples:

- Missing file → suggest correct path
- Invalid CSV → indicate likely cause
- Missing key → suggest available columns

Avoid stack traces unless `--verbose` is enabled.

---

## Performance Experience

For large datasets:

- Show progress indicators
- Indicate current phase:
  - Loading
  - Schema analysis
  - Type comparison
  - Row comparison

The CLI should never feel frozen.

---

## Explainability Layer

Every major result should include interpretation.

Examples:

- “3 columns removed — this is a breaking schema change”
- “Column ‘price’ changed from float to string — possible data ingestion issue”

This is a key differentiator from simple diff tools.

---

## Severity Model

Compute a severity score based on changes:

- Removed column → high weight
- Type change → medium-high
- Rename → medium
- Reorder → low
- Row mismatch → medium

Map to:
- Low
- Medium
- High
- Critical

Display prominently in summary.

---

## CLI Behavior Intelligence

### Smart Defaults

- Auto-detect key column if possible
- Auto-detect format based on environment
- Suggest likely fixes

### Helpful Suggestions

- If no key is provided → suggest candidate columns
- If files differ greatly → warn user

---

## Logging Strategy

- Normal mode → minimal logs
- Verbose mode → detailed step logs

Logs should not clutter primary output.

---

## Testing Strategy

### Unit Tests

- Schema detection
- Type comparison
- Row diff logic

### Snapshot Tests

- CLI output formatting
- Markdown output

### CLI Tests

- Flag combinations
- Error cases
- Output modes

---

## Implementation Phases

### Phase 1: Core CLI

- Single command
- Summary output
- Basic schema comparison

### Phase 2: Rich Output

- Structured sections
- Color semantics
- Explanation layer

### Phase 3: Advanced Features

- Rename detection
- Severity scoring
- Export formats

### Phase 4: Polish

- Smart suggestions
- Better errors
- Performance improvements

---

## Key Differences from TUI Approach

| Aspect | CLI-only Approach |
|------|------------------|
| Interaction | One-shot command |
| UI | Terminal output only |
| Complexity | Low |
| Speed | High |
| Scriptability | Excellent |
| Learning curve | Minimal |

---

## Final Goal

The final tool should feel like:

- “Run one command, understand everything”
- No navigation required
- No UI overhead
- Clear, explainable insights

This makes it ideal for:
- developers
- data engineers
- CI pipelines
- quick debugging workflows

---

## Summary

This CLI design:
- Avoids unnecessary UI complexity
- Focuses on clarity and speed
- Delivers explainable results
- Aligns perfectly with the project’s goals

The terminal becomes the interface, and the output becomes the experience.
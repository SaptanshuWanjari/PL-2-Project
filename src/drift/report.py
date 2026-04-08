"""Report generation for drift analysis."""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from rich.console import Console
from rich.columns import Columns
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


@dataclass
class ReportConfig:
    """Configuration for report generation."""

    format: str = "pretty"  # pretty, json, markdown, text
    output_file: Optional[str] = None
    no_color: bool = False
    summary_only: bool = False
    verbose: bool = False


class ReportGenerator:
    """Generates drift analysis reports in multiple formats.

    This class takes drift analysis results and produces formatted reports
    suitable for terminal output, documentation, or programmatic processing.

    Output sections (in order):
    1. Header Section - tool name, file paths, timestamp
    2. Summary Panel - severity, row counts, column counts, change counts
    3. Schema Changes - added, removed, reordered, possible renames
    4. Type Changes - column, old type → new type, risk level
    5. Row-Level Changes (if key provided) - changed rows, sample mismatches
    6. Explanation Section - human-readable insights
    7. Export Info - if output file generated

    Example:
        >>> generator = ReportGenerator()
        >>> report = generator.generate(results, format="pretty")
        >>> print(report)
    """

    COLOR_MAP = {
        "Low": "green",
        "Medium": "yellow",
        "High": "orange3",
        "Critical": "red",
    }

    RISK_COLORS = {
        "low": "green",
        "medium": "yellow",
        "high": "red",
    }

    def __init__(self, config: Optional[ReportConfig] = None):
        """Initialize the report generator.

        Args:
            config: Report configuration options
        """
        self.config = config or ReportConfig()
        self.console = Console(no_color=self.config.no_color)

    def _display_path(self, path_value: Any) -> str:
        """Render file paths relative to current working directory when possible."""
        if path_value is None:
            return "unknown"

        path_str = str(path_value)
        try:
            path = Path(path_str).expanduser()
            if not path.is_absolute():
                return path_str

            resolved = path.resolve()
            cwd = Path.cwd().resolve()
            if resolved.is_relative_to(cwd):
                return str(resolved.relative_to(cwd))

            return str(resolved)
        except Exception:
            return path_str

    def generate(
        self,
        results: dict[str, Any],
        old_info: dict,
        new_info: dict,
        format: Optional[str] = None,
    ) -> str:
        """Generate a report from drift analysis results.

        Args:
            results: Dictionary containing all drift analysis results
            old_info: Metadata about old CSV file
            new_info: Metadata about new CSV file
            format: Output format (overrides config)

        Returns:
            Formatted report string
        """
        fmt = format or self.config.format

        if fmt == "pretty":
            return self._format_pretty(results, old_info, new_info)
        elif fmt == "json":
            return self._format_json(results, old_info, new_info)
        elif fmt == "markdown":
            return self._format_markdown(results, old_info, new_info)
        elif fmt == "text":
            return self._format_text(results, old_info, new_info)
        else:
            return self._format_pretty(results, old_info, new_info)

    def _format_pretty(
        self,
        results: dict[str, Any],
        old_info: dict,
        new_info: dict,
    ) -> str:
        """Generate Rich-formatted output for terminal."""
        lines = []

        # Header
        lines.append(self._format_header_pretty(old_info, new_info))

        # Summary
        lines.append(self._format_summary_pretty(results, old_info, new_info))

        if not self.config.summary_only:
            # Schema Changes
            lines.append(self._format_schema_pretty(results.get("schema", {})))

            # Type Changes
            lines.append(self._format_types_pretty(results.get("type_changes", [])))

            # Row Changes
            if results.get("row_diff"):
                lines.append(self._format_rows_pretty(results.get("row_diff", {})))

            # Explanations
            lines.append(
                self._format_explanations_pretty(results.get("explanations", []))
            )

        # Export info
        if self.config.output_file:
            lines.append(self._format_export_pretty(self.config.output_file))

        return "\n".join(lines)

    def _format_header_pretty(self, old_info: dict, new_info: dict) -> str:
        """Format the header section."""
        from rich.text import Text

        old_path = self._display_path(old_info.get("file_path", "unknown"))
        new_path = self._display_path(new_info.get("file_path", "unknown"))

        header = Text()
        header.append("DRIFT ANALYZER", style="bold cyan")
        header.append(" v1.0.0\n")
        header.append(old_path, style="white")
        header.append(" vs ", style="dim")
        header.append(new_path, style="white")
        header.append(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")

        panel = Panel(header, border_style="cyan", padding=(1, 2))
        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()

    def _format_summary_pretty(
        self,
        results: dict[str, Any],
        old_info: dict,
        new_info: dict,
    ) -> str:
        """Format the summary panel."""
        severity = results.get("severity", "Low")
        severity_color = self.COLOR_MAP.get(severity, "white")

        # Calculate stats
        old_rows = old_info.get("row_count", 0)
        new_rows = new_info.get("row_count", 0)
        old_cols = old_info.get("column_count", 0)
        new_cols = new_info.get("column_count", 0)

        row_diff = new_rows - old_rows
        col_diff = new_cols - old_cols

        row_symbol = "+" if row_diff >= 0 else ""
        col_symbol = "+" if col_diff >= 0 else ""

        schema = results.get("schema", {})
        type_changes = results.get("type_changes", [])
        row_results = results.get("row_diff", {})

        table = Table(title="Summary", show_header=False, box=None, padding=(0, 2))
        table.add_column("key", style="dim")
        table.add_column("value")

        table.add_row("Severity", f"[{severity_color}]{severity}[/{severity_color}]")
        table.add_row("Rows", f"{old_rows:,} → {new_rows:,} ({row_symbol}{row_diff:,})")
        table.add_row("Columns", f"{old_cols} → {new_cols} ({col_symbol}{col_diff})")
        table.add_row(
            "Schema changes",
            str(len(schema.get("removed", [])) + len(schema.get("added", []))),
        )
        table.add_row("Type changes", str(len(type_changes)))
        table.add_row("Row changes", str(len(row_results.get("changed_rows", []))))

        with self.console.capture() as capture:
            self.console.print(
                Panel(table, title="[bold]Summary[/bold]", border_style="blue")
            )
        return capture.get()

    def _format_schema_pretty(self, schema: dict) -> str:
        """Format schema changes section."""
        if not schema:
            return ""

        tables = self._build_schema_tables(schema)
        if not tables:
            return ""

        with self.console.capture() as capture:
            if len(tables) == 1:
                self.console.print(tables[0])
            else:
                self.console.print(Columns(tables, equal=False, expand=True))
        return capture.get()

    def _build_schema_tables(self, schema: dict) -> list[Table]:
        """Build schema section tables for pretty rendering."""
        tables: list[Table] = []

        added = schema.get("added", [])
        if added:
            table = Table(title="Added Columns", show_header=False)
            table.add_column("column", style="green")
            for col in added:
                table.add_row(str(col))
            tables.append(table)

        removed = schema.get("removed", [])
        if removed:
            table = Table(title="Removed Columns", show_header=False)
            table.add_column("column", style="red")
            for col in removed:
                table.add_row(str(col))
            tables.append(table)

        renames = schema.get("renames", [])
        if renames:
            table = Table(title="Possible Renames")
            table.add_column("Old Name", style="yellow")
            table.add_column("New Name", style="yellow")
            table.add_column("Similarity", justify="right")
            for rename in renames:
                table.add_row(
                    rename.get("old_name", ""),
                    rename.get("new_name", ""),
                    f"{rename.get('similarity', 0):.0%}",
                )
            tables.append(table)

        reordered = schema.get("reordered", [])
        if reordered:
            table = Table(title="Reordered Columns")
            table.add_column("Column")
            table.add_column("Old Position", justify="right")
            table.add_column("New Position", justify="right")
            for item in reordered:
                table.add_row(
                    str(item.get("column", "")),
                    str(item.get("old_index", 0) + 1),
                    str(item.get("new_index", 0) + 1),
                )
            tables.append(table)

        return tables

    def _format_types_pretty(self, type_changes: list[dict]) -> str:
        """Format type changes section."""
        if not type_changes:
            return ""

        table = Table(title="Type Changes")
        table.add_column("Column", style="cyan")
        table.add_column("Old Type", style="dim")
        table.add_column("New Type")
        table.add_column("Risk", justify="center")

        for change in type_changes:
            risk = change.get("risk", "medium")
            risk_color = self.RISK_COLORS.get(risk, "white")
            table.add_row(
                change.get("column", ""),
                change.get("old_type", ""),
                change.get("new_type", ""),
                f"[{risk_color}]{risk}[/{risk_color}]",
            )

        with self.console.capture() as capture:
            self.console.print(table)
        return capture.get()

    def _format_rows_pretty(self, row_diff: dict) -> str:
        """Format row changes section."""
        parts = []

        # Summary stats
        table = Table(title="Row Comparison", show_header=False)
        table.add_column("key", style="dim")
        table.add_column("value")

        table.add_row("Total (old)", str(row_diff.get("total_old", 0)))
        table.add_row("Total (new)", str(row_diff.get("total_new", 0)))
        table.add_row("Missing rows", str(len(row_diff.get("missing_rows", []))))
        table.add_row("New rows", str(len(row_diff.get("new_rows", []))))
        table.add_row("Changed rows", str(len(row_diff.get("changed_rows", []))))
        table.add_row("Unchanged rows", str(row_diff.get("unchanged_rows", 0)))

        with self.console.capture() as capture:
            self.console.print(table)
        parts.append(capture.get())

        # Sample changes
        samples = row_diff.get("samples", [])
        if samples:
            sample_table = Table(title="Sample Changes")
            sample_table.add_column("Key")
            sample_table.add_column("Column")
            sample_table.add_column("Old Value", style="red")
            sample_table.add_column("New Value", style="green")

            for sample in samples[:5]:
                for change in sample.get("changes", [])[:3]:
                    sample_table.add_row(
                        str(sample.get("key", "")),
                        str(change.get("column", "")),
                        str(change.get("old_value", "")),
                        str(change.get("new_value", "")),
                    )

            with self.console.capture() as capture:
                self.console.print(sample_table)
            parts.append(capture.get())

        return "\n".join(parts)

    def _format_explanations_pretty(self, explanations: list[str]) -> str:
        """Format explanations section."""
        if not explanations:
            return ""

        content = "\n".join(f"• {exp}" for exp in explanations)
        panel = Panel(content, title="[bold]Insights[/bold]", border_style="cyan")

        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()

    def _format_export_pretty(self, output_file: str) -> str:
        """Format export information."""
        return f"\n[dim]Report saved to: {output_file}[/dim]"

    def _format_json(
        self,
        results: dict[str, Any],
        old_info: dict,
        new_info: dict,
    ) -> str:
        """Generate JSON output."""
        output = {
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat(),
            "files": {
                "old": old_info,
                "new": new_info,
            },
            "severity": results.get("severity", "Low"),
            "schema": results.get("schema", {}),
            "type_changes": results.get("type_changes", []),
            "row_diff": results.get("row_diff", {}),
            "explanations": results.get("explanations", []),
            "impact": results.get("impact", ""),
        }
        return json.dumps(output, indent=2, default=str)

    def _format_markdown(
        self,
        results: dict[str, Any],
        old_info: dict,
        new_info: dict,
    ) -> str:
        """Generate Markdown output."""
        lines = []

        # Header
        old_path = self._display_path(old_info.get("file_path", "unknown"))
        new_path = self._display_path(new_info.get("file_path", "unknown"))
        lines.append("# Drift Analysis Report\n")
        lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        lines.append(f"**Old file:** `{old_path}`")
        lines.append(f"**New file:** `{new_path}`\n")

        # Summary
        severity = results.get("severity", "Low")
        lines.append("## Summary\n")
        lines.append(f"| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| **Severity** | {severity} |")
        lines.append(
            f"| **Rows** | {old_info.get('row_count', 0):,} → {new_info.get('row_count', 0):,} |"
        )
        lines.append(
            f"| **Columns** | {old_info.get('column_count', 0)} → {new_info.get('column_count', 0)} |"
        )

        # Schema Changes
        schema = results.get("schema", {})
        if schema:
            lines.append("\n## Schema Changes\n")

            if schema.get("added"):
                lines.append("### Added Columns\n")
                for col in schema["added"]:
                    lines.append(f"- {col}")

            if schema.get("removed"):
                lines.append("\n### Removed Columns\n")
                for col in schema["removed"]:
                    lines.append(f"- {col}")

            if schema.get("renames"):
                lines.append("\n### Possible Renames\n")
                lines.append("| Old Name | New Name | Similarity |")
                lines.append("|----------|----------|-----------|")
                for rename in schema["renames"]:
                    lines.append(
                        f"| {rename.get('old_name', '')} | {rename.get('new_name', '')} | "
                        f"{rename.get('similarity', 0):.0%} |"
                    )

        # Type Changes
        type_changes = results.get("type_changes", [])
        if type_changes:
            lines.append("\n## Type Changes\n")
            lines.append("| Column | Old Type | New Type | Risk |")
            lines.append("|--------|----------|----------|------|")
            for change in type_changes:
                lines.append(
                    f"| {change.get('column', '')} | {change.get('old_type', '')} | "
                    f"{change.get('new_type', '')} | {change.get('risk', 'medium')} |"
                )

        # Row Changes
        row_diff = results.get("row_diff", {})
        if row_diff:
            lines.append("\n## Row Changes\n")
            lines.append(f"| Metric | Count |")
            lines.append("|--------|-------|")
            lines.append(f"| Total (old) | {row_diff.get('total_old', 0):,} |")
            lines.append(f"| Total (new) | {row_diff.get('total_new', 0):,} |")
            lines.append(
                f"| Missing rows | {len(row_diff.get('missing_rows', [])):,} |"
            )
            lines.append(f"| New rows | {len(row_diff.get('new_rows', [])):,} |")
            lines.append(
                f"| Changed rows | {len(row_diff.get('changed_rows', [])):,} |"
            )

        # Explanations
        explanations = results.get("explanations", [])
        if explanations:
            lines.append("\n## Insights\n")
            for exp in explanations:
                lines.append(f"- {exp}")

        return "\n".join(lines)

    def _format_text(
        self,
        results: dict[str, Any],
        old_info: dict,
        new_info: dict,
    ) -> str:
        """Generate plain text output (CI-friendly)."""
        lines = []

        # Header
        old_path = self._display_path(old_info.get("file_path", "unknown"))
        new_path = self._display_path(new_info.get("file_path", "unknown"))
        lines.append("=" * 60)
        lines.append("DRIFT ANALYSIS REPORT")
        lines.append("=" * 60)
        lines.append(f"Old: {old_path}")
        lines.append(f"New: {new_path}")
        lines.append(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # Summary
        severity = results.get("severity", "Low")
        lines.append(f"SEVERITY: {severity}")
        lines.append(
            f"Rows: {old_info.get('row_count', 0):,} -> {new_info.get('row_count', 0):,}"
        )
        lines.append(
            f"Columns: {old_info.get('column_count', 0)} -> {new_info.get('column_count', 0)}"
        )
        lines.append("")

        # Schema Changes
        schema = results.get("schema", {})
        if schema.get("added"):
            lines.append("ADDED COLUMNS:")
            for col in schema["added"]:
                lines.append(f"  + {col}")
            lines.append("")

        if schema.get("removed"):
            lines.append("REMOVED COLUMNS:")
            for col in schema["removed"]:
                lines.append(f"  - {col}")
            lines.append("")

        if schema.get("renames"):
            lines.append("POSSIBLE RENAMES:")
            for rename in schema["renames"]:
                lines.append(
                    f"  {rename.get('old_name', '')} -> {rename.get('new_name', '')} "
                    f"({rename.get('similarity', 0):.0%})"
                )
            lines.append("")

        # Type Changes
        type_changes = results.get("type_changes", [])
        if type_changes:
            lines.append("TYPE CHANGES:")
            for change in type_changes:
                lines.append(
                    f"  {change.get('column', '')}: "
                    f"{change.get('old_type', '')} -> {change.get('new_type', '')} "
                    f"[{change.get('risk', 'medium')}]"
                )
            lines.append("")

        # Row Changes
        row_diff = results.get("row_diff", {})
        if row_diff:
            lines.append("ROW CHANGES:")
            lines.append(f"  Missing: {len(row_diff.get('missing_rows', []))}")
            lines.append(f"  New: {len(row_diff.get('new_rows', []))}")
            lines.append(f"  Changed: {len(row_diff.get('changed_rows', []))}")
            lines.append("")

        # Explanations
        explanations = results.get("explanations", [])
        if explanations:
            lines.append("INSIGHTS:")
            for exp in explanations:
                lines.append(f"  - {exp}")
            lines.append("")

        lines.append("=" * 60)

        return "\n".join(lines)

    def save_report(self, report: str, output_file: str) -> None:
        """Save report to a file.

        Args:
            report: Formatted report string
            output_file: Path to output file
        """
        path = Path(output_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report)

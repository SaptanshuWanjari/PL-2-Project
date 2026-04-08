import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text

from drift.explain import ExplainabilityEngine
from drift.loader import CSVLoader
from drift.report import ReportConfig, ReportGenerator
from drift.rows import RowComparator
from drift.schema import SchemaAnalyzer
from drift.types import TypeChecker
from drift.utils import suggest_key_column

app = typer.Typer(
    name="drift",
    help="Explainable Schema Drift Analyzer for CSV Datasets",
    add_completion=False,
)

console = Console()


def is_fzf_available() -> bool:
    return shutil.which("fzf") is not None


def run_fzf(options: list[str], prompt: str) -> Optional[str]:
    if not options:
        return None

    result = subprocess.run(
        ["fzf", "--prompt", f"{prompt}: "],
        input="\n".join(options),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    selection = result.stdout.strip()
    return selection if selection else None


def list_csv_files() -> list[str]:
    cwd = Path.cwd().resolve()
    files: list[str] = []

    for root, dirs, filenames in os.walk(cwd, topdown=True):
        dirs[:] = [name for name in dirs if not name.startswith(".")]

        root_path = Path(root)
        for filename in filenames:
            if not filename.lower().endswith(".csv"):
                continue

            resolved = (root_path / filename).resolve()
            if not resolved.is_file():
                continue
            if not resolved.is_relative_to(cwd):
                continue
            files.append(str(resolved.relative_to(cwd)))

    return sorted(files)


def pick_csv_file(prompt: str) -> Optional[str]:
    csv_files = list_csv_files()
    if not csv_files:
        console.print("[yellow]No CSV files found in current directory.[/yellow]")
        return None
    return run_fzf(csv_files, prompt)


def detect_key_column(old_df, new_df) -> Optional[str]:
    common_columns = [col for col in old_df.columns if col in new_df.columns]
    if not common_columns:
        return None

    suggested = suggest_key_column(common_columns)
    if suggested:
        return suggested

    for col in common_columns:
        if old_df[col].isna().any() or new_df[col].isna().any():
            continue
        if old_df[col].duplicated().any() or new_df[col].duplicated().any():
            continue
        return col

    return None


def display_path(path_value: Path | str) -> str:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        return str(path)

    resolved = path.resolve()
    cwd = Path.cwd().resolve()
    if resolved.is_relative_to(cwd):
        return str(resolved.relative_to(cwd))
    return str(resolved)


def launch_interactive_mode() -> None:
    if not is_fzf_available():
        console.print(
            "[red]fzf is required for interactive mode but was not found.[/red]"
        )
        raise typer.Exit(1)

    command = run_fzf(["analyze", "types", "info", "exit"], "Command")
    if command is None or command == "exit":
        return

    if command == "analyze":
        old_file = pick_csv_file("Old CSV")
        if old_file is None:
            return
        new_file = pick_csv_file("New CSV")
        if new_file is None:
            return
        analyze(
            old_file=Path(old_file),
            new_file=Path(new_file),
            key=None,
            format="pretty",
            output=None,
            summary_only=False,
            verbose=False,
            no_color=False,
            strict=False,
        )
        return

    if command == "types":
        old_file = pick_csv_file("Old CSV")
        if old_file is None:
            return
        new_file = pick_csv_file("New CSV")
        if new_file is None:
            return
        types(old_file=Path(old_file), new_file=Path(new_file))
        return

    if command == "info":
        file = pick_csv_file("CSV File")
        if file is None:
            return
        info(file=Path(file))


def emit_report(report: str, format: str) -> None:
    if format == "json":
        print(report)
        return

    if format == "pretty":
        console.print(Text.from_ansi(report))
        return

    print(report)


@app.callback(invoke_without_command=True)
def interactive_callback(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        launch_interactive_mode()
        raise typer.Exit()


@app.command()
def analyze(
    old_file: Path = typer.Argument(
        ...,
        help="Path to the old CSV file",
        exists=True,
    ),
    new_file: Path = typer.Argument(
        ...,
        help="Path to the new CSV file",
        exists=True,
    ),
    key: Optional[str] = typer.Option(
        None,
        "--key",
        "-k",
        help="Key column for row comparison",
    ),
    format: str = typer.Option(
        "pretty",
        "--format",
        "-f",
        help="Output format: pretty, json, markdown, text",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output file path",
    ),
    summary_only: bool = typer.Option(
        False,
        "--summary-only",
        "-s",
        help="Show summary only, hide detailed changes",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed progress and debug information",
    ),
    no_color: bool = typer.Option(
        False,
        "--no-color",
        help="Disable colored output",
    ),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Fail on malformed CSV data",
    ),
) -> None:
    # Initialize components
    loader = CSVLoader(strict=strict)
    schema_analyzer = SchemaAnalyzer()
    type_checker = TypeChecker()
    row_comparator = RowComparator()
    explainability = ExplainabilityEngine()

    # Configure report generator
    config = ReportConfig(
        format=format,
        output_file=str(output) if output else None,
        no_color=no_color,
        summary_only=summary_only,
        verbose=verbose,
    )
    report_generator = ReportGenerator(config)

    try:
        # Load files with progress
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            disable=no_color,
        ) as progress:
            task = progress.add_task("Loading files...", total=None)

            old_df, old_info = loader.load(old_file)
            progress.update(task, description=f"Loaded {old_file.name}")

            new_df, new_info = loader.load(new_file)
            progress.update(task, description=f"Loaded {new_file.name}")

        if verbose:
            console.print("[dim]Analyzing schema...[/dim]")

        schema_diff = schema_analyzer.analyze(old_df, new_df)

        common_columns = list(set(old_df.columns) & set(new_df.columns))

        if verbose:
            console.print("[dim]Analyzing types...[/dim]")

        type_changes = type_checker.compare_types(old_df, new_df, common_columns)

        row_diff = {}
        key_column = key

        if key_column is None:
            key_column = detect_key_column(old_df, new_df)
            if key_column and verbose:
                console.print(f"[dim]Auto-detected key column: {key_column}[/dim]")

        if key_column:
            if verbose:
                console.print(f"[dim]Comparing rows using key: {key_column}[/dim]")

            if key_column not in old_df.columns:
                console.print(
                    f"[red]Error: Key column '{key_column}' not found in old file[/red]"
                )
                console.print(
                    f"[dim]Available columns: {', '.join(old_df.columns)}[/dim]"
                )
                raise typer.Exit(1)

            if key_column not in new_df.columns:
                console.print(
                    f"[red]Error: Key column '{key_column}' not found in new file[/red]"
                )
                console.print(
                    f"[dim]Available columns: {', '.join(new_df.columns)}[/dim]"
                )
                raise typer.Exit(1)

            result = row_comparator.compare_rows(old_df, new_df, key_column)
            row_diff = result.to_dict()

        # Calculate severity
        if verbose:
            console.print("[dim]Calculating severity...[/dim]")

        severity = explainability.calculate_severity(
            schema_diff, type_changes, row_diff
        )

        # Generate explanations
        explanations = explainability.generate_explanations(
            schema_diff, type_changes, row_diff
        )

        # Calculate impact
        change_summary = explainability.get_change_summary(
            schema_diff, type_changes, row_diff
        )
        impact = explainability.explain_impact(severity, change_summary)

        # Prepare results
        results = {
            "severity": severity,
            "schema": schema_diff,
            "type_changes": type_changes,
            "row_diff": row_diff,
            "explanations": explanations,
            "impact": impact,
        }

        # Generate report
        report = report_generator.generate(
            results, old_info.to_dict(), new_info.to_dict()
        )

        # Output
        if output:
            report_generator.save_report(report, str(output))
            console.print(f"[green]Report saved to: {output}[/green]")

        # Print report
        emit_report(report, format)

    except FileNotFoundError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1)


@app.command()
def types(
    old_file: Path = typer.Argument(..., exists=True),
    new_file: Path = typer.Argument(..., exists=True),
) -> None:
    """Compare column types between two CSV files."""
    loader = CSVLoader()
    checker = TypeChecker()

    try:
        old_df, old_info = loader.load(old_file)
        new_df, new_info = loader.load(new_file)

        common_columns = list(set(old_df.columns) & set(new_df.columns))
        changes = checker.compare_types(old_df, new_df, common_columns)

        table = Table(title="Type Comparison")
        table.add_column("Column / Metric", style="cyan")
        table.add_column("Old Type / Value", style="dim")
        table.add_column("New Type / Value")
        table.add_column("Risk", justify="center")

        table.add_row(
            "Files",
            display_path(old_info.file_path),
            display_path(new_info.file_path),
            "-",
        )
        table.add_row("Common columns", str(len(common_columns)), "", "-")
        table.add_row("Type changes", str(len(changes)), "", "-")
        table.add_section()

        if changes:
            for change in changes:
                risk = change.get("risk", "medium")
                risk_color = {"low": "green", "medium": "yellow", "high": "red"}.get(
                    risk, "white"
                )
                table.add_row(
                    change.get("column", ""),
                    change.get("old_type", ""),
                    change.get("new_type", ""),
                    f"[{risk_color}]{risk}[/{risk_color}]",
                )
        else:
            table.add_row("No type changes detected", "", "", "-")

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def info(
    file: Path = typer.Argument(..., exists=True),
) -> None:
    """Show information about a CSV file."""
    loader = CSVLoader()

    try:
        df, info = loader.load(file)
        types = loader.get_column_types(df)

        table = Table(title="CSV File Info")
        table.add_column("Item", style="cyan")
        table.add_column("Value", style="yellow")

        table.add_row("File", display_path(info.file_path))
        table.add_row("Rows", f"{info.row_count:,}")
        table.add_row("Columns", str(info.column_count))
        table.add_row("Size", f"{info.file_size_bytes / 1024:.1f} KB")
        table.add_row("Encoding", info.encoding)
        table.add_section()

        for col, dtype in types.items():
            table.add_row(f"Column: {col}", str(dtype))

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()

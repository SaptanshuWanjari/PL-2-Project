"""Tests for interactive CLI behavior."""

from pathlib import Path
from typing import Any
import builtins

from typer.testing import CliRunner
from rich.text import Text

import drift.cli as cli


def test_no_args_enters_interactive_mode(monkeypatch):
    """No args should launch interactive mode."""
    runner = CliRunner()
    called = {"value": False}

    def fake_launch() -> None:
        called["value"] = True

    monkeypatch.setattr(cli, "launch_interactive_mode", fake_launch)

    result = runner.invoke(cli.app, [])

    assert result.exit_code == 0
    assert called["value"] is True


def test_subcommand_does_not_enter_interactive_mode(monkeypatch, tmp_path):
    """Explicit subcommands should bypass interactive mode."""
    runner = CliRunner()
    called = {"value": False}

    def fake_launch() -> None:
        called["value"] = True

    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("id,name\n1,Alice\n", encoding="utf-8")

    monkeypatch.setattr(cli, "launch_interactive_mode", fake_launch)

    result = runner.invoke(cli.app, ["info", str(csv_file)])

    assert result.exit_code == 0
    assert called["value"] is False


def test_is_fzf_available(monkeypatch):
    """fzf availability should follow shutil.which."""
    monkeypatch.setattr(cli.shutil, "which", lambda _: "/usr/bin/fzf")
    assert cli.is_fzf_available() is True

    monkeypatch.setattr(cli.shutil, "which", lambda _: None)
    assert cli.is_fzf_available() is False


def test_run_fzf_returns_none_on_non_zero(monkeypatch):
    """Non-zero fzf return should behave like cancel."""

    class Result:
        returncode = 130
        stdout = ""

    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: Result())

    assert cli.run_fzf(["a", "b"], "Pick one") is None


def test_run_fzf_returns_selected_value(monkeypatch):
    """Successful fzf return should provide selected option."""

    class Result:
        returncode = 0
        stdout = "selected.csv\n"

    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: Result())

    assert cli.run_fzf(["a", "selected.csv"], "Pick one") == "selected.csv"


def test_launch_interactive_dispatches_analyze(monkeypatch, tmp_path):
    """Interactive mode should dispatch analyze with selected files."""
    old_file = tmp_path / "old.csv"
    new_file = tmp_path / "new.csv"
    old_file.write_text("id,name\n1,Alice\n", encoding="utf-8")
    new_file.write_text("id,name\n1,Alice\n", encoding="utf-8")

    picks = ["analyze", str(old_file), str(new_file)]

    def fake_run_fzf(options: list[str], prompt: str):
        _ = options
        _ = prompt
        return picks.pop(0)

    called: dict[str, Any] = {"args": None}

    def fake_analyze(old_file: Path, new_file: Path, **kwargs):
        called["args"] = (old_file, new_file, kwargs)

    monkeypatch.setattr(cli, "run_fzf", fake_run_fzf)
    monkeypatch.setattr(cli, "is_fzf_available", lambda: True)
    monkeypatch.setattr(cli, "analyze", fake_analyze)
    monkeypatch.setattr(cli.typer, "prompt", lambda *args, **kwargs: "")

    cli.launch_interactive_mode()

    assert called["args"] is not None
    assert called["args"][0] == old_file
    assert called["args"][1] == new_file


def test_emit_report_pretty_parses_ansi(monkeypatch):
    """Pretty output should parse ANSI before console printing."""
    captured: dict[str, Any] = {"value": None}

    def fake_console_print(value):
        captured["value"] = value

    monkeypatch.setattr(cli.console, "print", fake_console_print)

    cli.emit_report("\x1b[31mCritical\x1b[0m", "pretty")

    assert isinstance(captured["value"], Text)
    assert captured["value"].plain == "Critical"


def test_emit_report_text_uses_plain_print(monkeypatch):
    """Non-pretty output should use plain print."""
    captured: dict[str, Any] = {"value": None}

    def fake_print(value):
        captured["value"] = value

    monkeypatch.setattr(builtins, "print", fake_print)

    cli.emit_report("plain-output", "text")

    assert captured["value"] == "plain-output"


def test_list_csv_files_scopes_to_cwd_descendants(monkeypatch, tmp_path):
    """File list should include CWD and subfolders only, as relative paths."""
    root_csv = tmp_path / "root.csv"
    nested_dir = tmp_path / "nested"
    nested_dir.mkdir()
    nested_csv = nested_dir / "child.csv"
    root_csv.write_text("id\n1\n", encoding="utf-8")
    nested_csv.write_text("id\n2\n", encoding="utf-8")

    external_csv = tmp_path.parent / "outside.csv"
    external_csv.write_text("id\n3\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    files = cli.list_csv_files()

    assert "root.csv" in files
    assert "nested/child.csv" in files
    assert "../outside.csv" not in files
    assert str(external_csv) not in files


def test_list_csv_files_ignores_dot_directories(monkeypatch, tmp_path):
    """CSV files under dot-prefixed directories should be ignored."""
    public_dir = tmp_path / "data"
    public_dir.mkdir()
    public_csv = public_dir / "visible.csv"
    public_csv.write_text("id\n1\n", encoding="utf-8")

    hidden_dir = tmp_path / ".hidden"
    hidden_dir.mkdir()
    hidden_csv = hidden_dir / "secret.csv"
    hidden_csv.write_text("id\n2\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    files = cli.list_csv_files()

    assert "data/visible.csv" in files
    assert ".hidden/secret.csv" not in files

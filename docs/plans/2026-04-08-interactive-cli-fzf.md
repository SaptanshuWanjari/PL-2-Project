# Interactive CLI FZF Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a no-args `drift` interactive mode that lets users navigate subcommands and select CSV files using `fzf`, while preserving existing subcommand behavior.

**Architecture:** Introduce a Typer callback that intercepts `drift` invocation with no subcommand and launches an interactive flow. Keep all existing command functions as the source of analysis logic, and make the interactive flow gather inputs and call those functions directly. Implement small helper functions for `fzf` invocation, fallback behavior, and argument collection.

**Tech Stack:** Python 3, Typer, Rich, subprocess (`fzf` integration), pytest

---

### Task 1: Add failing tests for no-args interactive entrypoint

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/drift/cli.py`

**Step 1: Write the failing test**

Add tests that validate:
- Running app callback with no invoked subcommand triggers interactive mode.
- Running an explicit subcommand does not trigger interactive mode.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -k interactive -v`
Expected: FAIL because callback/interactive entrypoint does not exist.

**Step 3: Write minimal implementation**

Add Typer callback with `invoke_without_command=True`; if `ctx.invoked_subcommand is None`, launch interactive mode.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -k interactive -v`
Expected: PASS.

### Task 2: Add failing tests for `fzf`-driven selection utilities

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/drift/cli.py`

**Step 1: Write the failing test**

Add tests for helper functions:
- `is_fzf_available()` returns True/False based on `shutil.which`.
- File picker returns selected path when subprocess returns success.
- File picker returns None on cancel/non-zero status.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -k fzf -v`
Expected: FAIL because helpers are missing.

**Step 3: Write minimal implementation**

Implement `is_fzf_available`, `run_fzf`, and `pick_csv_file` helpers.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -k fzf -v`
Expected: PASS.

### Task 3: Add failing tests for interactive subcommand dispatch

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/drift/cli.py`

**Step 1: Write the failing test**

Add tests that mock picker functions and assert:
- Selecting `analyze` calls analyze function with selected old/new files.
- Selecting `schema` calls schema with selected files.
- Selecting `types` calls types with selected files.
- Selecting `info` calls info with selected file.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -k dispatch -v`
Expected: FAIL because dispatcher does not exist.

**Step 3: Write minimal implementation**

Implement interactive menu and dispatch logic in `src/drift/cli.py`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -k dispatch -v`
Expected: PASS.

### Task 4: Add graceful non-fzf fallback behavior

**Files:**
- Modify: `tests/test_cli.py`
- Modify: `src/drift/cli.py`

**Step 1: Write the failing test**

Add tests validating when `fzf` is unavailable:
- Interactive mode exits with a clear message (no crash).

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cli.py -k fallback -v`
Expected: FAIL before fallback path exists.

**Step 3: Write minimal implementation**

Add fallback mode with simple Typer prompts for menu and path entry.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cli.py -k fallback -v`
Expected: PASS.

### Task 5: Verify end-to-end CLI behavior and docs

**Files:**
- Modify: `README.md`

**Step 1: Add usage docs**

Document:
- `drift` opens interactive mode.
- Existing subcommands remain available.

**Step 2: Run focused tests**

Run: `pytest tests/test_cli.py -v`
Expected: PASS.

**Step 3: Run full suite**

Run: `pytest -v`
Expected: PASS.

**Step 4: Manual smoke test**

Run: `.venv/bin/drift`
Expected: interactive menu opens.

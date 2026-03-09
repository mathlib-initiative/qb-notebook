# AGENTS.md

This file gives coding agents repo-specific guidance for `qb-notebook`.

## Project Overview

- Language: Python (>= 3.10)
- Package layout: `qb_notebook/`
- Main data shape: parquet files under `data/`
- Main usage modes:
  - library-style helpers (`qb_notebook.data_io`, `qb_notebook.filters`, `qb_notebook.intervals`, `qb_notebook.plotting`)
  - notebook exploration (`pr_intervals.ipynb`)
  - static plot generation (`qb_notebook/generate_plot_site.py`)

## Environment and Tooling

- Use `uv` for all Python commands in this repo.
- Initial setup:
  - `uv sync`
- Run tests:
  - `uv run pytest`
- Run a single test file:
  - `uv run pytest tests/test_intervals.py`
- Lint:
  - `uv run ruff check .`
- CI lint target:
  - `uv run ruff check qb_notebook tests download_artifact.py`
- Format:
  - `uv run ruff format .`
- Format check (all Python files):
  - `uv run ruff format --check .`
- CI format check:
  - `uv run ruff format --check qb_notebook tests download_artifact.py`

## Data Expectations

`qb_notebook.data_io.load_pr_interval_data(...)` expects these parquet files in `data/`:

- `syncer_pullrequest.parquet`
- `syncer_prtimelineevent.parquet`
- `syncer_labeldef.parquet`
- `syncer_prlabel.parquet`
- `analyzer_prqueuewindow.parquet`

When adding new analyses, prefer going through `load_pr_interval_data` and existing helpers before introducing custom IO code.

## Plot Site Workflow

- Entry point: `qb_notebook.generate_plot_site` (module execution recommended).
- Generate site:
  - `uv run python -m qb_notebook.generate_plot_site --data-dir data --site-dir _site`
- The script writes:
  - `_site/index.html`
  - `_site/images/*.png`
- Add plots by:
  1. extending `_load_context(...)` only with data needed by renderers
  2. adding `render_*` functions returning `matplotlib.figure.Figure`
  3. adding a `PlotDefinition` to `PLOTS` in desired display order

## Coding Conventions for This Repo

- Prefer Polars expressions and dataframe operations over pandas unless plotting conversion is needed.
- Reuse existing interval helpers:
  - `with_effective_end`
  - `effective_queue_prs_per_day`
  - `effective_queue_window_durations`
  - `snapshot_queue_age_quantiles`
- Keep regex/filter logic consistent with notebook semantics when porting notebook analyses into code.
- For “last 365 days” plot variants in this repo, compute the full timeseries first, then filter by `max(date/day) - 365 days`.

## Testing Expectations

- If touching `qb_notebook/intervals.py`, `qb_notebook/filters.py`, or `qb_notebook/data_io.py`, add/update unit tests under `tests/`.
- For plot-site changes, at minimum run:
  - `uv run python -m py_compile qb_notebook/generate_plot_site.py`
  - `uv run python -m qb_notebook.generate_plot_site --data-dir data --site-dir /tmp/qb-plot-site-check`

## Known Gotchas

- Running `python qb_notebook/generate_plot_site.py` directly can fail due to import path issues; use module mode (`-m`) instead.
- Many helpers assume UTC-aware datetimes; preserve timezone handling when adding transformations.

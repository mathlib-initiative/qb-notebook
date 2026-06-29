# AGENTS.md

This file gives coding agents repo-specific guidance for `qb-notebook`.

## Project Overview

- Language: Python (>= 3.10)
- Package layout: `qb_notebook/` — data/IO + plotting helpers (`artifacts`,
  `data_io`, `filters`, `intervals`, `plotting`, `generate_plot_site`,
  `wasm_io`) plus the review-analysis helpers (`review_states`, `pr_shape`,
  `teams`, `temporal`, `assignments`)
- Main data shape: parquet files under `data/`, produced upstream by
  [`queueboard-core`](https://github.com/leanprover-community/queueboard-core)
  (see "Upstream data source" below).
- Main usage modes:
  - library-style helpers (`qb_notebook.data_io`, `qb_notebook.filters`,
    `qb_notebook.intervals`, `qb_notebook.plotting`, plus the
    review-analysis helpers `qb_notebook.review_states`, `pr_shape`,
    `teams`, `temporal`, `assignments`)
  - notebook exploration:
    - Jupyter (`.ipynb`): `pr_merge_throughput`, `pr_open_durations`,
      `queue_windows`
    - marimo (`marimo/*.py`): reactive notebooks for the review-analysis
      project (reader's guide:
      `docs/review-analysis/notebooks.md`; background:
      `docs/review-analysis-plan.md`)
  - static plot generation (`qb_notebook/generate_plot_site.py`)

## Environment and Tooling

- Use `uv` for all Python commands in this repo.
- Initial setup:
  - `uv sync`
- Run tests:
  - `uv run pytest`
- Run a single test file:
  - `uv run pytest tests/test_intervals.py`
- Lint (matches CI exactly — run before pushing):
  - `uv run ruff check .`
- Format:
  - `uv run ruff format .`
- Format check (matches CI exactly — run before pushing):
  - `uv run ruff format --check .`

CI (`.github/workflows/ci.yml`, the `Ruff` job) runs **both** of these
repo-wide commands over the *entire* tree — not just `qb_notebook/`,
`tests/`, and `download_artifact.py`. That sweep includes the `marimo/*.py`
notebooks and the Jupyter `*.ipynb` notebooks (ruff formats notebook cells
by default). Scoping a local check to a subset of paths can pass while CI
fails, so always run the bare `.` form above before pushing.

Gotcha: the `Ruff` job runs `ruff check .` *before* `ruff format --check .`,
and a failed step aborts the job — so a lint error masks any pending format
failure in the CI log. After fixing a lint error, run the format check too;
the next CI run will reach it.

## Upstream Data Source

The parquet files under `data/` are the `analytics-datasets` artifact
produced by the `upload_backup.yaml` GitHub Actions workflow in
`queueboard-core` (scheduled daily at 06:00 UTC, also runnable on demand).

Upstream pipeline:
1. `scripts/download_backup.sh` pulls the latest Heroku PGBackups dump.
2. `pg_restore` loads it into a throwaway Postgres instance.
3. `scripts/sanitize_backup.py` truncates operational/secret-bearing tables
   and scrubs private fields, per
   `docs/design-decisions/016-sanitized-backups.md` in `queueboard-core`.
4. `scripts/export_for_analysis.py` exports the curated table set defined in
   `EXPORT_TABLE_QUERIES` (in `scripts/backup_policy.py`) as parquet.

Implications for `qb-notebook`:
- Treat `data/*.parquet` as a read-only view of a Django-managed Postgres
  schema. Column names match the Django model field names; FK columns are
  named `*_id`.
- Nullable integer FK columns arrive as Float64 (pandas/pyarrow conversion
  of Postgres `bigint NULL`); cast to `Int64` when joining. `data_io`
  already does this for the `analyzer_prqueuewindow` FK columns.
- Snapshot tables (`analyzer_queuesnapshot`, `analyzer_areastatssnapshot`,
  reviewer assignment / convergence snapshots, syncer metrics snapshots)
  are deliberately not exported — they were sanitized away. Don't add code
  that depends on them.

To refresh the local `data/` directory, call
`qb_notebook.artifacts.download_and_extract_latest_successful_workflow_artifacts`
(or run `download_artifact.py`, which is a compat shim around the same
function).

## Data Expectations

`qb_notebook.data_io.load_pr_interval_data(...)` expects these parquet files
in `data/` and returns a dict with the following keys:

| key               | parquet file                                  |
| ----------------- | --------------------------------------------- |
| `prs`             | `syncer_pullrequest.parquet`                  |
| `events`          | `syncer_prtimelineevent.parquet`              |
| `label_defs`      | `syncer_labeldef.parquet`                     |
| `prlabel`         | `syncer_prlabel.parquet`                      |
| `queue_windows`   | `analyzer_prqueuewindow.parquet`              |
| `check_runs`      | `syncer_commitcheckrun.parquet`               |
| `status_contexts` | `syncer_commitstatuscontext.parquet`          |
| `inline_comments` | `syncer_prreviewinlinecomment.parquet`¹       |

¹ Loaded only if present (artifact-dependent). Don't assume the key exists.

The `events` frame includes review-related event types added upstream
in queueboard-core #164 (2026-05): `ISSUE_COMMENTED`, `REVIEW_APPROVED`,
`REVIEW_COMMENTED`, `REVIEW_CHANGES_REQUESTED`, `REVIEW_DISMISSED`,
`REVIEW_REQUESTED`, `REVIEW_REQUEST_REMOVED`, alongside the older
`LABELED` / `UNLABELED` / `CLOSED` / etc. Comment bodies are **not**
exported.

The full set of parquet files currently present in `data/` is broader and
also includes: `analyzer_prdependency`, `analyzer_prdependencystate`,
`analyzer_prqueuewindowbuildstate`, `analyzer_prrevision`,
`analyzer_prrevisionbuildstate`, `analyzer_queueruleset`,
`core_repository`, `core_user`, `syncer_commithistoryharvest`,
`syncer_prreviewinlinecommentbackfill`, `syncer_repobackfillcursor`.
Read these directly with `qb_notebook.data_io._read_and_parse` (or
`pl.read_parquet` + `parse_datetime_columns`) when you need them.

When adding new analyses, prefer going through `load_pr_interval_data` and
existing helpers before introducing custom IO code. If you need a new table,
extend `load_pr_interval_data` rather than duplicating the parse-and-cast
logic in the notebook.

## Marimo Notebooks

See [`marimo/AGENTS.md`](marimo/AGENTS.md) for the full set of marimo
conventions and gotchas (notably the **512-byte header rule** — long
docstrings can hide a notebook from the Workspace pane).

Marimo notebooks live in `marimo/` as plain `.py` files (reactive cell DAG,
no JSON / output state — clean git diffs).

- Author / explore (auto-reloads on save):
  - `uv run marimo edit marimo/<name>.py`
- Serve read-only (good for sharing a snapshot):
  - `uv run marimo run marimo/<name>.py`
- Sanity-check the cell DAG and style:
  - `uv run marimo check marimo/<name>.py`

Conventions:

- Every notebook starts with a `sys.path` bootstrap that adds the repo root
  so `qb_notebook` is importable regardless of the launching cwd, and
  resolves `data/` against the repo root (not `Path.cwd()`).
- IO and any reusable transform belongs in `qb_notebook/*` modules, not in
  the notebook body. The notebook is for layout, UI controls, and plotting.
- Marimo enforces unique global variable names across cells. Use `_`-prefix
  for cell-private temporaries (e.g. `_fig`, `_ax`) and reserve unprefixed
  names for values that should flow into downstream cells.
- When a plot or table is the cell's "output", leave it as the last
  expression so marimo renders it; don't bury it in a print.

## Plot Site Workflow

- Entry point: `qb_notebook.generate_plot_site` (module execution
  recommended).
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

- Prefer Polars expressions and dataframe operations over pandas unless
  plotting conversion is needed.
- Reuse existing interval helpers:
  - `with_effective_end`
  - `effective_open_prs_per_day`
  - `effective_queue_prs_per_day`
  - `effective_queue_window_durations`
  - `snapshot_queue_age_quantiles`
- Keep regex/filter logic consistent with notebook semantics when porting
  notebook analyses into code.
- For "last 365 days" plot variants in this repo, compute the full timeseries
  first, then filter by `max(date/day) - 365 days`.
- The mathlib4 review queue is partitioned by `rule_set_id` on
  `analyzer_prqueuewindow`; `split_queue_windows_by_rule(...)` returns one
  frame per ruleset, and ruleset 3 is the one currently driving the
  dashboard plots.

## Testing Expectations

- If touching `qb_notebook/intervals.py`, `qb_notebook/filters.py`, or
  `qb_notebook/data_io.py`, add/update unit tests under `tests/`.
- For plot-site changes, at minimum run:
  - `uv run python -m py_compile qb_notebook/generate_plot_site.py`
  - `uv run python -m qb_notebook.generate_plot_site --data-dir data --site-dir /tmp/qb-plot-site-check`

## Known Gotchas

- Running `python qb_notebook/generate_plot_site.py` directly can fail due to
  import path issues; use module mode (`-m`) instead.
- Many helpers assume UTC-aware datetimes; preserve timezone handling when
  adding transformations.
- Polars `rolling_mean` is row-order-based, not time-based — always sort by
  date before taking a rolling mean over daily series.
- Nullable integer FK columns from Postgres arrive as Float64 in parquet;
  cast to `Int64` (or use the `_cast_float_to_nullable_int` helper) before
  joining.

# Queueboard notebook

Exploratory analysis helpers and notebooks for the sanitized parquet dump
produced by [`queueboard-core`](https://github.com/leanprover-community/queueboard-core).

The parquet files under `data/` are the curated set of tables exported daily
by the `upload_backup.yaml` workflow in `queueboard-core`: it downloads the
latest Heroku PG backup, sanitizes it
(see `docs/design-decisions/016-sanitized-backups.md` over there), and uploads
the result as the `analytics-datasets` artifact. This repo's tooling pulls
that artifact down for offline analysis.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/)
- [`gh`](https://cli.github.com/) (authenticated against
  `leanprover-community/queueboard-core`, only required for refreshing data)

## Quickstart

- Sync the environment: `uv sync`
- Create a venv (if your editor needs an explicit one): `uv venv`
- Open the `.ipynb` notebooks in VS Code (or `uv run jupyter lab`) and select
  the kernel corresponding to that venv.

## Notebooks

Jupyter (`.ipynb`):

- `pr_merge_throughput.ipynb` — daily/14-day-avg PR merge throughput.
- `pr_open_durations.ipynb` — distributions of PR open durations.
- `queue_windows.ipynb` — review-queue window timeseries and age quantiles.

marimo (`marimo/*.py`) — a suite of reactive notebooks analyzing
mathlib4's review process (reviewer load, bottlenecks, area health, PR
shape, lifecycle, and more). See the reader's guide at
[`docs/review-analysis/notebooks.md`](docs/review-analysis/notebooks.md)
for what each one answers, and [`marimo/AGENTS.md`](marimo/AGENTS.md) for
how to run them.

## Refreshing the parquet data

`qb_notebook.artifacts.download_and_extract_latest_successful_workflow_artifacts`
wraps `gh run download` for the upstream workflow:

```python
from qb_notebook.artifacts import download_and_extract_latest_successful_workflow_artifacts

info = download_and_extract_latest_successful_workflow_artifacts(
    repo="leanprover-community/queueboard-core",
    workflow="upload_backup.yaml",
    out_dir="./data",
    artifact_name="analytics-datasets",
    branch="master",
    search_limit=100,
)
```

`download_artifact.py` at the repo root is a thin compatibility shim around
the same function, kept so older notebooks that imported it still work.

## Loading the data

```python
from qb_notebook.data_io import load_pr_interval_data

tables = load_pr_interval_data("data")
# keys: prs, events, label_defs, prlabel, queue_windows, check_runs,
#       status_contexts (+ inline_comments when present in the artifact)
```

`load_pr_interval_data` parses the queueboard datetime columns into UTC
`Datetime("us")` and casts the nullable-integer FK columns on
`analyzer_prqueuewindow` from Float64 back to Int64.

## Schema variants

- `qb_notebook.data_io.DEFAULT_DATETIME_COLUMNS` is a queueboard-oriented
  default, not a universal schema contract.
- If your dataset has different datetime columns, pass `datetime_columns=`
  explicitly.
- Missing columns in the configured list are ignored by
  `parse_datetime_columns`.

```python
import polars as pl
from qb_notebook.data_io import parse_datetime_columns

df_raw = pl.DataFrame(
    {
        "created_at": ["2025-02-01 10:00:00.000000+00:00"],
        "custom_ts": ["2025-02-01 11:30:00.000000+00:00"],
    }
)

df = parse_datetime_columns(
    df_raw,
    datetime_columns=["created_at", "custom_ts"],
)
```

See schema maintenance notes: [`docs/schema-notes.md`](docs/schema-notes.md).

## Filtering helpers

- Use `qb_notebook.filters` to build composable Polars expressions.
- Use `filter_rows(df, *exprs)` to combine multiple filter expressions.
- Most helpers support optional column names so the same logic can be reused
  across schema variants.

```python
from qb_notebook.filters import (
    expr_interval_started_between,
    expr_repo_in,
    expr_title_regex,
    filter_rows,
)

out = filter_rows(
    df,
    expr_repo_in([123, 456], repo_col="repository_id"),
    expr_title_regex(r"^feat", title_col="title"),
    expr_interval_started_between(
        start_after="2025-01-01",
        start_before="2026-01-01",
        start_col="start",
    ),
)
```

More filtering examples and conventions:
[`docs/filtering.md`](docs/filtering.md).

## Interval helpers

- `qb_notebook.intervals` separates raw interval endpoints from effective
  closed intervals.
- Use `with_effective_end(...)` as the explicit conversion step when null
  ends must be closed for computation.
- Prefer `effective_*` / `snapshot_*` functions for duration/time-series
  calculations that require non-null interval ends.

```python
from datetime import datetime, timezone
from qb_notebook.intervals import (
    effective_open_prs_per_day,
    with_effective_end,
)

asof = datetime.now(tz=timezone.utc)

intervals_eff = with_effective_end(
    intervals,
    end_col="end",
    effective_end_col="end_effective_ts",
    asof=asof,
)

daily = effective_open_prs_per_day(
    intervals_eff,
    start_col="start",
    effective_end_col="end_effective_ts",
    asof=asof,
)
```

More interval conventions and examples:
[`docs/intervals.md`](docs/intervals.md).

## Plotting helpers

- `qb_notebook.plotting` contains reusable plotting utilities for
  distributions and interval visualizations.
- Most plotting functions expect a Polars `DataFrame` and a numeric duration
  column (default: `duration_days`).
- Distribution-fit functions return fitted parameters (or `None` if
  insufficient data).

```python
from qb_notebook.plotting import (
    plot_duration_hist,
    plot_lognormal_fit_counts_logbins,
)

plot_duration_hist(df, col="duration_days", bins=100, logx=True)
params = plot_lognormal_fit_counts_logbins(df, col="duration_days", bins=100)
print(params)
```

More plotting examples and conventions:
[`docs/plotting.md`](docs/plotting.md).

## Static plot site

`qb_notebook.generate_plot_site` builds a self-contained HTML page with the
core queue-window plots:

```
uv run python -m qb_notebook.generate_plot_site --data-dir data --site-dir _site
```

Writes `_site/index.html` and `_site/images/*.png`. Run as a module (`-m`);
running the file path directly can fail due to import path issues.

## Included tools

- pandas/pyarrow and polars for parquet IO and data wrangling
- matplotlib, altair, seaborn, plotly for plotting
- scipy and statsmodels for statistical tests/modeling
- jupyterlab and ipykernel for notebooks

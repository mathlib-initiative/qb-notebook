# Queueboard notebook

Utilities and dependencies for exploring the sanitized parquet dump under `local-sanitize/data`.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/)
- [`gh`](https://cli.github.com/)

## Quickstart
- Sync the environment: `uv sync`
- Create a venv: `uv venv`
- Open `.ipynb` notebooks VS Code (or `uv run jupyter lab`) and select the kernel corresponding to the venv just created

## Useful snippets
```python
from qb_notebook.artifacts import download_and_extract_latest_successful_workflow_artifacts
from pathlib import Path
import pandas as pd
import polars as pl

info = download_and_extract_latest_successful_workflow_artifacts(
    repo="leanprover-community/queueboard-core",
    workflow="upload_backup.yaml",
    out_dir="./data",
    artifact_name="analytics-datasets",
    branch="master",
    search_limit=100,  # change this if you expect there to be > 100 failed runs before the first successful one
)

data_dir = Path("data")

# Pandas + PyArrow
df = pd.read_parquet(data_dir / "core_repository.parquet")

# Polars (fast, lazy)
lazy = pl.scan_parquet(data_dir.glob("*.parquet"))
agg = lazy.group_by("owner_login").agg(pl.len()).collect()
print(agg)
```

## Included tools
- pandas/pyarrow and polars for parquet IO and data wrangling
- matplotlib, altair, seaborn, plotly for plotting
- scipy and statsmodels for statistical tests/modeling
- jupyterlab and ipykernel for notebooks

## Schema variants
- `qb_notebook.data_io.DEFAULT_DATETIME_COLUMNS` is a queueboard-oriented default, not a universal schema contract.
- If your dataset has different datetime columns, pass `datetime_columns=` explicitly.
- Missing columns in the configured list are ignored by `parse_datetime_columns`.

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
- Most helpers support optional column names so the same logic can be reused across schema variants.

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
- `qb_notebook.intervals` separates raw interval endpoints from effective closed intervals.
- Use `with_effective_end(...)` as the explicit conversion step when null ends must be closed for computation.
- Prefer `effective_*` / `snapshot_*` functions for duration/time-series calculations that require non-null interval ends.

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
- `qb_notebook.plotting` contains reusable plotting utilities for distributions and interval visualizations.
- Most plotting functions expect a Polars `DataFrame` and a numeric duration column (default: `duration_days`).
- Distribution-fit functions return fitted parameters (or `None` if insufficient data).

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

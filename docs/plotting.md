# Plotting Guide

`qb_notebook.plotting` provides reusable plot builders for interval timelines and duration distributions.

## Function groups

### Interval timeline plots
- `prepare_swimlane_polars(...)`
- `plot_swimlane_matplotlib(...)`

Use these for PR interval swimlane charts. `prepare_swimlane_polars` computes y-ordering and effective ends.

### Duration histograms
- `plot_duration_hist(...)`
- `plot_duration_hists(...)`

Use these for one or multiple datasets. Optional percentile annotations are available in `plot_duration_hists`.

### Distribution fit overlays
- `plot_lognormal_fit_counts_logbins(...)`
- `plot_weibull_fit_counts_logbins(...)`
- `plot_loglogistic_fit_counts_logbins(...)`
- `plot_hist_and_lognormal_fit_overlays(...)`

The single-dataset fit functions return fitted parameters (or `None` when there is not enough data).

## Data expectations

- Inputs are Polars `DataFrame`s.
- Default numeric column is `duration_days`.
- Helper `get_x(df, col)` filters out non-finite and non-positive values.

## Basic usage

```python
from qb_notebook.plotting import (
    plot_duration_hist,
    plot_lognormal_fit_counts_logbins,
)

plot_duration_hist(df, col="duration_days", bins=100, logx=True)
params = plot_lognormal_fit_counts_logbins(df, col="duration_days", bins=100)
print(params)
```

## Multi-dataset overlays

```python
from qb_notebook.plotting import plot_hist_and_lognormal_fit_overlays

plot_hist_and_lognormal_fit_overlays(
    [df_2023, df_2024, df_2025],
    ["2023", "2024", "2025"],
    col="duration_days",
    bins=100,
)
```

## Notes for tests/CI

- Plot functions call `plt.show()`.
- In tests, use a non-interactive backend (for example `matplotlib.use("Agg")`) to avoid GUI requirements.

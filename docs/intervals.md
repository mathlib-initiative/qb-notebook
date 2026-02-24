# Intervals Guide

`qb_notebook.intervals` distinguishes two concepts:

- Raw interval end: may be null (`open` interval).
- Effective interval end: always non-null (`closed for computation`).

## Naming convention

- Raw columns: `start_ts`, `end_ts` (or current dataset equivalents like `start`, `to_ts`).
- Effective columns: `end_effective_ts` (or `to_ts_effective`).

Functions follow this convention:
- Preserve open intervals: `build_*`, `enrich_*`.
- Require closed/effective intervals: `effective_*`, `snapshot_*`.

## Explicit conversion step

Use `with_effective_end(...)` to close open intervals for computation:

```python
from datetime import datetime, timezone
from qb_notebook.intervals import with_effective_end

asof = datetime.now(tz=timezone.utc)

intervals_eff = with_effective_end(
    intervals,
    end_col="end",
    effective_end_col="end_effective_ts",
    asof=asof,
)
```

If `asof` is omitted, you must provide `null_end_fallback_col` (for example `updated_at`).

## Common workflows

### PR open intervals

```python
from qb_notebook.intervals import build_pr_open_intervals, effective_open_prs_per_day

intervals = build_pr_open_intervals(df_prs, df_events)
daily_open = effective_open_prs_per_day(intervals)
```

### Queue windows per day

```python
from qb_notebook.intervals import effective_queue_prs_per_day

daily_queue = effective_queue_prs_per_day(df_qw)
```

### Queue window durations and snapshot quantiles

```python
from qb_notebook.intervals import (
    effective_queue_window_durations,
    snapshot_queue_age_quantiles,
)

dur = effective_queue_window_durations(df_qw)
qs = snapshot_queue_age_quantiles(df_qw, quantiles=[0.75, 0.9])
```

## Compatibility wrappers

For notebook compatibility, these wrappers are still available:
- `open_prs_per_day` -> `effective_open_prs_per_day`
- `queue_prs_per_day` -> `effective_queue_prs_per_day`
- `with_queue_window_durations` -> `effective_queue_window_durations`
- `compute_quantiles` -> `snapshot_queue_age_quantiles`

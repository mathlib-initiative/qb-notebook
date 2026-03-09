from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

# `%#z` parses offsets like +00:00 in the current Polars/chrono combo.
_DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %T%.f%#z"

# Queueboard-focused default datetime columns.
# This tuple is intentionally not universal; callers should pass
# `datetime_columns=` when using different schemas.
DEFAULT_DATETIME_COLUMNS: tuple[str, ...] = (
    "created_at",
    "updated_at",
    "gh_created_at",
    "gh_updated_at",
    "last_synced_at",
    "commits_earliest_synced_at",
    "engagement_synced_at",
    "occurred_at",
    "closed_at",
    "merged_at",
    "from_ts",
    "to_ts",
    "first_on_queue_ts",
)


def parse_datetime_columns(
    df: pl.DataFrame,
    *,
    datetime_columns: Iterable[str] = DEFAULT_DATETIME_COLUMNS,
    fmt: str = _DEFAULT_DATETIME_FORMAT,
) -> pl.DataFrame:
    """
    Parse selected datetime string columns into UTC microsecond timestamps.

    Notes:
    - `DEFAULT_DATETIME_COLUMNS` matches queueboard parquet exports and may evolve.
    - Missing columns are ignored (no hard failure on schema drift).
    - For non-queueboard shapes, pass a custom `datetime_columns` iterable.
    """
    cols_present = [c for c in datetime_columns if c in df.columns]
    if not cols_present:
        return df

    return df.with_columns(
        [
            pl.col(c)
            .str.strptime(pl.Datetime("us", "UTC"), format=fmt, strict=False)
            .alias(c)
            for c in cols_present
        ]
    )


def parse_dt_cols(df_raw: pl.DataFrame) -> pl.DataFrame:
    """Backwards-compatible alias used by the notebook."""
    return parse_datetime_columns(df_raw)


def _read_and_parse(path: Path) -> pl.DataFrame:
    return parse_datetime_columns(pl.read_parquet(path))


def load_pr_interval_data(data_dir: str | Path = "data") -> dict[str, pl.DataFrame]:
    """
    Load and parse core queueboard tables used by pr_intervals analyses.

    Expected files under `data_dir`:
    - syncer_pullrequest.parquet
    - syncer_prtimelineevent.parquet
    - syncer_labeldef.parquet
    - syncer_prlabel.parquet
    - analyzer_prqueuewindow.parquet
    """
    root = Path(data_dir)
    return {
        "prs": _read_and_parse(root / "syncer_pullrequest.parquet"),
        "events": _read_and_parse(root / "syncer_prtimelineevent.parquet"),
        "label_defs": _read_and_parse(root / "syncer_labeldef.parquet"),
        "prlabel": _read_and_parse(root / "syncer_prlabel.parquet"),
        "queue_windows": _read_and_parse(root / "analyzer_prqueuewindow.parquet"),
    }


def split_queue_windows_by_rule(
    df_queue_windows: pl.DataFrame,
    *,
    rule_set_ids: Iterable[int] = (1, 2, 3),
) -> dict[int, pl.DataFrame]:
    """Return filtered queue-window frames keyed by rule_set_id."""
    return {
        rule_set_id: df_queue_windows.filter(pl.col("rule_set_id") == rule_set_id)
        for rule_set_id in rule_set_ids
    }

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import polars as pl


def _resolve_asof(asof: datetime | None) -> datetime:
    if asof is None:
        return datetime.now(tz=timezone.utc)
    if asof.tzinfo is None:
        return asof.replace(tzinfo=timezone.utc)
    return asof.astimezone(timezone.utc)


def with_effective_end(
    df: pl.DataFrame,
    *,
    end_col: str,
    effective_end_col: str = "end_effective_ts",
    asof: datetime | None = None,
    null_end_fallback_col: str | None = None,
) -> pl.DataFrame:
    """
    Add a non-null effective end timestamp for interval computations.

    Semantics:
    - `end_col` is the raw interval end and may be null for open intervals.
    - `effective_end_col` is always non-null.
    - Null raw ends use `asof` if given, else `null_end_fallback_col`.
    """
    if asof is None and null_end_fallback_col is None:
        raise ValueError(
            "Provide either asof or null_end_fallback_col for null end values."
        )

    if asof is not None:
        fallback = pl.lit(_resolve_asof(asof))
    else:
        fallback = pl.col(null_end_fallback_col)  # type: ignore[arg-type]

    return df.with_columns(
        [
            pl.coalesce([pl.col(end_col), fallback]).alias(effective_end_col),
            pl.col(end_col).is_null().alias("is_open_ended"),
        ]
    )


def build_pr_open_intervals(
    df_prs: pl.DataFrame,
    df_events: pl.DataFrame,
    *,
    asof: datetime | None = None,
) -> pl.DataFrame:
    """
    Build PR open intervals from PR creation + CLOSED/REOPENED events.

    Output columns include:
    - pull_request_id
    - start
    - end
    - is_open_ended
    - end_effective
    - duration / duration_hours / duration_days
    """
    asof_dt = _resolve_asof(asof)

    prs_changes = (
        df_prs.select(
            [
                pl.col("id").alias("pull_request_id"),
                pl.col("gh_created_at").alias("occurred_at"),
            ]
        )
        .with_columns(
            [
                pl.lit("CREATED").alias("etype"),
                pl.lit(1, dtype=pl.Int32).alias("change"),
                pl.lit(0, dtype=pl.Int32).alias("order"),
            ]
        )
        .drop_nulls(["occurred_at"])
    )

    ev_changes = (
        df_events.filter(pl.col("type").is_in(["CLOSED", "REOPENED"]))
        .select(
            [
                pl.col("pull_request_id"),
                pl.col("occurred_at"),
                pl.col("type").alias("etype"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("etype") == "CLOSED")
                .then(-1)
                .otherwise(1)
                .cast(pl.Int32)
                .alias("change"),
                pl.when(pl.col("etype") == "REOPENED")
                .then(1)
                .otherwise(2)
                .cast(pl.Int32)
                .alias("order"),
            ]
        )
        .drop_nulls(["occurred_at"])
    )

    changes = (
        pl.concat([prs_changes, ev_changes], how="vertical")
        .sort(["pull_request_id", "occurred_at", "order"])
        .with_columns(
            [pl.col("change").cum_sum().over("pull_request_id").alias("open_count")]
        )
        .with_columns(
            [
                pl.col("open_count")
                .shift(1)
                .over("pull_request_id")
                .fill_null(0)
                .alias("prev_open_count")
            ]
        )
        .with_columns(
            [
                ((pl.col("prev_open_count") <= 0) & (pl.col("open_count") > 0)).alias(
                    "start_flag"
                ),
                ((pl.col("prev_open_count") > 0) & (pl.col("open_count") <= 0)).alias(
                    "end_flag"
                ),
            ]
        )
        .with_columns(
            [
                pl.col("start_flag")
                .cast(pl.Int32)
                .cum_sum()
                .over("pull_request_id")
                .alias("start_idx"),
                pl.col("end_flag")
                .cast(pl.Int32)
                .cum_sum()
                .over("pull_request_id")
                .alias("end_idx"),
            ]
        )
    )

    starts = changes.filter(pl.col("start_flag")).select(
        [
            pl.col("pull_request_id"),
            pl.col("start_idx").alias("interval_idx"),
            pl.col("occurred_at").alias("start"),
        ]
    )
    ends = changes.filter(pl.col("end_flag")).select(
        [
            pl.col("pull_request_id"),
            pl.col("end_idx").alias("interval_idx"),
            pl.col("occurred_at").alias("end"),
        ]
    )

    intervals = (
        starts.join(ends, on=["pull_request_id", "interval_idx"], how="left")
        .select(["pull_request_id", "start", "end"])
        .sort(["pull_request_id", "start"])
        .with_columns(
            [
                pl.col("end").is_null().alias("is_open_ended"),
                pl.coalesce([pl.col("end"), pl.lit(asof_dt)]).alias("end_effective"),
            ]
        )
        .with_columns([(pl.col("end_effective") - pl.col("start")).alias("duration")])
        .with_columns(
            [
                (pl.col("duration").dt.total_seconds() / 3600.0).alias(
                    "duration_hours"
                ),
                (pl.col("duration").dt.total_seconds() / 86400.0).alias(
                    "duration_days"
                ),
            ]
        )
    )
    return intervals


def enrich_intervals_with_prs(
    intervals: pl.DataFrame,
    prs: pl.DataFrame,
    *,
    pr_id_col: str = "pull_request_id",
    prs_id_col: str = "id",
    pr_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Left-join PR metadata onto interval rows."""
    if pr_cols is None:
        pr_cols = [
            "number",
            "state",
            "is_draft",
            "repository_id",
            "author_id",
            "base_ref_name",
            "head_ref_name",
            "title",
            "additions",
            "deletions",
            "changed_files_count",
            "gh_created_at",
            "closed_at",
            "merged_at",
        ]

    cols_present = [c for c in pr_cols if c in prs.columns]
    prs_small = prs.select(
        [pl.col(prs_id_col).alias(pr_id_col), *[pl.col(c) for c in cols_present]]
    )
    return intervals.join(prs_small, on=pr_id_col, how="left")


def effective_open_prs_per_day(
    intervals: pl.DataFrame,
    *,
    start_col: str = "start",
    end_col: str = "end",
    effective_end_col: str = "end_effective",
    asof: datetime | None = None,
) -> pl.DataFrame:
    """
    Daily open-PR counts using effective closed intervals.

    If `effective_end_col` is absent, it is derived from `end_col` + `asof`.
    """
    asof_dt = _resolve_asof(asof)

    df = intervals
    if effective_end_col not in df.columns:
        df = with_effective_end(
            df,
            end_col=end_col,
            effective_end_col=effective_end_col,
            asof=asof_dt,
        )

    asof_lit = pl.lit(asof_dt)
    df = (
        df.with_columns([pl.col(start_col), pl.col(effective_end_col)])
        .with_columns(
            [
                pl.when(pl.col(effective_end_col) > asof_lit)
                .then(asof_lit)
                .otherwise(pl.col(effective_end_col))
                .alias(effective_end_col)
            ]
        )
        .with_columns(
            [
                pl.col(start_col).dt.date().alias("start_date"),
                pl.col(effective_end_col).dt.date().alias("end_date"),
            ]
        )
    )

    starts = (
        df.group_by("start_date")
        .agg(pl.len().cast(pl.Int64).alias("delta"))
        .rename({"start_date": "date"})
    )
    ends = (
        df.with_columns((pl.col("end_date") + pl.duration(days=1)).alias("date"))
        .group_by("date")
        .agg((-pl.len().cast(pl.Int64)).alias("delta"))
    )

    deltas = (
        pl.concat([starts, ends], how="vertical")
        .group_by("date")
        .agg(pl.sum("delta").alias("delta"))
        .sort("date")
    )

    min_date = df.select(pl.min("start_date")).item()
    max_date = df.select(pl.max("end_date")).item()
    all_days = pl.DataFrame(
        {"date": pl.date_range(min_date, max_date, interval="1d", eager=True)}
    )

    return (
        all_days.join(deltas, on="date", how="left")
        .with_columns(pl.col("delta").fill_null(0))
        .sort("date")
        .with_columns(pl.col("delta").cum_sum().alias("open_prs"))
    )


def effective_queue_prs_per_day(
    df_queue_windows: pl.DataFrame,
    *,
    start_col: str = "from_ts",
    end_col: str = "to_ts",
    fallback_end_col: str = "updated_at",
    asof: datetime | None = None,
    effective_end_col: str = "to_ts_effective",
) -> pl.DataFrame:
    """Daily unique PR counts on queue using effective closed intervals."""
    if asof is not None:
        df = with_effective_end(
            df_queue_windows,
            end_col=end_col,
            effective_end_col=effective_end_col,
            asof=asof,
        )
    else:
        df = with_effective_end(
            df_queue_windows,
            end_col=end_col,
            effective_end_col=effective_end_col,
            null_end_fallback_col=fallback_end_col,
        )

    return (
        df.select(
            [
                pl.col("pull_request_id"),
                pl.col(start_col).dt.date().alias("start_day"),
                (pl.col(effective_end_col) - pl.duration(microseconds=1))
                .dt.date()
                .alias("end_day"),
            ]
        )
        .filter(pl.col("end_day") >= pl.col("start_day"))
        .with_columns(
            pl.date_ranges(
                pl.col("start_day"),
                pl.col("end_day"),
                interval="1d",
                closed="both",
            ).alias("day")
        )
        .explode("day")
        .group_by("day")
        .agg(pl.col("pull_request_id").n_unique().alias("prs_on_queue"))
        .sort("day")
    )


def effective_queue_window_durations(
    df_queue_windows: pl.DataFrame,
    *,
    start_col: str = "from_ts",
    end_col: str = "to_ts",
    fallback_end_col: str = "updated_at",
    asof: datetime | None = None,
    effective_end_col: str = "to_ts_effective",
    clamp_nonpositive: bool = True,
) -> pl.DataFrame:
    """
    Add status and duration columns for queue windows with effective closed ends.

    `status` is based on the raw end column (`open` when null).
    """
    if asof is not None:
        out = with_effective_end(
            df_queue_windows,
            end_col=end_col,
            effective_end_col=effective_end_col,
            asof=asof,
        )
    else:
        out = with_effective_end(
            df_queue_windows,
            end_col=end_col,
            effective_end_col=effective_end_col,
            null_end_fallback_col=fallback_end_col,
        )

    out = out.with_columns(
        [
            pl.when(pl.col(end_col).is_null())
            .then(pl.lit("open"))
            .otherwise(pl.lit("closed"))
            .alias("status"),
            ((pl.col(effective_end_col) - pl.col(start_col)).dt.total_seconds())
            .cast(pl.Float64)
            .alias("duration_seconds"),
        ]
    ).with_columns((pl.col("duration_seconds") / 86400.0).alias("duration_days"))

    if clamp_nonpositive:
        out = out.filter(pl.col("duration_seconds") > 0)
    return out


def snapshot_queue_age_quantiles(
    df_queue_windows: pl.DataFrame,
    quantiles: Iterable[float],
    *,
    start_col: str = "from_ts",
    end_col: str = "to_ts",
    fallback_end_col: str = "updated_at",
    asof: datetime | None = None,
    effective_end_col: str = "to_ts_effective",
) -> pl.DataFrame:
    """
    Compute per-day age quantiles at end-of-day snapshots for queue windows.

    Uses effective closed windows where null ends are closed by `asof` or `fallback_end_col`.
    """
    if asof is not None:
        w = with_effective_end(
            df_queue_windows,
            end_col=end_col,
            effective_end_col=effective_end_col,
            asof=asof,
        )
    else:
        w = with_effective_end(
            df_queue_windows,
            end_col=end_col,
            effective_end_col=effective_end_col,
            null_end_fallback_col=fallback_end_col,
        )

    daily = (
        w.with_columns(
            [
                pl.col(start_col).dt.date().alias("from_date"),
                pl.col(effective_end_col).dt.date().alias("to_date"),
            ]
        )
        .with_columns(
            pl.date_ranges(
                start=pl.col("from_date"),
                end=pl.col("to_date"),
                interval="1d",
                closed="both",
            ).alias("date")
        )
        .explode("date")
        .with_columns(
            (
                (pl.col("date").cast(pl.Datetime("us", "UTC")) + pl.duration(days=1))
                - pl.col(start_col)
            ).alias("age")
        )
        .with_columns((pl.col("age").dt.total_seconds() / 86400.0).alias("age_days"))
    )

    qs = list(quantiles)
    return (
        daily.group_by("date")
        .agg(
            [
                pl.col("age_days")
                .quantile(q, interpolation="nearest")
                .alias(f"p{int(q * 100)}")
                for q in qs
            ]
        )
        .sort("date")
    )

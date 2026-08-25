"""Time-of-day, day-of-week, and seasonality helpers (all UTC).

These underpin Session 14 (`marimo/temporal_patterns.py`) — the first
systematic temporal analysis in the repo. The mathlib4 community is
global but the queueboard ingest preserves only UTC timestamps, so the
analyses keep a UTC clock axis and label it explicitly.

:func:`with_temporal_columns` decorates any frame that carries a
datetime column with hour-of-day, weekday, is-weekend, month, year, and
year-month derivations so groupbys can pivot on time-bins without
re-writing the dt expression in every cell.
:func:`weekday_hour_histogram` produces the dense 7×24 = 168-cell count
frame the heatmaps render off, zero-filling empty cells so axes are
stable across different event streams.
:func:`actor_activity_window` infers each actor's best
``window_hours``-long contiguous UTC window from the empirical
distribution of their event timestamps — a cheap proxy for "what
timezone is this person in" that supports the author-vs-reviewer
activity-overlap cut. :func:`hour_set_overlap` is the trivial
pairwise comparator for that cut.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import polars as pl

from qb_notebook.review_states import DEFAULT_BOT_ACTORS, bot_actor_expr

# Polars dt.weekday() returns 1..7 (Mon=1, Sun=7). All helpers in this
# module normalise to 0..6 (Mon=0, Sun=6) so downstream code matches
# Python's datetime.weekday() and numpy plotting conventions.
WEEKDAY_LABELS: tuple[str, ...] = (
    "Mon",
    "Tue",
    "Wed",
    "Thu",
    "Fri",
    "Sat",
    "Sun",
)
MONTH_LABELS: tuple[str, ...] = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)
# Sat = 5, Sun = 6 in the 0..6 convention.
WEEKEND_DAYS: frozenset[int] = frozenset({5, 6})


def with_temporal_columns(
    df: pl.DataFrame,
    ts_col: str,
    *,
    prefix: str = "",
) -> pl.DataFrame:
    """Append UTC temporal-decoration columns derived from ``ts_col``.

    Adds (with optional ``prefix``):

    - ``hour_utc``   ``Int8`` 0-23
    - ``weekday``    ``Int8`` 0-6 (Mon=0, Sun=6)
    - ``is_weekend`` ``Boolean`` (weekday in {5, 6})
    - ``month``      ``Int8`` 1-12
    - ``year``       ``Int32``
    - ``year_month`` ``Date`` (first of the month)

    Rows with null ``ts_col`` produce nulls in every derived column. Pass
    distinct prefixes to decorate the same frame with multiple
    timestamps (e.g. ``"open_"`` vs ``"merge_"``).
    """
    ts = pl.col(ts_col)
    weekday0 = (ts.dt.weekday() - 1).cast(pl.Int8)
    return df.with_columns(
        ts.dt.hour().cast(pl.Int8).alias(f"{prefix}hour_utc"),
        weekday0.alias(f"{prefix}weekday"),
        (weekday0 >= 5).alias(f"{prefix}is_weekend"),
        ts.dt.month().cast(pl.Int8).alias(f"{prefix}month"),
        ts.dt.year().cast(pl.Int32).alias(f"{prefix}year"),
        ts.dt.truncate("1mo").dt.date().alias(f"{prefix}year_month"),
    )


def weekday_hour_histogram(
    df: pl.DataFrame,
    *,
    ts_col: str = "occurred_at",
) -> pl.DataFrame:
    """Count rows per UTC (weekday, hour_utc) bin; always emits 168 cells.

    Returns a frame with columns ``weekday`` (Int8 0-6), ``hour_utc``
    (Int8 0-23), and ``count`` (Int64). Empty bins are zero-filled so
    heatmap rendering doesn't need to reindex.

    The caller is responsible for any row-level filtering (event type,
    bot exclusion, date window) before passing the frame in — keeping
    the histogram helper schema-agnostic so it works on events, PRs,
    attribution outputs, and any other per-row-keyed frame.
    """
    decorated = (
        with_temporal_columns(df.drop_nulls([ts_col]), ts_col)
        .group_by(["weekday", "hour_utc"])
        .agg(pl.len().alias("count"))
    )
    grid = pl.DataFrame(
        {"weekday": pl.int_range(0, 7, dtype=pl.Int8, eager=True)}
    ).join(
        pl.DataFrame({"hour_utc": pl.int_range(0, 24, dtype=pl.Int8, eager=True)}),
        how="cross",
    )
    return (
        grid.join(decorated, on=["weekday", "hour_utc"], how="left")
        .with_columns(pl.col("count").fill_null(0).cast(pl.Int64))
        .sort(["weekday", "hour_utc"])
    )


def actor_activity_window(
    df_events: pl.DataFrame,
    *,
    window_hours: int = 8,
    min_events: int = 20,
    event_types: Sequence[str] | None = None,
    exclude_bots: bool = True,
    bot_actors: Iterable[str] = DEFAULT_BOT_ACTORS,
    ts_col: str = "occurred_at",
    actor_col: str = "actor_login",
) -> pl.DataFrame:
    """Per-actor inferred ``window_hours``-long UTC active window.

    For each actor with at least ``min_events`` qualifying events, build
    a 24-bin hour-of-day histogram and find the contiguous
    ``window_hours``-hour window of UTC time with the most events
    (treating the 24-hour clock as circular so windows can cross
    midnight). Returns one row per qualifying actor with:

    - ``actor_login``       the actor (column name follows ``actor_col``)
    - ``n_events``          total qualifying events for this actor
    - ``peak_hour``         single UTC hour with the most events (0-23)
    - ``window_start``      start hour of the best contiguous window
    - ``window_end``        end hour, exclusive, mod 24
    - ``active_hours``      hours in the window as ``list[int]`` (length
                            ``window_hours``); may wrap across midnight
    - ``active_hours_share``fraction of the actor's events that fall in
                            ``active_hours`` — higher = sharper window

    The "window" framing maps cleanly to "what timezone is this actor
    in" — most active-hour clusters are 8–10 hours wide and roughly
    aligned to local waking hours. Use :func:`hour_set_overlap` to
    compare windows pairwise.

    Empty input or no actors passing ``min_events`` yields an empty
    DataFrame with the expected schema.
    """
    if not 1 <= window_hours <= 24:
        raise ValueError("window_hours must be in [1, 24]")

    df = df_events.drop_nulls([ts_col, actor_col])
    if event_types is not None:
        df = df.filter(pl.col("type").is_in(list(event_types)))
    if exclude_bots:
        df = df.filter(~bot_actor_expr(df, bot_actors=bot_actors, actor_col=actor_col))

    histogram = (
        df.with_columns(pl.col(ts_col).dt.hour().cast(pl.Int64).alias("__hour"))
        .group_by([actor_col, "__hour"])
        .agg(pl.len().alias("__count"))
        .sort([actor_col, "__hour"])
    )

    output_rows: list[dict[str, object]] = []
    for actor_key, group in histogram.group_by([actor_col], maintain_order=True):
        actor = actor_key[0]
        counts = [0] * 24
        for hour, count in zip(group["__hour"].to_list(), group["__count"].to_list()):
            counts[int(hour)] = int(count)
        n_events = sum(counts)
        if n_events < min_events:
            continue
        best_start = 0
        best_sum = -1
        for start in range(24):
            window_sum = sum(counts[(start + i) % 24] for i in range(window_hours))
            if window_sum > best_sum:
                best_sum = window_sum
                best_start = start
        peak_hour = max(range(24), key=lambda h: counts[h])
        active_hours = [(best_start + i) % 24 for i in range(window_hours)]
        output_rows.append(
            {
                actor_col: actor,
                "n_events": n_events,
                "peak_hour": peak_hour,
                "window_start": best_start,
                "window_end": (best_start + window_hours) % 24,
                "active_hours": active_hours,
                "active_hours_share": best_sum / n_events,
            }
        )

    schema: dict[str, pl.DataType] = {
        actor_col: pl.String,
        "n_events": pl.Int64,
        "peak_hour": pl.Int64,
        "window_start": pl.Int64,
        "window_end": pl.Int64,
        "active_hours": pl.List(pl.Int64),
        "active_hours_share": pl.Float64,
    }
    return pl.DataFrame(output_rows, schema=schema)


def hour_set_overlap(
    hours_a: Sequence[int],
    hours_b: Sequence[int],
) -> int:
    """Count of common hours (treated as a set) between two hour sequences.

    Hours outside 0-23 are not validated — both inputs are assumed to
    have come from :func:`actor_activity_window`'s ``active_hours``.
    """
    return len(set(hours_a) & set(hours_b))

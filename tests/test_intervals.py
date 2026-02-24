from datetime import datetime, timezone

import polars as pl

from qb_notebook.intervals import (
    effective_queue_window_durations,
    snapshot_queue_age_quantiles,
    with_effective_end,
)


def test_with_effective_end_uses_asof_for_open_intervals() -> None:
    df = pl.DataFrame(
        {
            "from_ts": [
                datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc),
                datetime(2025, 2, 1, 12, 0, tzinfo=timezone.utc),
            ],
            "to_ts": [None, datetime(2025, 2, 1, 13, 0, tzinfo=timezone.utc)],
        }
    )
    asof = datetime(2025, 2, 1, 15, 0, tzinfo=timezone.utc)

    out = with_effective_end(
        df, end_col="to_ts", effective_end_col="to_ts_effective", asof=asof
    )

    assert out["to_ts_effective"][0] == asof
    assert out["to_ts_effective"][1] == datetime(2025, 2, 1, 13, 0, tzinfo=timezone.utc)
    assert out["is_open_ended"].to_list() == [True, False]


def test_effective_queue_window_durations_preserves_open_closed_status() -> None:
    df = pl.DataFrame(
        {
            "from_ts": [
                datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc),
                datetime(2025, 2, 1, 12, 0, tzinfo=timezone.utc),
            ],
            "to_ts": [None, datetime(2025, 2, 1, 14, 0, tzinfo=timezone.utc)],
            "updated_at": [
                datetime(2025, 2, 1, 16, 0, tzinfo=timezone.utc),
                datetime(2025, 2, 1, 16, 0, tzinfo=timezone.utc),
            ],
        }
    )

    out = effective_queue_window_durations(df)

    assert out["status"].to_list() == ["open", "closed"]
    assert out["duration_seconds"].to_list() == [21600.0, 7200.0]


def test_snapshot_queue_age_quantiles_uses_effective_end() -> None:
    df = pl.DataFrame(
        {
            "from_ts": [datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc)],
            "to_ts": [None],
            "updated_at": [datetime(2025, 2, 2, 0, 0, tzinfo=timezone.utc)],
        }
    )

    out = snapshot_queue_age_quantiles(df, quantiles=[0.75, 0.9])

    assert out.height == 2
    assert out["p75"].to_list() == [1.0, 2.0]
    assert out["p90"].to_list() == [1.0, 2.0]

import polars as pl

import json
import os
import tempfile

from qb_notebook.data_io import (
    DEFAULT_DATETIME_COLUMNS,
    ContributorEntry,
    ContributorInterval,
    load_contributor_config,
    parse_datetime_columns,
)


def test_parse_datetime_columns_ignores_missing_columns() -> None:
    df_raw = pl.DataFrame(
        {
            "id": [1, 2],
            "name": ["a", "b"],
        }
    )

    out = parse_datetime_columns(df_raw)

    assert out.columns == ["id", "name"]
    assert out.to_dict(as_series=False) == df_raw.to_dict(as_series=False)


def test_parse_datetime_columns_uses_custom_column_list() -> None:
    df_raw = pl.DataFrame(
        {
            "created_at": ["2025-02-01 10:00:00.000000+00:00"],
            "custom_ts": ["2025-02-01 11:30:00.000000+00:00"],
            "value": [7],
        }
    )

    out = parse_datetime_columns(df_raw, datetime_columns=["custom_ts"])

    assert out.schema["custom_ts"] == pl.Datetime("us", "UTC")
    assert out.schema["created_at"] == pl.String
    assert out["value"][0] == 7


def test_parse_datetime_columns_parses_default_known_column() -> None:
    assert "created_at" in DEFAULT_DATETIME_COLUMNS
    df_raw = pl.DataFrame(
        {
            "created_at": [
                "2025-02-01 10:00:00.000000+00:00",
                "2025-02-02 11:15:30.000000+00:00",
            ]
        }
    )

    out = parse_datetime_columns(df_raw)

    assert out.schema["created_at"] == pl.Datetime("us", "UTC")
    assert out.height == 2
    assert out["created_at"].null_count() == 0


def test_load_contributor_config_basic() -> None:
    data = [
        {
            "login": "octocat",
            "intervals": [{"start": "2024-01-01", "end": "2024-12-31"}],
        },
        {"login": "monalisa", "intervals": []},
    ]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name
    try:
        config = load_contributor_config(path)
    finally:
        os.unlink(path)

    assert len(config) == 2
    assert config[0] == ContributorEntry(
        login="octocat",
        intervals=[ContributorInterval(start="2024-01-01", end="2024-12-31")],
    )
    assert config[1] == ContributorEntry(login="monalisa", intervals=[])


def test_load_contributor_config_missing_intervals_key() -> None:
    data = [{"login": "octocat"}]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name
    try:
        config = load_contributor_config(path)
    finally:
        os.unlink(path)

    assert config[0].intervals == []


def test_load_contributor_config_null_interval_bounds() -> None:
    data = [{"login": "octocat", "intervals": [{"start": "2024-01-01", "end": None}]}]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name
    try:
        config = load_contributor_config(path)
    finally:
        os.unlink(path)

    assert config[0].intervals[0] == ContributorInterval(start="2024-01-01", end=None)

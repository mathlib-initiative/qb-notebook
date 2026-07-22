from __future__ import annotations

from datetime import date

import pytest

from qb_notebook import weekly_report as wr


# --------------------------------------------------------------------------- #
# date arithmetic
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "anchor,start,end,label",
    [
        # Wednesday -> most recent complete Mon..Sun, label = following Monday.
        (date(2026, 7, 22), date(2026, 7, 13), date(2026, 7, 19), date(2026, 7, 20)),
        # Sunday counts as that week's end.
        (date(2026, 7, 19), date(2026, 7, 13), date(2026, 7, 19), date(2026, 7, 20)),
        # Monday looks back to the prior complete week.
        (date(2026, 7, 20), date(2026, 7, 13), date(2026, 7, 19), date(2026, 7, 20)),
    ],
)
def test_week_window_and_label(anchor, start, end, label):
    assert wr.week_window(anchor) == (start, end)
    assert wr.report_date(anchor) == label


# --------------------------------------------------------------------------- #
# queueboard table scraping
# --------------------------------------------------------------------------- #
def _queue_html(table_id: str, total_days: list[int]) -> str:
    """A review-dashboard-style table: 3 hidden timedelta keys per row, the last
    being 'total time in review'."""
    rows = []
    for i, d in enumerate(total_days):
        rows.append(
            f"<tr><td>{i}</td>"
            f'<td><div style="display:none">1-0</div>a</td>'
            f'<td><div style="display:none">2-0</div>b</td>'
            f'<td><a><div style="display:none">{d}-0</div>{d} days</a></td></tr>'
        )
    body = "".join(rows)
    return (
        f'<table id="{table_id}"><thead><tr><th>n</th><th>x</th><th>y</th>'
        f"<th>total time in review</th></tr></thead><tbody>{body}</tbody></table>"
    )


def test_parse_queue_table_size_and_top10():
    html = _queue_html("t-queue", list(range(1, 13)))  # 12 rows, totals 1..12 days
    size, top10 = wr._parse_queue_table(html, "t-queue")
    assert size == 12
    # top 10 = 12,11,...,3 -> mean 7.5
    assert top10 == pytest.approx(7.5)


def test_parse_queue_table_fewer_than_10_rows():
    html = _queue_html("t-queue", [10, 20, 30])
    size, top10 = wr._parse_queue_table(html, "t-queue")
    assert size == 3
    assert top10 == pytest.approx(20.0)


def test_parse_queue_table_missing_table_raises():
    with pytest.raises(wr.WeeklyReportError):
        wr._parse_queue_table("<html></html>", "t-queue")


# --------------------------------------------------------------------------- #
# declaration-count scraping
# --------------------------------------------------------------------------- #
def test_parse_decl_counts_maps_headers_to_columns():
    html = (
        '<h2>Counts</h2><table class="table"><tr>'
        "<th>Definitions</th><th>Theorems</th><th>Contributors</th></tr>"
        "<tr><td>134,678</td><td>283067</td><td>772</td></tr></table>"
    )
    assert wr._parse_decl_counts(html) == (134678, 283067)


def test_parse_decl_counts_missing_header_raises():
    html = "<table><tr><th>Foo</th></tr><tr><td>1</td></tr></table>"
    with pytest.raises(wr.WeeklyReportError):
        wr._parse_decl_counts(html)


# --------------------------------------------------------------------------- #
# commit classification (git output monkeypatched)
# --------------------------------------------------------------------------- #
def test_commit_counts_classifies_feat(monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()
    subjects = "\n".join(
        [
            "feat: add a theorem",
            "feat(Algebra): more",
            "chore: bump",
            "fix: typo",
            "refactor: cleanup",
        ]
    )
    monkeypatch.setattr(wr, "_run", lambda *a, **k: subjects)
    feat, nonfeat = wr.commit_counts(
        tmp_path, date(2026, 7, 13), date(2026, 7, 19), fetch=False
    )
    assert (feat, nonfeat) == (2, 3)


def test_commit_counts_requires_git_repo(tmp_path):
    with pytest.raises(wr.WeeklyReportError):
        wr.commit_counts(tmp_path, date(2026, 7, 13), date(2026, 7, 19), fetch=False)


# --------------------------------------------------------------------------- #
# declaration deltas
# --------------------------------------------------------------------------- #
def test_decl_deltas_uses_last_known_cumulative():
    history = [
        {"defs_total": None, "thms_total": None},
        {"defs_total": 100, "thms_total": 200},
    ]
    assert wr.decl_deltas(history, 112, 215) == (12, 15)


def test_decl_deltas_baseline_when_no_prior_total():
    history = [{"defs_total": None, "thms_total": None}]
    assert wr.decl_deltas(history, 100, 200) == (None, None)


# --------------------------------------------------------------------------- #
# CSV store round-trip
# --------------------------------------------------------------------------- #
def test_store_round_trip(tmp_path):
    path = tmp_path / "weekly_stats.csv"
    r1 = {k: None for k in [*wr.METRIC_KEYS, *wr.EXTRA_KEYS]}
    r1.update({"date": "2026-07-06", "open_prs": 2750, "top10_queue": 134.8})
    r2 = {k: None for k in [*wr.METRIC_KEYS, *wr.EXTRA_KEYS]}
    r2.update({"date": "2026-07-13", "open_prs": 2855, "defs_total": 134678})
    wr.append_week(path, r1)
    wr.append_week(path, r2)

    rows = wr.read_store(path)
    assert [r["date"] for r in rows] == ["2026-07-06", "2026-07-13"]
    assert rows[0]["open_prs"] == 2750 and isinstance(rows[0]["open_prs"], int)
    assert rows[0]["top10_queue"] == pytest.approx(134.8)
    assert rows[0]["defs_total"] is None  # blank cell -> None
    assert rows[1]["defs_total"] == 134678


def test_read_store_missing_file_is_empty(tmp_path):
    assert wr.read_store(tmp_path / "nope.csv") == []


# --------------------------------------------------------------------------- #
# report assembly
# --------------------------------------------------------------------------- #
def test_assemble_report_today_last_avg_diff():
    # 9 prior weeks of an integer metric, values 1..9; new week = 20.
    history = [{"open_prs": v} for v in range(1, 10)]
    new_row = {"open_prs": 20}
    report = {r["key"]: r for r in wr.assemble_report(history, new_row)}
    m = report["open_prs"]
    assert m["today"] == 20
    assert m["last"] == 9
    assert m["diff"] == 11
    # Avg8w = mean of last 8 including today: weeks 3..9 (=3,4,5,6,7,8,9) + 20
    assert m["avg8w"] == pytest.approx((3 + 4 + 5 + 6 + 7 + 8 + 9 + 20) / 8)


def test_assemble_report_handles_missing_values():
    report = {r["key"]: r for r in wr.assemble_report([], {})}
    m = report["defs_delta"]
    assert m["today"] is None and m["last"] is None
    assert m["avg8w"] is None and m["diff"] is None
    assert report["tactic_docs"]["manual"] is True

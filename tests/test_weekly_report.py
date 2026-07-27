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
# declaration counts from git history
# --------------------------------------------------------------------------- #
_COUNTS_HTML = (
    "<h2>Counts</h2><table><tr>"
    "<th>Definitions</th><th>Theorems</th><th>Contributors</th></tr>"
    "<tr><td>130,157</td><td>272490</td><td>556</td></tr></table>"
)


def test_fetch_stats_ref_builds_fetch_command(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(wr, "_run", lambda cmd, **k: calls.append(cmd) or "")
    wr.fetch_stats_ref(tmp_path, "origin/master")
    assert calls == [["git", "fetch", "origin", "master"]]


def test_fetch_stats_ref_rejects_bare_ref(tmp_path):
    with pytest.raises(wr.WeeklyReportError):
        wr.fetch_stats_ref(tmp_path, "master")


def test_resolve_stats_commit_selects_and_parses_date(monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()
    seen = {}

    def fake_run(cmd, **k):
        seen["cmd"] = cmd
        return "abc123\t2026-06-15T05:22:10+00:00\n"

    monkeypatch.setattr(wr, "_run", fake_run)
    sha, day = wr.resolve_stats_commit(tmp_path, date(2026, 6, 15))
    assert sha == "abc123"
    assert day == date(2026, 6, 15)
    # `on_or_before` is inclusive: the cutoff is the *following* midnight, UTC.
    assert "--before=2026-06-16T00:00:00+00:00" in seen["cmd"]


def test_resolve_stats_commit_no_match_raises(monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()
    monkeypatch.setattr(wr, "_run", lambda *a, **k: "\n")
    with pytest.raises(wr.WeeklyReportError):
        wr.resolve_stats_commit(tmp_path, date(2019, 1, 1))


def test_resolve_stats_commit_requires_git_repo(tmp_path):
    with pytest.raises(wr.WeeklyReportError):
        wr.resolve_stats_commit(tmp_path, date(2026, 6, 15))


def test_decl_counts_from_git_end_to_end(monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()

    def fake_run(cmd, **k):
        if "log" in cmd:
            return "abc123\t2026-05-04T05:10:00+00:00\n"
        if "show" in cmd:
            return _COUNTS_HTML
        raise AssertionError(cmd)

    monkeypatch.setattr(wr, "_run", fake_run)
    assert wr.decl_counts_from_git(tmp_path, date(2026, 5, 4)) == (130157, 272490)


def test_decl_counts_from_git_fetches_when_requested(monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()
    fetched = []

    def fake_run(cmd, **k):
        if "fetch" in cmd:
            fetched.append(cmd)
            return ""
        if "log" in cmd:
            return "abc123\t2026-05-04T05:10:00+00:00\n"
        return _COUNTS_HTML

    monkeypatch.setattr(wr, "_run", fake_run)
    wr.decl_counts_from_git(tmp_path, date(2026, 5, 4), fetch=True)
    assert fetched == [["git", "fetch", "origin", "master"]]


# --------------------------------------------------------------------------- #
# backfilling the store's declaration columns
# --------------------------------------------------------------------------- #
def test_backfill_decl_columns_uses_seven_day_window():
    # A dense daily history; the store skips a week between the 2nd and 3rd row.
    totals = {
        date(2026, 5, 4): (130157, 272490),
        date(2026, 4, 27): (129763, 271289),  # 7 days before the first row
        date(2026, 5, 26): (131160, 274770),
        date(2026, 5, 19): (130804, 273839),  # 7 days before the gap row
    }
    rows = [
        {
            "date": "2026-05-04",
            "open_prs": 2451,
            "defs_total": None,
            "thms_total": None,
        },
        {
            "date": "2026-05-26",
            "open_prs": 2624,
            "defs_total": None,
            "thms_total": None,
        },
    ]
    out = wr.backfill_decl_columns(rows, lambda d: totals.get(d))

    assert out[0]["defs_total"] == 130157 and out[0]["thms_total"] == 272490
    # Delta is total(D) - total(D-7), not a diff against the previous stored row.
    assert out[0]["defs_delta"] == 130157 - 129763
    assert out[0]["thms_delta"] == 272490 - 271289
    assert out[1]["defs_delta"] == 131160 - 130804
    assert out[1]["thms_delta"] == 274770 - 273839
    # Non-declaration columns are preserved; inputs are not mutated.
    assert out[0]["open_prs"] == 2451
    assert rows[0]["defs_total"] is None


def test_backfill_decl_columns_missing_week_leaves_none():
    rows = [{"date": "2026-05-04", "defs_total": None, "thms_total": None}]
    out = wr.backfill_decl_columns(rows, lambda d: None)
    assert out[0]["defs_total"] is None
    assert out[0]["defs_delta"] is None and out[0]["thms_delta"] is None


def test_write_store_round_trip(tmp_path):
    path = tmp_path / "weekly_stats.csv"
    rows = [
        {k: None for k in [*wr.METRIC_KEYS, *wr.EXTRA_KEYS]} | {"date": "2026-05-04"},
        {k: None for k in [*wr.METRIC_KEYS, *wr.EXTRA_KEYS]}
        | {"date": "2026-05-11", "defs_total": 130533, "defs_delta": 376},
    ]
    wr.write_store(path, rows)
    back = wr.read_store(path)
    assert [r["date"] for r in back] == ["2026-05-04", "2026-05-11"]
    assert back[0]["defs_total"] is None
    assert back[1]["defs_total"] == 130533 and back[1]["defs_delta"] == 376


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


# --------------------------------------------------------------------------- #
# slide-table CSV
# --------------------------------------------------------------------------- #
def test_write_report_csv(tmp_path):
    import csv

    history = [{"open_prs": 2855, "top10_queue": 142.0}]
    new_row = {"open_prs": 2987, "top10_queue": 144.446}
    report = wr.assemble_report(history, new_row)
    path = tmp_path / "weekly_report.csv"
    wr.write_report_csv(path, report)

    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["Metric", "Today", "Last", "Avg8w", "Diff"]
    assert len(rows) == 1 + len(wr.METRIC_KEYS)
    by_label = {r[0]: r[1:] for r in rows[1:]}
    # Integer metric: plain ints, one-decimal average, signed diff.
    assert by_label["Open PRs"] == ["2987", "2855", "2921.0", "+132"]
    # Float metric: one decimal everywhere, signed diff.
    assert by_label["Top 10 avg time on queue"] == ["144.4", "142.0", "143.2", "+2.4"]
    # Missing values become empty cells, not em-dashes.
    assert by_label["#definitions in Mathlib this week"] == ["", "", "", ""]

#!/usr/bin/env python
"""Assemble the weekly Mathlib Initiative meeting statistics.

Run from the repo root::

    uv run python -m scripts.weekly_report               # this week
    uv run python -m scripts.weekly_report --dry-run      # compute, don't record
    uv run python -m scripts.weekly_report --anchor 2026-07-20 --tactic-docs 125

Gathers the eight automatable metrics, carries the one manual metric
(rewritten tactic docs) forward, appends a row to the CSV store, and prints a
paste-ready Today / Last / Avg8w / Diff table for the slide.

The queueboard backend defaults to scraping the public dashboard; set
``--queueboard-api-base`` or ``$QUEUEBOARD_API_BASE_URL`` to use the JSON API.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

from qb_notebook.weekly_report import (
    DEFAULT_MATHLIB_STATS_URL,
    DEFAULT_QUEUEBOARD_SITE,
    DEFAULT_REPO,
    DEFAULT_RULE_SET_ID,
    WeeklyReportError,
    append_week,
    assemble_report,
    commit_counts,
    decl_counts,
    decl_deltas,
    fetch_queueboard_metrics,
    open_pr_count,
    read_store,
    report_date,
    week_window,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _warn(msg: str) -> None:
    print(f"warning: {msg}", file=sys.stderr)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--anchor",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="Reference date (YYYY-MM-DD); default today. The reported week is the "
        "most recent complete Mon-Sun on/before this date.",
    )
    p.add_argument(
        "--tactic-docs", type=int, default=None, help="Manual tactic-docs count."
    )
    p.add_argument(
        "--mathlib4-dir",
        type=Path,
        default=REPO_ROOT.parent / "mathlib4",
        help="Path to a mathlib4 checkout (default: ../mathlib4).",
    )
    p.add_argument(
        "--store",
        type=Path,
        default=REPO_ROOT / "weekly_stats.csv",
        help="CSV store path (default: ./weekly_stats.csv).",
    )
    p.add_argument("--repo", default=DEFAULT_REPO)
    p.add_argument("--rule-set-id", type=int, default=DEFAULT_RULE_SET_ID)
    p.add_argument("--queueboard-site", default=DEFAULT_QUEUEBOARD_SITE)
    p.add_argument("--queueboard-api-base", default=None)
    p.add_argument("--mathlib-stats-url", default=DEFAULT_MATHLIB_STATS_URL)
    p.add_argument(
        "--no-fetch", action="store_true", help="Skip 'git fetch' of mathlib4."
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Compute and print; do not write."
    )
    p.add_argument(
        "--force", action="store_true", help="Overwrite an existing row for the date."
    )
    return p.parse_args(argv)


def _gather(args: argparse.Namespace, history: list[dict]) -> dict:
    week_start, week_end = week_window(args.anchor)
    row: dict = {"date": report_date(args.anchor).isoformat()}

    # Queueboard (metrics 1, 2, 6, 7).
    try:
        qb = fetch_queueboard_metrics(
            site=args.queueboard_site,
            api_base=args.queueboard_api_base,
            repo=args.repo,
            rule_set_id=args.rule_set_id,
        )
    except WeeklyReportError as e:
        _warn(f"queueboard: {e}")
        qb = {}
    row["queue_prs"] = qb.get("queue_prs")
    row["top10_queue"] = qb.get("top10_queue")
    row["top10_newcontrib"] = qb.get("top10_newcontrib")
    if "open_prs" in qb:
        row["open_prs"] = qb["open_prs"]
    else:
        try:
            row["open_prs"] = open_pr_count(args.repo)
        except WeeklyReportError as e:
            _warn(f"open PR count: {e}")
            row["open_prs"] = None

    # Commit counts (metrics 3, 4).
    try:
        feat, nonfeat = commit_counts(
            args.mathlib4_dir, week_start, week_end, fetch=not args.no_fetch
        )
        row["feat_commits"], row["nonfeat_commits"] = feat, nonfeat
    except WeeklyReportError as e:
        _warn(f"commit counts: {e}")
        row["feat_commits"] = row["nonfeat_commits"] = None

    # Declarations (metrics 8, 9): store cumulative totals, report weekly delta.
    try:
        defs_total, thms_total = decl_counts(args.mathlib_stats_url)
        row["defs_total"], row["thms_total"] = defs_total, thms_total
        row["defs_delta"], row["thms_delta"] = decl_deltas(
            history, defs_total, thms_total
        )
    except WeeklyReportError as e:
        _warn(f"declaration counts: {e}")
        row["defs_total"] = row["thms_total"] = None
        row["defs_delta"] = row["thms_delta"] = None

    # Tactic docs (metric 5): manual; use override or carry forward.
    if args.tactic_docs is not None:
        row["tactic_docs"] = args.tactic_docs
    else:
        prev = next(
            (
                r["tactic_docs"]
                for r in reversed(history)
                if r.get("tactic_docs") is not None
            ),
            None,
        )
        row["tactic_docs"] = prev
        if prev is not None:
            _warn(
                f"tactic docs carried forward from last week ({prev}); "
                "override with --tactic-docs N"
            )
    return row


def _fmt(value, *, is_float: bool, signed: bool = False, avg: bool = False) -> str:
    if value is None:
        return "—"
    if is_float or avg:
        return f"{value:+.1f}" if signed else f"{value:.1f}"
    return f"{int(round(value)):+d}" if signed else f"{int(round(value))}"


def _print_report(args: argparse.Namespace, report: list[dict]) -> None:
    week_start, week_end = week_window(args.anchor)
    label = report_date(args.anchor).isoformat()
    print(f"Week of {label}  (commits {week_start} .. {week_end})")
    print()
    label_w = max(len(r["label"]) for r in report)
    header = (
        f"{'Metric':<{label_w}}  {'Today':>8}  {'Last':>8}  {'Avg8w':>8}  {'Diff':>7}"
    )
    print(header)
    print("-" * len(header))
    for r in report:
        flt = r["is_float"]
        line = (
            f"{r['label']:<{label_w}}  "
            f"{_fmt(r['today'], is_float=flt):>8}  "
            f"{_fmt(r['last'], is_float=flt):>8}  "
            f"{_fmt(r['avg8w'], is_float=flt, avg=True):>8}  "
            f"{_fmt(r['diff'], is_float=flt, signed=True):>7}"
        )
        note = ""
        if r["manual"]:
            note = "  (manual — confirm)"
        elif r["key"] in ("defs_delta", "thms_delta") and r["today"] is None:
            note = "  (baseline — delta next week)"
        print(line + note)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    store_rows = read_store(args.store)
    label = report_date(args.anchor).isoformat()
    existing = [r for r in store_rows if r["date"] == label]
    history = [r for r in store_rows if r["date"] < label]

    if existing and not (args.force or args.dry_run):
        _warn(
            f"a row for {label} already exists in {args.store}; "
            "printing only (use --force to overwrite)."
        )

    row = _gather(args, history)
    report = assemble_report(history, row)
    _print_report(args, report)

    if args.dry_run:
        print("\n(dry run — store not modified)")
        return 0
    if existing and not args.force:
        return 0
    if existing and args.force:
        _rewrite_without(args.store, label, store_rows)
    append_week(args.store, row)
    print(f"\n✓ recorded {label} in {args.store}")
    return 0


def _rewrite_without(path: Path, label: str, store_rows: list[dict]) -> None:
    """Rewrite the store dropping any row for ``label`` (for --force)."""
    from qb_notebook.weekly_report import STORE_FIELDS

    import csv

    kept = [r for r in store_rows if r["date"] != label]
    with Path(path).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STORE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in kept:
            writer.writerow(
                {k: ("" if r.get(k) is None else r[k]) for k in STORE_FIELDS}
            )


if __name__ == "__main__":
    raise SystemExit(main())

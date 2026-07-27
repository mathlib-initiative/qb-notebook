#!/usr/bin/env python
"""Backfill historical definition/theorem counts in the weekly-stats store.

The declaration numbers (``defs_total``/``thms_total`` and the weekly
``defs_delta``/``thms_delta``) are the one part of the weekly report that *can*
be reconstructed after the fact. The rendered ``mathlib_stats.html`` on the
``master`` branch of a ``leanprover-community.github.io`` checkout is a dated,
roughly-daily snapshot of the cumulative counts (the deploy bot commits the
built site there). This script reads each row already in ``weekly_stats.csv``,
looks up the counts as of that week from git history, and fills the four
declaration columns; every other column is left untouched. Weekly deltas are
true 7-day windows (``total(D) - total(D-7)``), so they stay correct even for
weeks the store skipped.

It is **dry-run by default** — it prints what it would change and writes nothing
unless you pass ``--write``.

Run from the repo root::

    uv run python -m scripts.backfill_decls                  # preview
    uv run python -m scripts.backfill_decls --write          # apply
    uv run python -m scripts.backfill_decls --fetch --write  # refresh git first

The queueboard/open-PR metrics remain non-reconstructable (live scrapes only).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from qb_notebook.weekly_report import (
    DEFAULT_STATS_GIT_PATH,
    DEFAULT_STATS_GIT_REF,
    WeeklyReportError,
    backfill_decl_columns,
    decl_counts_at_commit,
    fetch_stats_ref,
    read_store,
    resolve_stats_commit,
    write_store,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _warn(msg: str) -> None:
    print(f"warning: {msg}", file=sys.stderr)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--store",
        type=Path,
        default=REPO_ROOT / "weekly_stats.csv",
        help="CSV store to backfill (default: ./weekly_stats.csv).",
    )
    p.add_argument(
        "--stats-repo",
        type=Path,
        default=REPO_ROOT.parent / "leanprover-community.github.io",
        help="Path to a leanprover-community.github.io checkout "
        "(default: ../leanprover-community.github.io).",
    )
    p.add_argument(
        "--ref",
        default=DEFAULT_STATS_GIT_REF,
        help=f"Git ref of the deployed site (default: {DEFAULT_STATS_GIT_REF}).",
    )
    p.add_argument("--path", default=DEFAULT_STATS_GIT_PATH)
    p.add_argument(
        "--fetch",
        action="store_true",
        help="Run 'git fetch' on the stats repo first (needed if it is stale).",
    )
    p.add_argument(
        "--write",
        action="store_true",
        help="Write the updated store (default: dry run — print only).",
    )
    return p.parse_args(argv)


def _fmt(value) -> str:
    return "—" if value is None else str(value)


def _cell(old, new) -> str:
    """Render an ``old -> new`` transition, or just the value when unchanged."""
    return _fmt(new) if old == new else f"{_fmt(old)} -> {_fmt(new)}"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if not (args.stats_repo / ".git").exists():
        _warn(
            f"{args.stats_repo} is not a git checkout of "
            "leanprover-community.github.io; pass --stats-repo PATH."
        )
        return 1

    rows = read_store(args.store)
    if not rows:
        _warn(f"no rows in {args.store}; nothing to backfill.")
        return 1

    if args.fetch:
        try:
            fetch_stats_ref(args.stats_repo, args.ref)
        except WeeklyReportError as e:
            _warn(f"git fetch failed (continuing with local refs): {e}")

    # Resolve counts as of any date once, remembering the deploy used per date.
    counts_cache: dict[date, tuple[int, int] | None] = {}
    deploy: dict[date, date] = {}

    def counts_at(d: date):
        if d not in counts_cache:
            try:
                sha, dep = resolve_stats_commit(
                    args.stats_repo, d, ref=args.ref, path=args.path
                )
                counts_cache[d] = decl_counts_at_commit(
                    args.stats_repo, sha, path=args.path
                )
                deploy[d] = dep
            except WeeklyReportError as e:
                _warn(f"{d}: {e}")
                counts_cache[d] = None
        return counts_cache[d]

    updated = backfill_decl_columns(rows, counts_at)

    # Report table: what each row's declaration columns become.
    label_w = max(len(r["date"]) for r in rows)
    header = (
        f"{'week':<{label_w}}  {'deploy':<10}  {'defs_total':>18}  "
        f"{'thms_total':>18}  {'Δdefs':>14}  {'Δthms':>14}"
    )
    print(f"Backfilling declaration counts in {args.store}")
    print(f"  from {args.ref}:{args.path} in {args.stats_repo}\n")
    print(header)
    print("-" * len(header))
    changed = 0
    for old, new in zip(rows, updated):
        d = date.fromisoformat(old["date"])
        if any(old.get(k) != new.get(k) for k in ("defs_total", "thms_total")):
            changed += 1
        dep = deploy.get(d)
        print(
            f"{old['date']:<{label_w}}  {(dep.isoformat() if dep else '—'):<10}  "
            f"{_cell(old.get('defs_total'), new.get('defs_total')):>18}  "
            f"{_cell(old.get('thms_total'), new.get('thms_total')):>18}  "
            f"{_cell(old.get('defs_delta'), new.get('defs_delta')):>14}  "
            f"{_cell(old.get('thms_delta'), new.get('thms_delta')):>14}"
        )

    print(f"\n{changed} of {len(rows)} rows get new/updated declaration totals.")
    if not args.write:
        print("(dry run — store not written; re-run with --write to apply)")
        return 0
    write_store(args.store, updated)
    print(f"✓ wrote {args.store}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

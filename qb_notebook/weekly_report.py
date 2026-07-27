"""Collect the weekly Mathlib Initiative meeting statistics.

This module gathers the numbers that used to be assembled by hand each week
(see ``scripts/pr_table.sh`` and ``scripts/decl-log.sh``, now retired) and the
manual queueboard download + ``jq`` step, and turns them into a small CSV time
series plus a paste-ready Today / Last / Avg8w / Diff table.

Nothing ephemeral is hard-coded: the queueboard backend defaults to scraping the
public static dashboard (a stable ``github.io`` URL) plus ``gh`` for the open-PR
count, and only uses the JSON snapshot API when a base URL is supplied.

The nine tracked metrics (slide order) are described by :data:`METRIC_LABELS`.
"""

from __future__ import annotations

import gzip
import json
import os
import re
import subprocess
import time
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from statistics import mean

DEFAULT_QUEUEBOARD_SITE = "https://leanprover-community.github.io/queueboard"
DEFAULT_MATHLIB_STATS_URL = "https://leanprover-community.github.io/mathlib_stats.html"
DEFAULT_REPO = "leanprover-community/mathlib4"
DEFAULT_RULE_SET_ID = 3
_USER_AGENT = "qb-notebook-weekly-report"
_HTTP_TIMEOUT = 120

# CSV column order. The nine slide metrics, then two helper columns that hold the
# cumulative declaration totals so the weekly deltas can be recomputed.
METRIC_KEYS = [
    "open_prs",
    "queue_prs",
    "feat_commits",
    "nonfeat_commits",
    "tactic_docs",
    "top10_queue",
    "top10_newcontrib",
    "defs_delta",
    "thms_delta",
]
EXTRA_KEYS = ["defs_total", "thms_total"]
STORE_FIELDS = ["date", *METRIC_KEYS, *EXTRA_KEYS]

# Metrics reported with one decimal place; everything else is an integer.
FLOAT_KEYS = {"top10_queue", "top10_newcontrib"}
# Metrics with no automated source; carried forward and flagged.
MANUAL_KEYS = {"tactic_docs"}

METRIC_LABELS = {
    "open_prs": "Open PRs",
    "queue_prs": "PRs on queueboard",
    "feat_commits": "#commits last week (feat)",
    "nonfeat_commits": "#commits last week (non-feat)",
    "tactic_docs": "#rewritten tactic docs (/~500)",
    "top10_queue": "Top 10 avg time on queue",
    "top10_newcontrib": "Top 10 avg time new contributor",
    "defs_delta": "#definitions in Mathlib this week",
    "thms_delta": "#theorems in Mathlib this week",
}


class WeeklyReportError(RuntimeError):
    """Raised when a weekly-report data source cannot be read."""


# --------------------------------------------------------------------------- #
# low-level helpers
# --------------------------------------------------------------------------- #
def _run(cmd: list[str], cwd: str | Path | None = None) -> str:
    """Run a command and return stdout, raising :class:`WeeklyReportError`."""
    env = os.environ.copy()
    env["NO_COLOR"] = "true"
    env["CLICOLOR_FORCE"] = "0"
    try:
        return subprocess.check_output(
            cmd,
            env=env,
            cwd=str(cwd) if cwd is not None else None,
            stderr=subprocess.PIPE,
            encoding="utf-8-sig",
            text=True,
        )
    except FileNotFoundError as e:
        raise WeeklyReportError(f"Command not found: {cmd[0]}") from e
    except subprocess.CalledProcessError as e:
        raise WeeklyReportError(
            f"Command failed (exit {e.returncode}): {' '.join(cmd)}\n\n{e.output}"
        ) from e


def _fetch(url: str) -> str:
    """GET ``url`` and return the decoded body (handles gzip)."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
    except OSError as e:
        raise WeeklyReportError(f"Failed to fetch {url}: {e}") from e
    return raw.decode("utf-8", errors="replace")


def _strip_tags(html: str) -> str:
    return re.sub(r"<[^>]+>", "", html)


def _to_int(text: str) -> int:
    """Parse an integer from a cell that may contain commas/whitespace."""
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        raise WeeklyReportError(f"Expected a number, got {text!r}")
    return int(digits)


# --------------------------------------------------------------------------- #
# dates
# --------------------------------------------------------------------------- #
def week_window(anchor: date) -> tuple[date, date]:
    """Return the most recent complete ``(Monday, Sunday)`` week on/before ``anchor``.

    Ports the date arithmetic of ``scripts/pr_table.sh``: the "past week" is the
    week ending on the most recent Sunday that is on or before ``anchor``.
    """
    dow = anchor.isoweekday()  # 1 = Monday .. 7 = Sunday
    days_to_sunday = 0 if dow == 7 else dow
    week_end = anchor - timedelta(days=days_to_sunday)
    week_start = week_end - timedelta(days=6)
    return week_start, week_end


def report_date(anchor: date) -> date:
    """The row label: the Monday that starts the meeting week (= week_end + 1)."""
    _, week_end = week_window(anchor)
    return week_end + timedelta(days=1)


# --------------------------------------------------------------------------- #
# commit counts (metrics 3, 4)
# --------------------------------------------------------------------------- #
def commit_counts(
    mathlib4_dir: str | Path,
    week_start: date,
    week_end: date,
    *,
    ref: str = "upstream/master",
    fetch: bool = True,
) -> tuple[int, int]:
    """Count ``(feat, non-feat)`` commits on ``ref`` within ``[start, end]``.

    Counts against ``ref`` directly, so no working-tree checkout is required.
    ``feat`` means the commit subject begins with ``feat`` (conventional commit),
    matching the historical ``pr_table.sh`` behaviour.
    """
    mathlib4_dir = Path(mathlib4_dir).expanduser()
    if not (mathlib4_dir / ".git").exists():
        raise WeeklyReportError(f"Not a git repo: {mathlib4_dir}")
    if fetch:
        remote = ref.split("/", 1)[0]
        _run(["git", "fetch", remote, ref.split("/", 1)[-1]], cwd=mathlib4_dir)
    # --until is exclusive of the following day's midnight, matching pr_table.sh.
    until = week_end + timedelta(days=1)
    out = _run(
        [
            "git",
            "log",
            ref,
            f"--since={week_start.isoformat()}",
            f"--until={until.isoformat()}",
            "--pretty=format:%s",
        ],
        cwd=mathlib4_dir,
    )
    subjects = [line for line in out.splitlines() if line.strip()]
    feat = sum(1 for s in subjects if s.startswith("feat"))
    return feat, len(subjects) - feat


# --------------------------------------------------------------------------- #
# declaration counts (metrics 8, 9)
# --------------------------------------------------------------------------- #
def decl_counts(url: str = DEFAULT_MATHLIB_STATS_URL) -> tuple[int, int]:
    """Return the cumulative ``(definitions, theorems)`` totals from the stats page.

    Parses the "Counts" table by mapping each ``<th>`` header to its column's
    ``<td>`` value, so it is robust to the table gaining columns and fails loudly
    if the ``Definitions`` / ``Theorems`` headers disappear.
    """
    return _parse_decl_counts(_fetch(url), source=url)


def _parse_decl_counts(html: str, *, source: str = "stats page") -> tuple[int, int]:
    idx = html.find("<th>Definitions</th>")
    if idx < 0:
        raise WeeklyReportError(
            f"Could not find the 'Definitions' header at {source}; layout changed?"
        )
    tstart = html.rfind("<table", 0, idx)
    tend = html.find("</table>", idx)
    if tstart < 0 or tend < 0:
        raise WeeklyReportError(f"Could not locate the counts table at {source}")
    table = html[tstart:tend]
    rows = re.findall(r"<tr\b.*?</tr>", table, re.S)
    if len(rows) < 2:
        raise WeeklyReportError(f"Counts table at {source} has too few rows")
    headers = [
        _strip_tags(h).strip()
        for h in re.findall(r"<th\b[^>]*>(.*?)</th>", rows[0], re.S)
    ]
    values = [
        _strip_tags(v).strip()
        for v in re.findall(r"<td\b[^>]*>(.*?)</td>", rows[1], re.S)
    ]
    mapping = dict(zip(headers, values))
    if "Definitions" not in mapping or "Theorems" not in mapping:
        raise WeeklyReportError(
            f"Counts table at {source} missing expected headers: {headers}"
        )
    return _to_int(mapping["Definitions"]), _to_int(mapping["Theorems"])


# --------------------------------------------------------------------------- #
# queueboard (metrics 1, 2, 6, 7)
# --------------------------------------------------------------------------- #
def open_pr_count(repo: str = DEFAULT_REPO) -> int:
    """Total open PRs (incl. drafts) via the GitHub search API ``total_count``.

    ``total_count`` is exact even beyond the 1000-item listing cap.
    """
    out = _run(
        [
            "gh",
            "api",
            "-X",
            "GET",
            "search/issues",
            "-f",
            f"q=repo:{repo} is:pr is:open",
            "--jq",
            ".total_count",
        ]
    )
    return _to_int(out)


def _parse_queue_table(html: str, table_id: str) -> tuple[int, float | None]:
    """Return ``(row_count, top10_avg_days)`` for a review-dashboard table.

    Each data row carries three hidden ``display:none">DAYS-SECONDS<`` timedelta
    sort keys (last update, last status change, total time in review); the *last*
    one is the total time in review.
    """
    marker = f'id="{table_id}"'
    i = html.find(marker)
    if i < 0:
        raise WeeklyReportError(f"Table '{table_id}' not found on review dashboard")
    j = html.find("</table>", i)
    table = html[i : j + len("</table>")]
    rows = re.findall(r"<tr\b.*?</tr>", table, re.S)
    days: list[float] = []
    count = 0
    for row in rows:
        if "<th" in row:  # header row
            continue
        count += 1
        keys = re.findall(r"display:\s*none[^>]*>(\d+)-(\d+)<", row)
        if not keys:
            continue
        d, s = keys[-1]
        days.append(int(d) + int(s) / 86400)
    top = sorted(days, reverse=True)[:10]
    top10 = mean(top) if top else None
    return count, top10


def _queueboard_from_site(site: str) -> dict[str, float]:
    html = _fetch(f"{site.rstrip('/')}/review_dashboard.html")
    queue_prs, top10_queue = _parse_queue_table(html, "t-queue")
    _, top10_newcontrib = _parse_queue_table(html, "t-queue-new-contributors")
    return {
        "queue_prs": queue_prs,
        "top10_queue": top10_queue,
        "top10_newcontrib": top10_newcontrib,
    }


def _queueboard_from_api(
    api_base: str, repo: str, rule_set_id: int, retries: int
) -> dict[str, float]:
    url = (
        f"{api_base.rstrip('/')}/api/v1/queueboard/snapshot"
        f"?repo={repo}&rule_set_id={rule_set_id}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    data = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                if resp.status == 202:  # snapshot not yet computed; back off
                    time.sleep(2 * (attempt + 1))
                    continue
                data = json.loads(resp.read().decode("utf-8"))
                break
        except OSError as e:
            raise WeeklyReportError(f"Failed to fetch snapshot API {url}: {e}") from e
    if data is None:
        raise WeeklyReportError(f"Snapshot API not ready after {retries} tries: {url}")

    prs = data["prs"]
    dashboards = data["lists"]["dashboards"]

    def top10(kind: str) -> float | None:
        secs = []
        for n in dashboards.get(kind, []):
            info = prs.get(str(n)) or {}
            value = (info.get("total_queue_time") or {}).get("value_td")
            if value is not None:
                secs.append(value)
        top = sorted(secs, reverse=True)[:10]
        return mean(top) / 86400 if top else None

    return {
        "open_prs": len(prs),
        "queue_prs": len(dashboards.get("Queue", [])),
        "top10_queue": top10("Queue"),
        "top10_newcontrib": top10("QueueNewContributor"),
    }


def fetch_queueboard_metrics(
    *,
    site: str = DEFAULT_QUEUEBOARD_SITE,
    api_base: str | None = None,
    repo: str = DEFAULT_REPO,
    rule_set_id: int = DEFAULT_RULE_SET_ID,
    retries: int = 5,
) -> dict[str, float]:
    """Return ``{open_prs?, queue_prs, top10_queue, top10_newcontrib}``.

    Uses the JSON snapshot API when ``api_base`` (or ``$QUEUEBOARD_API_BASE_URL``)
    is set; otherwise scrapes the public static dashboard. In the scrape path
    ``open_prs`` is left to :func:`open_pr_count` (via ``gh``).
    """
    api_base = api_base or os.environ.get("QUEUEBOARD_API_BASE_URL")
    if api_base:
        return _queueboard_from_api(api_base, repo, rule_set_id, retries)
    return _queueboard_from_site(site)


# --------------------------------------------------------------------------- #
# CSV store
# --------------------------------------------------------------------------- #
def _parse_cell(value: str) -> int | float | None:
    value = value.strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return float(value)


def read_store(path: str | Path) -> list[dict]:
    """Load the CSV store as a list of row dicts (numbers parsed), date-ordered."""
    import csv

    path = Path(path)
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        rows = []
        for raw in csv.DictReader(f):
            row: dict = {"date": raw.get("date", "")}
            for key in [*METRIC_KEYS, *EXTRA_KEYS]:
                row[key] = _parse_cell(raw.get(key, "") or "")
            rows.append(row)
    return sorted(rows, key=lambda r: r["date"])


def append_week(path: str | Path, row: dict) -> None:
    """Append ``row`` to the CSV store, writing the header if the file is new."""
    import csv

    path = Path(path)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STORE_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(
            {k: ("" if row.get(k) is None else row[k]) for k in STORE_FIELDS}
        )


def decl_deltas(
    history: list[dict], defs_total: int, thms_total: int
) -> tuple[int | None, int | None]:
    """Weekly declaration deltas vs. the most recent stored cumulative totals.

    Returns ``(None, None)`` when no prior cumulative total is stored (the
    baseline run); the delta becomes available the following week.
    """
    prev = next(
        (r for r in reversed(history) if r.get("defs_total") is not None),
        None,
    )
    if prev is None:
        return None, None
    return defs_total - int(prev["defs_total"]), thms_total - int(prev["thms_total"])


# --------------------------------------------------------------------------- #
# report assembly and output
# --------------------------------------------------------------------------- #
def assemble_report(
    history: list[dict], new_row: dict, *, window: int = 8
) -> list[dict]:
    """Build the per-metric report rows: Today / Last / Avg8w / Diff.

    ``history`` is the prior weeks (excluding ``new_row``). ``Avg8w`` is the mean
    of the last ``window`` weeks including this one; ``Diff`` is Today − Last.
    """
    report = []
    for key in METRIC_KEYS:
        today = new_row.get(key)
        last = history[-1].get(key) if history else None
        series = [r.get(key) for r in history] + [today]
        recent = [v for v in series[-window:] if v is not None]
        avg = mean(recent) if recent else None
        diff = (today - last) if (today is not None and last is not None) else None
        report.append(
            {
                "key": key,
                "label": METRIC_LABELS[key],
                "today": today,
                "last": last,
                "avg8w": avg,
                "diff": diff,
                "manual": key in MANUAL_KEYS,
                "is_float": key in FLOAT_KEYS,
            }
        )
    return report


def format_value(
    value,
    *,
    is_float: bool,
    signed: bool = False,
    avg: bool = False,
    none: str = "—",
) -> str:
    """Format one report cell: one decimal for float/avg cells, ints otherwise."""
    if value is None:
        return none
    if is_float or avg:
        return f"{value:+.1f}" if signed else f"{value:.1f}"
    return f"{int(round(value)):+d}" if signed else f"{int(round(value))}"


def write_report_csv(path: str | Path, report: list[dict]) -> None:
    """Write the assembled report as a small CSV table (for Google Sheets/Slides).

    Cells hold the same formatted strings as the printed table, except that
    missing values become empty cells (Sheets-friendly). Paste path: import the
    CSV into Google Sheets, copy the range, paste into Slides as a table.
    """
    import csv

    with Path(path).open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Today", "Last", "Avg8w", "Diff"])
        for r in report:
            flt = r["is_float"]
            writer.writerow(
                [
                    r["label"],
                    format_value(r["today"], is_float=flt, none=""),
                    format_value(r["last"], is_float=flt, none=""),
                    format_value(r["avg8w"], is_float=flt, avg=True, none=""),
                    format_value(r["diff"], is_float=flt, signed=True, none=""),
                ]
            )

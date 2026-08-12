"""Export slimmed per-notebook parquet for the WASM notebook builds.

The full marimo notebooks read ~155 MB of raw parquet via
:func:`qb_notebook.data_io.load_pr_interval_data`. For the browser/WASM
builds we ship only the tables + columns each notebook's cells actually
read, written *post-parse* (dtypes preserved) with zstd compression, plus a
``manifest.json``. :func:`qb_notebook.wasm_io.load_slimmed_data` reads the
output back in-browser and returns the same dict shape as
``load_pr_interval_data``.

Slimming is **column-only** (all rows kept), so the figures a notebook
produces from the slimmed data are identical to the full-data figures — the
dropped columns are simply never referenced. Row filtering is intentionally
left out to keep that guarantee; revisit only if a table is still too large.

Usage::

    uv run python -m scripts.export_wasm_data --notebook queue_window_state \\
        --data-dir data --out-dir _wasm/queue_window_state/public

    # all configured notebooks, each into <out-root>/<notebook>/public:
    uv run python -m scripts.export_wasm_data --all --out-root _wasm
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from qb_notebook.data_io import _read_and_parse, load_pr_interval_data

# Per-notebook slimming spec: {notebook: {table_key: [columns]}}.
# Columns are the exact set the notebook's cells (and the helpers they call)
# reference. `None` would mean "all columns" but we prefer explicit lists so
# a schema change surfaces as a clear error here rather than silently
# shipping unused data.
SPECS: dict[str, dict[str, list[str]]] = {
    # Theme 1 companion: queue-window state. Reads prs / events /
    # queue_windows only (no check_runs, status_contexts, labels, inline).
    #   events    -> label_intervals + stage_timestamps + label_overlap
    #   queue_win -> queue_window_intervals (rest of its columns are derived)
    #   prs       -> merged_prs_frame + size_buckets + pr_type + pr_close
    "queue_window_state": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "closed_at",
            "merged_at",
            "is_draft",
            "title",
            "base_ref_name",
            "state",
            "additions",
            "deletions",
            "changed_files_count",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "queue_windows": [
            "pull_request_id",
            "rule_set_id",
            "from_ts",
            "to_ts",
            "cycle_index",
            "window_count",
            "first_on_queue_ts",
            "opened_by_event_type",
            "closed_by_event_type",
        ],
    },
    # Theme 5: PR-shape effects. Same three tables as queue_window_state
    #   prs        -> size_buckets + author_cohort + pr_type + started_as_draft
    #                 + had_wip_label_at_open + merged_prs_frame
    #   events     -> attribute_label_events + reviewers_court_intervals
    #                 (awaiting-review fallback) + started_as_draft /
    #                 had_wip_label_at_open event scans + LABELED filters
    #   queue_win  -> reviewers_court_intervals (queue_window_intervals)
    "pr_shape_effects": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "closed_at",
            "merged_at",
            "is_draft",
            "title",
            "base_ref_name",
            "state",
            "additions",
            "deletions",
            "changed_files_count",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "queue_windows": [
            "pull_request_id",
            "rule_set_id",
            "from_ts",
            "to_ts",
            "cycle_index",
            "window_count",
            "first_on_queue_ts",
            "opened_by_event_type",
            "closed_by_event_type",
        ],
    },
    # Theme: temporal patterns. Reads prs / events / core_user (no queue
    # windows). core_user supplies author_login for first_review_touch and the
    # author-vs-reviewer activity-window overlap.
    #   prs       -> opens histogram + merged_prs_frame + monthly counts;
    #                joined to core_user for author_login
    #   events    -> first_review_touch + attribute_label_events +
    #                actor_activity_window
    #   core_user -> id -> github_login (author_login)
    "temporal_patterns": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "closed_at",
            "merged_at",
            "title",
            "base_ref_name",
            "state",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
    },
    # Theme 3: bottleneck localization. Reads prs / events / queue_windows /
    # label_defs / core_user, plus inline_comments (§9) when the artifact has
    # it. core_user supplies author_login so inline_comment_stats can exclude
    # self-comments. inline_comments is an optional load_pr_interval_data key;
    # shipping it keeps §9 identical in-browser (a build on an artifact without
    # it would error here, matching the notebook's own data expectations).
    #   prs            -> merged_prs_frame + size_buckets/pr_type + §5/§6
    #                     (number, head_ci_state); joined to core_user
    #   events         -> label_intervals (mm / stall labels / t-* areas)
    #   queue_windows  -> §3 bounce cycles + §4 close-reason mix (ruleset 3)
    #   label_defs     -> t-* area name list
    #   core_user      -> id -> github_login (author_login)
    #   inline_comments-> §9 inline_comment_stats
    "bottleneck_localization": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "closed_at",
            "merged_at",
            "base_ref_name",
            "title",
            "state",
            "number",
            "head_ci_state",
            "additions",
            "deletions",
            "changed_files_count",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "queue_windows": [
            "pull_request_id",
            "rule_set_id",
            "cycle_index",
            "closed_by_event_type",
            "to_ts",
        ],
        "label_defs": [
            "name",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
        "inline_comments": [
            "pull_request_id",
            "author_login",
            "thread_root_node_id",
            "reply_to_node_id",
            "path",
            "gh_created_at",
        ],
    },
    # Theme: request effectiveness (review-request + assignment response).
    # Reads prs / events / queue_windows / core_user. The events table needs
    # the assignment-specific raw columns (id, assignee_login,
    # requested_reviewer_login) on top of the usual five.
    #   prs           -> human-PR-to-master filter (repository_id,
    #                    base_ref_name) + size_buckets/pr_type/author_cohort +
    #                    merged_at_effective; joined to core_user
    #   events        -> classify_assignment/unassign + review_request_responses
    #                    + assignment_responses + attribute_label_events +
    #                    label_intervals (t-* areas)
    #   queue_windows -> queue_window_intervals (ruleset 3) for the
    #                    intervention/queue-cycle cell
    #   core_user     -> id -> github_login (author_login)
    "request_effectiveness": {
        "prs": [
            "id",
            "repository_id",
            "base_ref_name",
            "author_id",
            "gh_created_at",
            "closed_at",
            "merged_at",
            "title",
            "state",
            "additions",
            "deletions",
            "changed_files_count",
        ],
        "events": [
            "id",
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
            "assignee_login",
            "requested_reviewer_login",
        ],
        "queue_windows": [
            "pull_request_id",
            "rule_set_id",
            "from_ts",
            "to_ts",
            "cycle_index",
            "window_count",
            "first_on_queue_ts",
            "opened_by_event_type",
            "closed_by_event_type",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
    },
    # Theme 4: topic-area health. Reads prs / events / label_defs / prlabel /
    # queue_windows (no core_user). Team membership ships separately as a
    # teams.json snapshot (see scripts/build_wasm_site.write_teams_snapshot).
    #   prs           -> open backlog (state, gh_created_at) + merged_prs_frame
    #   events        -> label_intervals (t-* + maintainer-merge) +
    #                    attribute_label_events + reviewers_court_intervals
    #   label_defs    -> t-* inventory (id, name)
    #   prlabel       -> current-state open backlog join (label_def_id -> id)
    #   queue_windows -> queue_window_intervals + reviewers_court_intervals
    "area_health": {
        "prs": [
            "id",
            "state",
            "gh_created_at",
            "merged_at",
            "closed_at",
            "base_ref_name",
            "title",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "label_defs": [
            "id",
            "name",
        ],
        "prlabel": [
            "label_def_id",
            "pull_request_id",
        ],
        "queue_windows": [
            "pull_request_id",
            "rule_set_id",
            "from_ts",
            "to_ts",
            "cycle_index",
            "window_count",
            "first_on_queue_ts",
            "opened_by_event_type",
            "closed_by_event_type",
        ],
    },
    # Theme 2: reviewer load. Reads prs / events / core_user (no queue
    # windows). Team membership ships separately as a teams.json snapshot.
    # events needs the assignment columns (id, assignee_login,
    # requested_reviewer_login) on top of the usual five.
    #   prs       -> size_buckets/author_cohort/pr_type + first_review_touch;
    #                joined to core_user. Uses raw merged_at (not bors-aware).
    #   events    -> attribute_label_events + first_review_touch +
    #                classify_assignment_events + review_request_responses
    #   core_user -> id -> github_login (author_login)
    "reviewer_load": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "title",
            "additions",
            "deletions",
            "changed_files_count",
            "merged_at",
            "state",
        ],
        "events": [
            "id",
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
            "assignee_login",
            "requested_reviewer_login",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
    },
    # Theme 1: review state machine. Reads prs / events / core_user (no queue
    # windows — it reconstructs court state from the retired awaiting-review
    # label). Team membership ships separately as a teams.json snapshot.
    #   prs       -> merged_prs_frame + expr_is_draft + size_buckets/pr_type;
    #                joined to core_user for the self-merge attribution cell
    #   events    -> label_intervals + stage_timestamps + attribute_label_events
    #   core_user -> id -> github_login (author_login)
    "review_state_machine": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "closed_at",
            "merged_at",
            "base_ref_name",
            "title",
            "state",
            "is_draft",
            "additions",
            "deletions",
            "changed_files_count",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
    },
    # Anatomy of a merge. Reads prs / events / queue_windows / core_user, plus
    # inline_comments when present. Offline-only export cells (Sankey -> PNG via
    # kaleido, HTML dumps) are guarded behind `not is_wasm` in the notebook.
    #   prs            -> human-PR-to-master filter + size_buckets/pr_type/
    #                     author_cohort + §-tables (number, title); joined to
    #                     core_user. Bors-aware merged_at_effective.
    #   events         -> pipeline_stages + label_intervals + labels_active_at
    #   queue_windows  -> queue_window_intervals (cycles before/after MM)
    #   core_user      -> id -> github_login (author_login)
    #   inline_comments-> inline_comment_stats (review-depth columns)
    "anatomy_of_a_merge": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "merged_at",
            "closed_at",
            "base_ref_name",
            "title",
            "state",
            "number",
            "additions",
            "deletions",
            "changed_files_count",
            "repository_id",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "queue_windows": [
            "pull_request_id",
            "rule_set_id",
            "from_ts",
            "to_ts",
            "cycle_index",
            "window_count",
            "first_on_queue_ts",
            "opened_by_event_type",
            "closed_by_event_type",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
        "inline_comments": [
            "pull_request_id",
            "author_login",
            "thread_root_node_id",
            "reply_to_node_id",
            "path",
            "gh_created_at",
        ],
    },
    # Cohort comparison. The multi-cohort companion to anatomy_of_a_merge:
    # same enrichment and same pipeline_stages reconstruction, but no Sankey
    # and no queue windows (it never counts review cycles), and no inline
    # comments. Every figure is matplotlib, so no plotly/kaleido concerns.
    #   prs       -> human-PR-to-master filter + size_buckets/pr_type/
    #                author_cohort (the per-cohort filter dimensions); joined
    #                to core_user. Bors-aware merged_at_effective.
    #   events    -> pipeline_stages + label_intervals (t-* topic filter)
    #   core_user -> id -> github_login (author_login)
    "cohort_comparison": {
        "prs": [
            "id",
            "author_id",
            "gh_created_at",
            "merged_at",
            "closed_at",
            "base_ref_name",
            "title",
            "state",
            "number",
            "additions",
            "deletions",
            "changed_files_count",
            "repository_id",
        ],
        "events": [
            "pull_request_id",
            "type",
            "label_name",
            "occurred_at",
            "actor_login",
        ],
        "core_user": [
            "id",
            "github_login",
        ],
    },
}

# Tables some notebooks read directly (not produced by load_pr_interval_data).
# Map a SPECS table key -> its raw parquet filename under --data-dir.
# Registering a table here lets a notebook's spec slim it like any
# load_pr_interval_data table; load_slimmed_data returns it under the same key
# in-browser. Registration is explicit (not <key>.parquet by convention) so a
# SPECS typo surfaces as a clear error rather than a missing-file read.
EXTRA_TABLE_FILES: dict[str, str] = {
    "core_user": "core_user.parquet",
}

# zstd at a high level: parquet is already columnar+compressed, but zstd
# squeezes the wide string/timestamp columns harder than the snappy default.
_ZSTD_LEVEL = 19


def load_data_for_specs(
    data_dir: str | Path,
    notebooks: Iterable[str],
) -> dict[str, pl.DataFrame]:
    """Load ``load_pr_interval_data`` plus any extra tables the notebooks need.

    A SPECS table key that ``load_pr_interval_data`` does not produce must be
    registered in :data:`EXTRA_TABLE_FILES`; it is read straight from
    ``<data_dir>/<file>`` via the same parse helper ``data_io`` uses, so the
    slimmed frame keeps identical dtypes. The merged dict can be passed to
    :func:`export_notebook` unchanged (its ``data[key]`` lookup just works).
    """
    data = load_pr_interval_data(data_dir)
    root = Path(data_dir)
    needed = {key for nb in notebooks for key in SPECS.get(nb, {})}
    for key in sorted(needed - set(data)):
        if key not in EXTRA_TABLE_FILES:
            raise KeyError(
                f"SPECS references table {key!r}, which is neither a "
                f"load_pr_interval_data key nor registered in "
                f"EXTRA_TABLE_FILES (known extras: {sorted(EXTRA_TABLE_FILES)})"
            )
        data[key] = _read_and_parse(root / EXTRA_TABLE_FILES[key])
    return data


def export_notebook(
    notebook: str,
    data: dict[str, pl.DataFrame],
    out_dir: Path,
) -> dict:
    """Write the slimmed tables for one notebook into ``out_dir``.

    ``data`` is the result of :func:`load_pr_interval_data` (loaded once and
    reused across notebooks). Returns the manifest dict that was written.
    """
    if notebook not in SPECS:
        raise KeyError(
            f"No WASM slimming spec for {notebook!r}; known: {sorted(SPECS)}"
        )
    spec = SPECS[notebook]
    out_dir.mkdir(parents=True, exist_ok=True)

    tables: dict[str, str] = {}
    rows: dict[str, int] = {}
    for key, cols in spec.items():
        if key not in data:
            raise KeyError(
                f"{notebook}: table {key!r} not in loaded data (have {sorted(data)})"
            )
        df = data[key]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise KeyError(f"{notebook}/{key}: columns missing from source: {missing}")
        df = df.select(cols)
        filename = f"{key}.parquet"
        df.write_parquet(
            out_dir / filename,
            compression="zstd",
            compression_level=_ZSTD_LEVEL,
        )
        tables[key] = filename
        rows[key] = df.height

    manifest = {
        "notebook": notebook,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "tables": tables,
        "rows": rows,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _report(out_dir: Path, manifest: dict) -> None:
    total = 0
    print(f"  -> {out_dir}")
    for key, filename in manifest["tables"].items():
        size = (out_dir / filename).stat().st_size
        total += size
        print(
            f"     {filename:<24} {size / 1e6:7.2f} MB  ({manifest['rows'][key]:,} rows)"
        )
    print(f"     {'TOTAL':<24} {total / 1e6:7.2f} MB")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--notebook", help="notebook stem (e.g. queue_window_state)")
    ap.add_argument(
        "--all", action="store_true", help="export all configured notebooks"
    )
    ap.add_argument("--data-dir", default="data", help="raw parquet directory")
    ap.add_argument("--out-dir", help="output dir for a single --notebook")
    ap.add_argument(
        "--out-root",
        default="_wasm",
        help="root for --all; each notebook -> <out-root>/<notebook>/public",
    )
    args = ap.parse_args()

    if not args.all and not args.notebook:
        ap.error("pass --notebook NAME or --all")

    notebooks = sorted(SPECS) if args.all else [args.notebook]

    print(f"Loading raw data from {args.data_dir} ...")
    data = load_data_for_specs(args.data_dir, notebooks)

    for nb in notebooks:
        if args.all:
            out_dir = Path(args.out_root) / nb / "public"
        else:
            out_dir = (
                Path(args.out_dir)
                if args.out_dir
                else Path(args.out_root) / nb / "public"
            )
        print(f"Exporting {nb} ...")
        manifest = export_notebook(nb, data, out_dir)
        _report(out_dir, manifest)


if __name__ == "__main__":
    main()

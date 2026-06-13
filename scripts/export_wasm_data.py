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
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from qb_notebook.data_io import load_pr_interval_data

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
}

# zstd at a high level: parquet is already columnar+compressed, but zstd
# squeezes the wide string/timestamp columns harder than the snappy default.
_ZSTD_LEVEL = 19


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

    print(f"Loading raw data from {args.data_dir} ...")
    data = load_pr_interval_data(args.data_dir)

    notebooks = sorted(SPECS) if args.all else [args.notebook]
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

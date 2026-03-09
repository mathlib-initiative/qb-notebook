from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import matplotlib
import matplotlib.pyplot as plt
import polars as pl

from qb_notebook.data_io import load_pr_interval_data, split_queue_windows_by_rule
from qb_notebook.intervals import (
    effective_queue_prs_per_day,
    enrich_intervals_with_prs,
    snapshot_queue_age_quantiles,
)

matplotlib.use("Agg")


@dataclass(frozen=True)
class PlotDefinition:
    title: str
    output_filename: str
    render: Callable[[dict[str, pl.DataFrame]], plt.Figure]


def _load_context(data_dir: Path) -> dict[str, pl.DataFrame]:
    tables = load_pr_interval_data(data_dir)
    queue_windows = split_queue_windows_by_rule(
        tables["queue_windows"], rule_set_ids=(1, 2, 3)
    )
    df_qw3_enriched = enrich_intervals_with_prs(queue_windows[3], tables["prs"])
    feat_expr = pl.col("title").fill_null("").str.contains(
        r"(^feat)|(^\[Merged by Bors\] -\s+[fF]eat)", literal=False
    )
    return {
        "df_qw1": queue_windows[1],
        "df_qw2": queue_windows[2],
        "df_qw3": queue_windows[3],
        "df_qw3_feat": df_qw3_enriched.filter(feat_expr),
        "df_qw3_nonfeat": df_qw3_enriched.filter(~feat_expr),
    }


def render_qw3_age_percentiles(context: dict[str, pl.DataFrame]) -> plt.Figure:
    quantiles = [0.75, 0.90]
    pdf = snapshot_queue_age_quantiles(context["df_qw3"], quantiles).to_pandas()

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for q in quantiles:
        col = f"p{int(q * 100)}"
        ax.plot(pdf["date"], pdf[col], label=col)

    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("age (days) at end-of-day")
    ax.set_title("Queue window age percentiles over time")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig

def render_qw3_age_percentiles_year(context: dict[str, pl.DataFrame]) -> plt.Figure:
    quantiles = [0.75, 0.90]
    pdf = snapshot_queue_age_quantiles(context["df_qw3"], quantiles).to_pandas()
    if not pdf.empty:
        cutoff = pdf["date"].max() - timedelta(days=365)
        pdf = pdf[pdf["date"] > cutoff]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for q in quantiles:
        col = f"p{int(q * 100)}"
        ax.plot(pdf["date"], pdf[col], label=col)

    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("age (days) at end-of-day")
    ax.set_title("Queue window age percentiles over time (last 365 days)")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


def _build_qw3_feat_nonfeat_daily(context: dict[str, pl.DataFrame]) -> pl.DataFrame:
    daily_feat = effective_queue_prs_per_day(context["df_qw3_feat"]).rename(
        {"prs_on_queue": "feat"}
    )
    daily_nonfeat = effective_queue_prs_per_day(context["df_qw3_nonfeat"]).rename(
        {"prs_on_queue": "non_feat"}
    )
    return (
        daily_feat.join(daily_nonfeat, on="day", how="full", coalesce=True)
        .with_columns(pl.col("feat").fill_null(0), pl.col("non_feat").fill_null(0))
        .sort("day")
    )


def render_qw3_feat_nonfeat_queue_counts(context: dict[str, pl.DataFrame]) -> plt.Figure:
    pdf = _build_qw3_feat_nonfeat_daily(context).to_pandas()

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(pdf["day"], pdf["feat"], label="feat")
    ax.plot(pdf["day"], pdf["non_feat"], label="non-feat")

    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("PRs on queue")
    ax.set_title("Queue window 3 PRs on queue: feat vs non-feat")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


def render_qw3_feat_nonfeat_queue_counts_year(
    context: dict[str, pl.DataFrame],
) -> plt.Figure:
    pdf = _build_qw3_feat_nonfeat_daily(context).to_pandas()
    if not pdf.empty:
        cutoff = pdf["day"].max() - timedelta(days=365)
        pdf = pdf[pdf["day"] > cutoff]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(pdf["day"], pdf["feat"], label="feat")
    ax.plot(pdf["day"], pdf["non_feat"], label="non-feat")

    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("PRs on queue")
    ax.set_title("Queue window 3 PRs on queue: feat vs non-feat (last 365 days)")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


PLOTS: list[PlotDefinition] = [
    PlotDefinition(
        title="Queue window age percentiles over time (df_qw3, last 365 days)",
        output_filename="queue-window-age-percentiles-qw3-last-year.png",
        render=render_qw3_age_percentiles_year,
    ),
    PlotDefinition(
        title="Queue window age percentiles over time (df_qw3)",
        output_filename="queue-window-age-percentiles-qw3.png",
        render=render_qw3_age_percentiles,
    ),
    PlotDefinition(
        title="Queue window 3 PRs on queue: feat vs non-feat (last 365 days)",
        output_filename="queue-window-prs-feat-vs-nonfeat-qw3-last-year.png",
        render=render_qw3_feat_nonfeat_queue_counts_year,
    ),
    PlotDefinition(
        title="Queue window 3 PRs on queue: feat vs non-feat",
        output_filename="queue-window-prs-feat-vs-nonfeat-qw3.png",
        render=render_qw3_feat_nonfeat_queue_counts,
    ),
]


def _write_index(site_dir: Path, plots: list[PlotDefinition]) -> None:
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sections = "\n".join(
        f"""    <section class="card">
      <h2>{plot.title}</h2>
      <img src="images/{plot.output_filename}" alt="{plot.title}" loading="lazy" />
    </section>"""
        for plot in plots
    )

    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>qb-notebook plots</title>
    <style>
      :root {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        color: #152235;
        background: #f8fafc;
      }}
      body {{
        margin: 0;
        padding: 2rem;
      }}
      main {{
        max-width: 1200px;
        margin: 0 auto;
      }}
      .card {{
        background: #fff;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        padding: 1rem;
        margin-bottom: 1.5rem;
      }}
      img {{
        width: 100%;
        height: auto;
      }}
      .timestamp {{
        margin-bottom: 1.25rem;
        color: #425466;
      }}
    </style>
  </head>
  <body>
    <main>
      <h1>qb-notebook generated plots</h1>
      <p class="timestamp">Generated at: {generated_at}</p>
{sections}
    </main>
  </body>
</html>
"""
    (site_dir / "index.html").write_text(html)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate static plot site content.")
    parser.add_argument(
        "--data-dir", default="data", help="Directory with parquet data."
    )
    parser.add_argument(
        "--site-dir",
        default="_site",
        help="Directory where index.html and images/ are written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    site_dir = Path(args.site_dir)
    images_dir = site_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    context = _load_context(Path(args.data_dir))
    for plot in PLOTS:
        fig = plot.render(context)
        fig.savefig(images_dir / plot.output_filename, dpi=150)
        plt.close(fig)

    _write_index(site_dir, PLOTS)


if __name__ == "__main__":
    main()

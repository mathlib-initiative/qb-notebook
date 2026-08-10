from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Callable

import matplotlib
import matplotlib.pyplot as plt
import polars as pl

from qb_notebook.data_io import (
    load_contributor_config,
    load_pr_interval_data,
    split_queue_windows_by_rule,
)
from qb_notebook.filters import (
    expr_commenters_include_any,
    expr_pr_has_any_of,
    pr_ids_with_any_labels,
)
from qb_notebook.intervals import (
    enrich_intervals_with_prs,
    snapshot_queue_age_quantiles,
)

matplotlib.use("Agg")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRIBUTORS_PATH = REPO_ROOT / "contributors_MI.json"

NON_YEAR_PLOT_START = datetime(2023, 1, 1, tzinfo=timezone.utc)

LLM_LABEL = "LLM-generated"
# The `LLM-generated` label was only created on 2026-03-16, and December 2025
# is the first month with non-trivial LLM-labelled queue activity. Starting the
# LLM plots at NON_YEAR_PLOT_START would prepend three years of flat zero.
LLM_PLOT_START = datetime(2025, 12, 1, tzinfo=timezone.utc)
LLM_PLOT_NOTE = (
    "The LLM-generated label was created on 2026-03-16; PRs from before that "
    "date carry it only if it was applied retroactively, so the earliest part "
    "of the LLM series undercounts."
)

SERIES_COLORS = {
    "p75": "#1f77b4",
    "p90": "#d62728",
    "feat": "#2ca02c",
    "non_feat": "#ff7f0e",
    "merged_14d_avg": "#9467bd",
    "mi": "#2e0050",
    "non_mi": "#ff0073",
    "llm": "#17becf",
    "non_llm": "#7f7f7f",
}


@dataclass(frozen=True)
class PlotDefinition:
    title: str
    output_filename: str
    render: Callable[[dict[str, pl.DataFrame]], plt.Figure]
    note: str | None = None


@dataclass(frozen=True)
class PlotWindow:
    """A time range applied to a plot's x-axis.

    ``start`` is an absolute cutoff; ``None`` means the trailing 365 days
    relative to the last date present in the data.
    """

    start: datetime | None
    caption: str
    slug_suffix: str


FULL_WINDOW = PlotWindow(NON_YEAR_PLOT_START, "since 2023-01-01", "")
LAST_YEAR_WINDOW = PlotWindow(None, "last 365 days", "-last-year")
LLM_WINDOW = PlotWindow(LLM_PLOT_START, "since 2025-12-01", "")


@dataclass(frozen=True)
class SplitSpec:
    """A two-way partition of PRs, drawn as a pair of series in one figure.

    ``frame_a`` / ``frame_b`` are `_load_context` keys holding the two
    complementary subsets.
    """

    slug: str
    caption: str
    frame_a: str
    frame_b: str
    label_a: str
    label_b: str
    color_a: str
    color_b: str


def _qw3_asof(context: dict[str, pl.DataFrame]) -> datetime:
    return context["df_qw3"].select(pl.max("updated_at")).item()


def _filter_since(pdf, date_col: str, start: datetime):
    if pdf.empty:
        return pdf
    series = pdf[date_col]
    try:
        series_tz = series.dt.tz
    except AttributeError:
        return pdf[series >= start.date()]

    if series_tz is None:
        start_cmp = start.replace(tzinfo=None)
    else:
        start_cmp = start.astimezone(series_tz)
    return pdf[series >= start_cmp]


def _apply_window(pdf, date_col: str, window: PlotWindow):
    if window.start is not None:
        return _filter_since(pdf, date_col, window.start)
    if pdf.empty:
        return pdf
    cutoff = pdf[date_col].max() - timedelta(days=365)
    return pdf[pdf[date_col] > cutoff]


def _load_context(
    data_dir: Path,
    contributors_path: Path = DEFAULT_CONTRIBUTORS_PATH,
) -> dict[str, pl.DataFrame]:
    tables = load_pr_interval_data(data_dir)
    queue_windows = split_queue_windows_by_rule(
        tables["queue_windows"], rule_set_ids=(1, 2, 3)
    )
    df_merged = tables["prs"].filter(
        pl.col("closed_at").is_not_null()
        & pl.col("title")
        .fill_null("")
        .str.contains(r"(?i)^\[Merged by Bors\] -", literal=False)
    )
    df_qw3_enriched = enrich_intervals_with_prs(queue_windows[3], tables["prs"])

    # Queue windows cover open PRs too, whose titles lack the bors prefix.
    qw3_feat_expr = (
        pl.col("title")
        .fill_null("")
        .str.contains(r"(^feat)|(^\[Merged by Bors\] -\s+[fF]eat)", literal=False)
    )
    merged_feat_expr = (
        pl.col("title")
        .fill_null("")
        .str.contains(r"^\[Merged by Bors\] -\s+[fF]eat", literal=False)
    )

    mi_logins = [entry.login for entry in load_contributor_config(contributors_path)]
    mi_expr = expr_commenters_include_any(mi_logins)

    llm_pr_ids = pr_ids_with_any_labels(
        tables["prlabel"], tables["label_defs"], [LLM_LABEL]
    )
    qw3_llm_expr = expr_pr_has_any_of(llm_pr_ids)
    merged_llm_expr = expr_pr_has_any_of(llm_pr_ids, pr_id_col="id")

    return {
        "df_qw1": queue_windows[1],
        "df_qw2": queue_windows[2],
        "df_qw3": queue_windows[3],
        "df_qw3_feat": df_qw3_enriched.filter(qw3_feat_expr),
        "df_qw3_nonfeat": df_qw3_enriched.filter(~qw3_feat_expr),
        "df_qw3_llm": df_qw3_enriched.filter(qw3_llm_expr),
        "df_qw3_nonllm": df_qw3_enriched.filter(~qw3_llm_expr),
        "df_merged": df_merged,
        "df_merged_feat": df_merged.filter(merged_feat_expr),
        "df_merged_nonfeat": df_merged.filter(~merged_feat_expr),
        "df_merged_mi": df_merged.filter(mi_expr),
        "df_merged_nonmi": df_merged.filter(~mi_expr),
        "df_merged_llm": df_merged.filter(merged_llm_expr),
        "df_merged_nonllm": df_merged.filter(~merged_llm_expr),
    }


def _render_qw3_age_percentiles(context, window: PlotWindow) -> plt.Figure:
    quantiles = [0.75, 0.90]
    pdf = snapshot_queue_age_quantiles(
        context["df_qw3"], quantiles, asof=_qw3_asof(context)
    ).to_pandas()
    pdf = _apply_window(pdf, "date", window)

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for q in quantiles:
        col = f"p{int(q * 100)}"
        ax.plot(pdf["date"], pdf[col], label=col, color=SERIES_COLORS[col])

    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("age (days) at end-of-day")
    ax.set_title(f"Queue window age percentiles over time ({window.caption})")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


def render_qw3_age_percentiles(context: dict[str, pl.DataFrame]) -> plt.Figure:
    return _render_qw3_age_percentiles(context, FULL_WINDOW)


def render_qw3_age_percentiles_year(context: dict[str, pl.DataFrame]) -> plt.Figure:
    return _render_qw3_age_percentiles(context, LAST_YEAR_WINDOW)


def _build_qw3_split_daily(
    context: dict[str, pl.DataFrame], spec: SplitSpec
) -> pl.DataFrame:
    """Daily count of distinct PRs on the queue, for each side of ``spec``."""
    asof = _qw3_asof(context)

    def _daily_counts(df: pl.DataFrame, out_col: str) -> pl.DataFrame:
        return (
            df.with_columns(
                pl.coalesce([pl.col("to_ts"), pl.col("closed_at"), pl.lit(asof)]).alias(
                    "to_ts_effective"
                )
            )
            .select(
                [
                    pl.col("pull_request_id"),
                    pl.col("from_ts").dt.date().alias("start_day"),
                    (pl.col("to_ts_effective") - pl.duration(microseconds=1))
                    .dt.date()
                    .alias("end_day"),
                ]
            )
            .filter(pl.col("end_day") >= pl.col("start_day"))
            .with_columns(
                pl.date_ranges(
                    pl.col("start_day"),
                    pl.col("end_day"),
                    interval="1d",
                    closed="both",
                ).alias("day")
            )
            .explode("day")
            .group_by("day")
            .agg(pl.col("pull_request_id").n_unique().alias(out_col))
            .sort("day")
        )

    daily_a = _daily_counts(context[spec.frame_a], "a")
    daily_b = _daily_counts(context[spec.frame_b], "b")
    return (
        daily_a.join(daily_b, on="day", how="full", coalesce=True)
        .with_columns(pl.col("a").fill_null(0), pl.col("b").fill_null(0))
        .sort("day")
    )


def _render_qw3_split_queue_counts(
    context: dict[str, pl.DataFrame], spec: SplitSpec, window: PlotWindow
) -> plt.Figure:
    pdf = _apply_window(
        _build_qw3_split_daily(context, spec).to_pandas(), "day", window
    )

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(pdf["day"], pdf["a"], label=spec.label_a, color=spec.color_a)
    ax.plot(pdf["day"], pdf["b"], label=spec.label_b, color=spec.color_b)

    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("PRs on queue")
    ax.set_title(f"Queue window 3 PRs on queue: {spec.caption} ({window.caption})")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


def _build_merged_per_day(context: dict[str, pl.DataFrame]) -> pl.DataFrame:
    return (
        context["df_merged"]
        .with_columns(pl.col("closed_at").dt.truncate("1d").alias("date"))
        .group_by("date")
        .agg(pl.len().alias("prs_merged"))
        .sort(
            "date"
        )  # must sort before rolling_mean; Polars rolling_mean is row-order-based, not time-based
        .with_columns(pl.col("prs_merged").rolling_mean(14).alias("prs_merged_14d_avg"))
    )


def _render_merged_per_day(pdf, title) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(
        pdf["date"],
        pdf["prs_merged"],
        alpha=0.3,
        label="daily count",
        color=SERIES_COLORS["merged_14d_avg"],
        width=1,
    )
    ax.plot(
        pdf["date"],
        pdf["prs_merged_14d_avg"],
        label="14d avg",
        color=SERIES_COLORS["merged_14d_avg"],
    )
    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("PRs merged")
    ax.set_ylim(bottom=0)
    ax.set_title(title)
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


def _build_merged_split_per_day(
    context: dict[str, pl.DataFrame], spec: SplitSpec
) -> pl.DataFrame:
    """Daily merge counts plus 14-day rolling averages for each side of ``spec``.

    The two sides are joined (and zero-filled) *before* the rolling average, so
    a day with no merges on one side counts as a zero in that side's window
    rather than being dropped from it. Polars' `rolling_mean` is row-based, so
    rolling each side separately would silently average a sparse series over a
    much longer calendar span and inflate it — badly so for a rare category
    like the LLM split. Joining first also makes the two rolling series sum
    back to `_build_merged_per_day`'s total.

    Days on which *nothing* merged are still absent from the frame (three since
    2023), matching `_build_merged_per_day`.
    """

    def _daily(df: pl.DataFrame, out_col: str) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("closed_at").dt.truncate("1d").alias("date"))
            .group_by("date")
            .agg(pl.len().alias(out_col))
        )

    daily_a = _daily(context[spec.frame_a], "a")
    daily_b = _daily(context[spec.frame_b], "b")
    return (
        daily_a.join(daily_b, on="date", how="full", coalesce=True)
        .with_columns(pl.col("a").fill_null(0), pl.col("b").fill_null(0))
        .sort(
            "date"
        )  # must sort before rolling_mean; Polars rolling_mean is row-order-based, not time-based
        .with_columns(
            pl.col("a").rolling_mean(14).alias("a_14d_avg"),
            pl.col("b").rolling_mean(14).alias("b_14d_avg"),
        )
    )


def _render_merged_split_per_day(
    context: dict[str, pl.DataFrame], spec: SplitSpec, window: PlotWindow
) -> plt.Figure:
    pdf = _apply_window(
        _build_merged_split_per_day(context, spec).to_pandas(), "date", window
    )

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(
        pdf["date"],
        pdf["a_14d_avg"],
        label=f"{spec.label_a} (14d avg)",
        color=spec.color_a,
    )
    ax.plot(
        pdf["date"],
        pdf["b_14d_avg"],
        label=f"{spec.label_b} (14d avg)",
        color=spec.color_b,
    )
    ax.legend()
    ax.set_xlabel("date (UTC)")
    ax.set_ylabel("PRs merged")
    ax.set_ylim(bottom=0)
    ax.set_title(f"PRs merged per day: {spec.caption} (14d avg, {window.caption})")
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    return fig


def render_merged_per_day(context: dict[str, pl.DataFrame]) -> plt.Figure:
    pdf = _apply_window(_build_merged_per_day(context).to_pandas(), "date", FULL_WINDOW)
    return _render_merged_per_day(
        pdf,
        "PRs merged per day (14d avg, title matches 'Merged by Bors', since 2023-01-01)",
    )


def render_merged_per_day_year(context: dict[str, pl.DataFrame]) -> plt.Figure:
    pdf = _apply_window(
        _build_merged_per_day(context).to_pandas(), "date", LAST_YEAR_WINDOW
    )
    return _render_merged_per_day(
        pdf,
        "PRs merged per day (14d avg, title matches 'Merged by Bors', last 365 days)",
    )


QW3_FEAT_SPLIT = SplitSpec(
    slug="feat-vs-nonfeat",
    caption="feat vs non-feat",
    frame_a="df_qw3_feat",
    frame_b="df_qw3_nonfeat",
    label_a="feat",
    label_b="non-feat",
    color_a=SERIES_COLORS["feat"],
    color_b=SERIES_COLORS["non_feat"],
)
QW3_LLM_SPLIT = SplitSpec(
    slug="llm-vs-nonllm",
    caption="LLM vs non-LLM",
    frame_a="df_qw3_llm",
    frame_b="df_qw3_nonllm",
    label_a="LLM-generated",
    label_b="non-LLM",
    color_a=SERIES_COLORS["llm"],
    color_b=SERIES_COLORS["non_llm"],
)
MERGED_FEAT_SPLIT = SplitSpec(
    slug="feat-vs-nonfeat",
    caption="feat vs non-feat",
    frame_a="df_merged_feat",
    frame_b="df_merged_nonfeat",
    label_a="feat",
    label_b="non-feat",
    color_a=SERIES_COLORS["feat"],
    color_b=SERIES_COLORS["non_feat"],
)
MERGED_MI_SPLIT = SplitSpec(
    slug="mi-vs-nonmi",
    caption="MI vs non-MI",
    frame_a="df_merged_mi",
    frame_b="df_merged_nonmi",
    label_a="MI",
    label_b="non-MI",
    color_a=SERIES_COLORS["mi"],
    color_b=SERIES_COLORS["non_mi"],
)
MERGED_LLM_SPLIT = SplitSpec(
    slug="llm-vs-nonllm",
    caption="LLM vs non-LLM",
    frame_a="df_merged_llm",
    frame_b="df_merged_nonllm",
    label_a="LLM-generated",
    label_b="non-LLM",
    color_a=SERIES_COLORS["llm"],
    color_b=SERIES_COLORS["non_llm"],
)


def _qw3_split_plot(
    spec: SplitSpec, window: PlotWindow, *, note: str | None = None
) -> PlotDefinition:
    return PlotDefinition(
        title=f"Queue window 3 PRs on queue: {spec.caption} ({window.caption})",
        output_filename=f"queue-window-prs-{spec.slug}-qw3{window.slug_suffix}.png",
        render=lambda context: _render_qw3_split_queue_counts(context, spec, window),
        note=note,
    )


def _merged_split_plot(
    spec: SplitSpec, window: PlotWindow, *, note: str | None = None
) -> PlotDefinition:
    return PlotDefinition(
        title=f"PRs merged per day: {spec.caption} (14d avg, {window.caption})",
        output_filename=f"prs-merged-per-day-{spec.slug}{window.slug_suffix}.png",
        render=lambda context: _render_merged_split_per_day(context, spec, window),
        note=note,
    )


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
    _qw3_split_plot(QW3_FEAT_SPLIT, LAST_YEAR_WINDOW),
    _qw3_split_plot(QW3_FEAT_SPLIT, FULL_WINDOW),
    _qw3_split_plot(QW3_LLM_SPLIT, LLM_WINDOW, note=LLM_PLOT_NOTE),
    PlotDefinition(
        title="PRs merged per day (14d avg, title matches 'Merged by Bors', last 365 days)",
        output_filename="prs-merged-per-day-bors-last-year.png",
        render=render_merged_per_day_year,
    ),
    PlotDefinition(
        title="PRs merged per day (14d avg, title matches 'Merged by Bors')",
        output_filename="prs-merged-per-day-bors.png",
        render=render_merged_per_day,
    ),
    _merged_split_plot(MERGED_FEAT_SPLIT, LAST_YEAR_WINDOW),
    _merged_split_plot(MERGED_FEAT_SPLIT, FULL_WINDOW),
    _merged_split_plot(MERGED_MI_SPLIT, LAST_YEAR_WINDOW),
    _merged_split_plot(MERGED_MI_SPLIT, FULL_WINDOW),
    _merged_split_plot(MERGED_LLM_SPLIT, LLM_WINDOW, note=LLM_PLOT_NOTE),
]


def _render_section(plot: PlotDefinition) -> str:
    anchor = plot.output_filename.removesuffix(".png")
    lines = [
        f'    <section class="card" id="{anchor}">',
        f'      <h2><a href="#{anchor}">{escape(plot.title)}</a></h2>',
    ]
    if plot.note:
        lines.append(f'      <p class="note">{escape(plot.note)}</p>')
    lines.append(
        f'      <img src="images/{plot.output_filename}"'
        f' alt="{escape(plot.title)}" loading="lazy" />'
    )
    lines.append("    </section>")
    return "\n".join(lines)


def _write_index(site_dir: Path, plots: list[PlotDefinition]) -> None:
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sections = "\n".join(_render_section(plot) for plot in plots)

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
      .note {{
        margin: 0 0 0.75rem;
        color: #425466;
        font-size: 0.9rem;
      }}
      a {{
        all: unset;
      }}
      a:hover {{
        text-decoration: underline;
        cursor: pointer;
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
    parser.add_argument(
        "--contributors",
        default=str(DEFAULT_CONTRIBUTORS_PATH),
        help="Contributor config JSON defining the MI cohort.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    site_dir = Path(args.site_dir)
    images_dir = site_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    context = _load_context(Path(args.data_dir), Path(args.contributors))
    for plot in PLOTS:
        fig = plot.render(context)
        fig.savefig(images_dir / plot.output_filename, dpi=150)
        plt.close(fig)

    _write_index(site_dir, PLOTS)


if __name__ == "__main__":
    main()

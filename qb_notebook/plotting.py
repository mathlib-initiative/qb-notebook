from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from matplotlib import transforms
from matplotlib.collections import LineCollection
from scipy.stats import fisk, lognorm, weibull_min


def label_series(df: pl.DataFrame, value_col: str, label: str) -> pl.DataFrame:
    return df.select(["date", pl.col(value_col).alias(value_col)]).with_columns(
        pl.lit(label).alias("series")
    )


def prepare_swimlane_polars(
    intervals: pl.DataFrame,
    *,
    asof: datetime | None = None,
    max_prs: int | None = None,
) -> pl.DataFrame:
    if asof is None:
        asof = datetime.now(tz=timezone.utc)

    df = intervals.with_columns(
        pl.coalesce([pl.col("end"), pl.lit(asof)]).alias("end_effective")
    )

    order = (
        df.group_by("pull_request_id")
        .agg(pl.min("start").alias("first_open"))
        .sort("first_open")
        .with_row_index("y")
    )
    if max_prs is not None:
        order = order.head(max_prs)

    return (
        df.join(
            order.select(["pull_request_id", "y"]), on="pull_request_id", how="inner"
        )
        .select(["pull_request_id", "start", "end_effective", "y"])
        .sort(["y", "start"])
    )


def plot_swimlane_matplotlib(
    swim: pl.DataFrame,
    *,
    figsize: tuple[int, int] = (12, 12),
    linewidth: float = 0.5,
    title: str | None = None,
) -> None:
    start = swim["start"].to_numpy()
    end = swim["end_effective"].to_numpy()
    y = swim["y"].to_numpy()

    x0 = mdates.date2num(start)
    x1 = mdates.date2num(end)
    segments = [((a, yi), (b, yi)) for a, b, yi in zip(x0, x1, y)]

    fig, ax = plt.subplots(figsize=figsize)
    lc = LineCollection(segments, linewidths=linewidth)
    lc.set_rasterized(True)
    ax.add_collection(lc)

    ax.set_xlim(x0.min(), x1.max())
    ax.set_ylim(-1, y.max() + 1)
    ax.xaxis_date()
    fig.autofmt_xdate()

    ax.set_xlabel("Time")
    ax.set_ylabel("PR (earliest at bottom)")
    ax.set_title(title or "PR Open Intervals (swimlane)")
    plt.tight_layout()
    plt.show()


def get_x(df: pl.DataFrame, col: str) -> np.ndarray:
    x = df.select(pl.col(col)).to_numpy().ravel().astype(float)
    return x[np.isfinite(x) & (x > 0)]


def _bin_edges(x: np.ndarray, bins: int, *, logx: bool) -> np.ndarray:
    if logx:
        lo = max(x.min(), np.nextafter(0, 1))
        hi = x.max()
        return np.logspace(np.log10(lo), np.log10(hi), bins + 1)
    return np.linspace(x.min(), x.max(), bins + 1)


def plot_duration_hist(
    df: pl.DataFrame,
    *,
    col: str = "duration_days",
    bins: int = 100,
    logx: bool = False,
    logy: bool = False,
    exponential_fit: bool = False,
    title: str | None = None,
) -> None:
    x = get_x(df, col)
    if x.size < 2:
        print("Not enough data to plot.")
        return

    edges = _bin_edges(x, bins, logx=logx)
    fig, ax = plt.subplots(figsize=(10, 4))
    _, edges, _ = ax.hist(x, bins=edges)

    if logx:
        ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")

    if exponential_fit and logy:
        n = x.size
        lam = 1.0 / x.mean()
        centers = 0.5 * (edges[:-1] + edges[1:])
        widths = np.diff(edges)
        expected = n * lam * np.exp(-lam * centers) * widths
        ax.plot(centers, expected, linewidth=3, label=f"Exponential (λ={lam:.3g})")
        ax.legend()

    ax.set_xlabel(col.replace("_", " "))
    ax.set_ylabel("Count")
    ax.set_title(title or f"Histogram of {col}")
    plt.tight_layout()
    plt.show()


def plot_duration_hists(
    datasets: Iterable[tuple[pl.DataFrame, str]],
    *,
    col: str = "duration_days",
    bins: int = 100,
    logx: bool = False,
    logy: bool = False,
    title: str | None = None,
    alpha: float = 0.5,
    percentiles: list[float] | None = None,
    percentile_style: str = "vline",
    value_fmt: str = "{:.3g}",
    label_rotate: float = 90,
    label_stagger: float = 0.06,
) -> None:
    xs: list[np.ndarray] = []
    labels: list[str] = []
    for df, label in datasets:
        x = get_x(df, col)
        if x.size >= 2:
            xs.append(x)
            labels.append(label)

    if not xs:
        print("Not enough data to plot.")
        return

    all_x = np.concatenate(xs)
    bin_edges = _bin_edges(all_x, bins, logx=logx)

    fig, ax = plt.subplots(figsize=(10, 4))
    trans_top = transforms.blended_transform_factory(ax.transData, ax.transAxes)
    base_y = 0.98

    for i, (x, label) in enumerate(zip(xs, labels)):
        _, _, patches = ax.hist(
            x,
            bins=bin_edges,
            alpha=alpha,
            label=label,
            histtype="stepfilled",
        )

        color = patches[0].get_facecolor() if patches else None
        if color is not None:
            color = (color[0], color[1], color[2], 1.0)

        if percentiles:
            ps = np.asarray(percentiles, dtype=float)
            qx = np.percentile(x, ps)
            y0 = base_y - i * label_stagger

            for j, (p, xv) in enumerate(zip(ps, qx)):
                if percentile_style == "vline":
                    ax.axvline(xv, linestyle=":", linewidth=2, color=color)
                elif percentile_style == "rug":
                    ax.plot(
                        [xv, xv],
                        [0.0, 0.06],
                        transform=trans_top,
                        color=color,
                        linewidth=3,
                    )
                else:
                    raise ValueError("percentile_style must be 'vline' or 'rug'")

                y = y0 - j * (label_stagger * 0.35)
                txt = f"{label} {p:g}p = {value_fmt.format(xv)}"
                ax.text(
                    xv,
                    y,
                    txt,
                    transform=trans_top,
                    ha="right",
                    va="top",
                    rotation=label_rotate,
                    fontsize=9,
                    color=color,
                    bbox={
                        "boxstyle": "round,pad=0.2",
                        "facecolor": "white",
                        "edgecolor": "black",
                        "linewidth": 0.6,
                        "alpha": 0.9,
                    },
                    clip_on=False,
                )

    if logx:
        ax.set_xscale("log")
    if logy:
        ax.set_yscale("log")

    ax.set_xlabel(col.replace("_", " "))
    ax.set_ylabel("Count")
    ax.set_title(title or f"Histogram of {col}")
    ax.legend()
    plt.tight_layout()
    plt.show()


def _fit_expected_counts_lognormal(
    x: np.ndarray, edges: np.ndarray
) -> tuple[np.ndarray, str, dict[str, float]]:
    sigma, loc, scale = lognorm.fit(x, floc=0)
    mu = np.log(scale)
    cdf = lognorm.cdf(edges, s=sigma, loc=loc, scale=scale)
    expected = x.size * np.diff(cdf)
    label = f"Lognormal (μ={mu:.2f}, σ={sigma:.2f})"
    params = {"mu": float(mu), "sigma": float(sigma), "n": int(x.size)}
    return expected, label, params


def _fit_expected_counts_weibull(
    x: np.ndarray, edges: np.ndarray
) -> tuple[np.ndarray, str, dict[str, float]]:
    k, loc, scale = weibull_min.fit(x, floc=0)
    cdf = weibull_min.cdf(edges, k, loc=loc, scale=scale)
    expected = x.size * np.diff(cdf)
    label = f"Weibull (k={k:.2f})"
    params = {"k": float(k), "scale": float(scale), "n": int(x.size)}
    return expected, label, params


def _fit_expected_counts_loglogistic(
    x: np.ndarray, edges: np.ndarray
) -> tuple[np.ndarray, str, dict[str, float]]:
    c, loc, scale = fisk.fit(x, floc=0)
    cdf = fisk.cdf(edges, c, loc=loc, scale=scale)
    expected = x.size * np.diff(cdf)
    label = f"Log-logistic (c={c:.2f})"
    params = {"c": float(c), "scale": float(scale), "n": int(x.size)}
    return expected, label, params


def _plot_fit_counts_logbins(
    df: pl.DataFrame,
    *,
    col: str = "duration_days",
    bins: int = 100,
    title: str | None = None,
    dist: str,
) -> dict[str, float] | None:
    x = get_x(df, col)
    if x.size < 2:
        print("Not enough data.")
        return None

    lo = max(x.min(), np.nextafter(0, 1))
    hi = x.max()
    edges = np.logspace(np.log10(lo), np.log10(hi), bins + 1)
    centers = np.sqrt(edges[:-1] * edges[1:])

    if dist == "lognormal":
        expected, label, params = _fit_expected_counts_lognormal(x, edges)
        default_title = "Lognormal fit (counts)"
    elif dist == "weibull":
        expected, label, params = _fit_expected_counts_weibull(x, edges)
        default_title = "Weibull fit (counts)"
    elif dist == "loglogistic":
        expected, label, params = _fit_expected_counts_loglogistic(x, edges)
        default_title = "Log-logistic fit (counts)"
    else:
        raise ValueError(f"Unsupported dist: {dist}")

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.hist(x, bins=edges, density=False, alpha=0.6)
    ax.plot(centers, expected, linewidth=3, label=label)

    ax.set_xscale("log")
    ax.set_xlabel(col.replace("_", " "))
    ax.set_ylabel("Count per log bin")
    ax.set_title(title or default_title)
    ax.legend()
    plt.tight_layout()
    plt.show()
    return params


def plot_lognormal_fit_counts_logbins(
    df: pl.DataFrame,
    *,
    col: str = "duration_days",
    bins: int = 100,
    title: str | None = None,
) -> dict[str, float] | None:
    return _plot_fit_counts_logbins(
        df, col=col, bins=bins, title=title, dist="lognormal"
    )


def plot_weibull_fit_counts_logbins(
    df: pl.DataFrame,
    *,
    col: str = "duration_days",
    bins: int = 100,
    title: str | None = None,
) -> dict[str, float] | None:
    return _plot_fit_counts_logbins(df, col=col, bins=bins, title=title, dist="weibull")


def plot_loglogistic_fit_counts_logbins(
    df: pl.DataFrame,
    *,
    col: str = "duration_days",
    bins: int = 100,
    title: str | None = None,
) -> dict[str, float] | None:
    return _plot_fit_counts_logbins(
        df, col=col, bins=bins, title=title, dist="loglogistic"
    )


def plot_hist_and_lognormal_fit_overlays(
    dfs: list[pl.DataFrame],
    labels: list[str],
    *,
    col: str = "duration_days",
    bins: int = 100,
    title: str | None = None,
    show_params_in_legend: bool = False,
    hist_linewidth: float = 1.2,
    fit_linewidth: float = 3.0,
) -> None:
    if len(dfs) != len(labels):
        raise ValueError("dfs and labels must have the same length.")

    xs = [get_x(df, col) for df in dfs]
    allx = np.concatenate([x for x in xs if x.size])
    if allx.size < 2:
        raise ValueError("Not enough data across datasets to plot.")

    lo = max(allx.min(), np.nextafter(0, 1))
    hi = allx.max()
    edges = np.logspace(np.log10(lo), np.log10(hi), bins + 1)
    centers = np.sqrt(edges[:-1] * edges[1:])

    fig, ax = plt.subplots(figsize=(10, 4))
    for x, label in zip(xs, labels):
        if x.size < 2:
            continue

        hist_counts, _ = np.histogram(x, bins=edges)
        y_step = np.r_[hist_counts, hist_counts[-1]]
        (hist_line,) = ax.step(
            edges, y_step, where="post", linewidth=hist_linewidth, alpha=0.9
        )
        color = hist_line.get_color()

        sigma, loc, scale = lognorm.fit(x, floc=0)
        mu = np.log(scale)
        cdf = lognorm.cdf(edges, s=sigma, loc=loc, scale=scale)
        expected = x.size * np.diff(cdf)

        fit_label = label
        if show_params_in_legend:
            fit_label = f"{label} (μ={mu:.2f}, σ={sigma:.2f}, n={x.size})"

        ax.plot(
            centers, expected, color=color, linewidth=fit_linewidth, label=fit_label
        )

    ax.set_xscale("log")
    ax.set_xlabel(col.replace("_", " "))
    ax.set_ylabel("Count per log bin")
    ax.set_title(title or "Overlaid histograms + lognormal fits")
    ax.legend()
    plt.tight_layout()
    plt.show()

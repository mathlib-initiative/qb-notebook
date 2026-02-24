import matplotlib
import polars as pl

from qb_notebook.plotting import get_x, plot_lognormal_fit_counts_logbins

matplotlib.use("Agg")


def test_get_x_filters_nonpositive_and_nonfinite() -> None:
    df = pl.DataFrame(
        {"duration_days": [1.0, 2.0, 0.0, -3.0, float("nan"), float("inf")]}
    )
    x = get_x(df, "duration_days")
    assert x.tolist() == [1.0, 2.0]


def test_plot_lognormal_fit_counts_logbins_returns_params() -> None:
    df = pl.DataFrame({"duration_days": [1.0, 2.0, 3.0, 5.0, 8.0, 13.0]})
    params = plot_lognormal_fit_counts_logbins(df, bins=10)
    assert params is not None
    assert "mu" in params
    assert "sigma" in params
    assert params["n"] == 6

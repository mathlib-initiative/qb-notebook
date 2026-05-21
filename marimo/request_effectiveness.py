"""Request / assignment effectiveness — review requests, assignments, policy outcomes."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Request / assignment effectiveness

    Do **review requests** and **assignments** actually accelerate
    review, and do **assignees** end up driving their PRs through to
    `maintainer-merge` the way the policy says they should?

    - **§1** — what fraction of `REVIEW_REQUESTED` events get a
      same-reviewer response, and how long does the response take?
      **§1b** repeats the analysis for `ASSIGNED` events, broken down
      by what action the assignee took (review / comment /
      self-unassign).
    - **§2** — *latency lift*: does requesting a reviewer measurably
      accelerate time-to-first-review? Within-PR (before/after the
      request) for the headline, cross-PR matched on size × area ×
      author cohort as a robustness check. **§2b** repeats the
      within-PR lift around the first `ASSIGNED` event, split bot vs.
      manual.
    - **§3** — *assignment-policy outcome*: of PRs with at least one
      `ASSIGNED` event, did **any** assignee end up being the
      inferred trigger of `maintainer-merge`? Stratified by automatic
      (bot-driven) vs. manual assignment.
    - **§4** — *churn*: distributions of `REVIEW_REQUEST_REMOVED` and
      re-requests; `UNASSIGNED` breakdown by kind (self / bot / other
      human); correlation with downstream time-to-merge.
    - **§5** — who uses manual assignment and review requests most.
    - **§6** — descriptive histograms of time-to-merge and
      queue-window durations, grouped by review-request status and
      assignment kind. **Selection bias warning**: PRs that attract
      manual assignments are often already stuck, so naive overlays
      can make assignment *look* harmful. Read against §2's matched /
      within-PR views.

    **Data caveats**: ~65 % of `ASSIGNED` events come from the
    `leanprover-community-bot-assistant` + `mathlib-triage` automation;
    `REVIEW_REQUESTED` is 100 % manual. The recent `mathlib-triage`
    inactivity-unassign automation produces the `bot` bucket on
    `UNASSIGNED`. See `qb_notebook.assignments.ASSIGNMENT_BOT_ACTORS`.
    """)
    return


@app.cell
def _():
    import sys
    from pathlib import Path

    _repo_root = Path(__file__).resolve().parents[1]
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))

    from datetime import datetime, timezone

    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl

    from qb_notebook.assignments import (
        ASSIGNMENT_BOT_ACTORS,
        assignment_policy_outcome,
        assignment_responses,
        classify_assignment_events,
        classify_unassign_events,
        review_request_responses,
    )
    from qb_notebook.data_io import load_pr_interval_data
    from qb_notebook.filters import (
        expr_merged_at_effective,
        expr_merged_to_master,
    )
    from qb_notebook.pr_shape import (
        DEFAULT_LINES_BREAKS,
        author_cohort,
        bucket_labels,
        pr_type,
        pr_type_order,
        size_buckets,
    )
    from qb_notebook.review_states import (
        MAINTAINER_MERGE_LABEL,
        MATHLIB_LABEL_RETIRED_AT,
        attribute_label_events,
        label_intervals,
    )

    return (
        ASSIGNMENT_BOT_ACTORS,
        DEFAULT_LINES_BREAKS,
        MAINTAINER_MERGE_LABEL,
        MATHLIB_LABEL_RETIRED_AT,
        Path,
        assignment_policy_outcome,
        assignment_responses,
        attribute_label_events,
        author_cohort,
        bucket_labels,
        classify_assignment_events,
        classify_unassign_events,
        datetime,
        expr_merged_at_effective,
        expr_merged_to_master,
        label_intervals,
        load_pr_interval_data,
        np,
        pl,
        plt,
        pr_type,
        pr_type_order,
        review_request_responses,
        size_buckets,
        timezone,
    )


@app.cell
def _(Path, datetime, load_pr_interval_data, pl, timezone):
    _data_dir = Path(__file__).resolve().parents[1] / "data"
    data = load_pr_interval_data(_data_dir)
    events = data["events"]
    prs_raw = data["prs"]
    queue_windows = data["queue_windows"]
    _users = pl.read_parquet(_data_dir / "core_user.parquet")
    users = _users.select(
        pl.col("id").alias("author_id"),
        pl.col("github_login").alias("author_login"),
    )
    asof = datetime.now(tz=timezone.utc)
    return asof, events, prs_raw, queue_windows, users


@app.cell
def _(
    DEFAULT_LINES_BREAKS,
    asof,
    author_cohort,
    bucket_labels,
    expr_merged_at_effective,
    expr_merged_to_master,
    pl,
    pr_type,
    prs_raw,
    size_buckets,
    users,
):
    """Filter to human PRs against `master`, attach shape + cohort columns,
    and derive `merged_at_effective` (null for non-merges)."""
    _MATHLIB_REPO_ID = 1
    prs_enriched = (
        prs_raw.filter(pl.col("repository_id") == _MATHLIB_REPO_ID)
        .filter(pl.col("base_ref_name") == "master")
        .join(users, on="author_id", how="left")
        .with_columns(
            pl.when(expr_merged_to_master())
            .then(expr_merged_at_effective())
            .otherwise(None)
            .alias("merged_at_effective"),
        )
    )
    prs_enriched = size_buckets(prs_enriched)
    prs_enriched = pr_type(prs_enriched)
    prs_enriched = author_cohort(prs_enriched)
    prs_enriched = prs_enriched.with_columns(
        pl.col("merged_at_effective").is_not_null().alias("is_merged"),
        pl.col("closed_at").is_not_null().alias("is_closed"),
    )
    lines_bucket_order = bucket_labels(DEFAULT_LINES_BREAKS)
    _ = asof
    return lines_bucket_order, prs_enriched


@app.cell
def _(
    MATHLIB_LABEL_RETIRED_AT,
    asof,
    events,
    label_intervals,
    pl,
    prs_enriched,
):
    """`t-*` label intervals for the topic filter (same convention as
    `anatomy_of_a_merge.py` — open intervals run to `asof`)."""
    _t_labels = (
        events.filter(pl.col("type") == "LABELED")
        .filter(pl.col("label_name").str.starts_with("t-"))
        .select("label_name")
        .unique()
        .get_column("label_name")
        .to_list()
    )
    t_intervals = label_intervals(
        events,
        _t_labels,
        asof=asof,
        label_asof_overrides=MATHLIB_LABEL_RETIRED_AT,
    )
    _ = prs_enriched
    return (t_intervals,)


@app.cell(hide_code=True)
def _(mo):
    """Cohort dropdown — same options as `anatomy_of_a_merge.py`.
    Default is post-MM since the policy-outcome analysis hinges on the
    `maintainer-merge` label, which only entered widespread use 2024-02."""
    cohort = mo.ui.dropdown(
        options={
            "post-MM (2024-02-15+)": "post_mm",
            "pre-MM (pre-2024-02-15)": "pre_mm",
            "pre-bors (pre-2022-08-01)": "pre_bors",
            "bors → pre-MM (2022-08-01 to 2024-02-15)": "bors_pre_mm",
            "2024 (post-MM)": "2024",
            "2025": "2025",
            "all-time": "all",
        },
        value="post-MM (2024-02-15+)",
        label="Cohort",
    )
    mo.hstack([cohort])
    return (cohort,)


@app.cell(hide_code=True)
def _(pr_type_order, t_intervals):
    """Filter-option menus. `TOPIC_NONE` keeps unlabeled PRs in the cohort."""
    TOPIC_NONE = "(none)"
    available_topics = sorted(
        t_intervals.get_column("label_name").unique().to_list()
    ) + [TOPIC_NONE]
    available_pr_types = pr_type_order()
    return TOPIC_NONE, available_pr_types, available_topics


@app.cell(hide_code=True)
def _(mo):
    """All/None buttons for the topic filter."""
    topic_all_btn = mo.ui.button(label="All topics", value=0, on_click=lambda v: v + 1)
    topic_none_btn = mo.ui.button(label="None", value=0, on_click=lambda v: v + 1)
    return topic_all_btn, topic_none_btn


@app.cell(hide_code=True)
def _(available_topics, mo, topic_all_btn, topic_none_btn):
    _default = topic_all_btn.value >= topic_none_btn.value
    topic_checks = mo.ui.array(
        [mo.ui.checkbox(value=_default, label=lbl) for lbl in available_topics]
    )
    return (topic_checks,)


@app.cell(hide_code=True)
def _(available_topics, mo, topic_all_btn, topic_checks, topic_none_btn):
    _n_selected = sum(1 for v in topic_checks.value if v)
    mo.accordion(
        {
            f"Topic filter ({_n_selected}/{len(available_topics)} selected)": mo.vstack(
                [
                    mo.hstack([topic_all_btn, topic_none_btn], justify="start"),
                    mo.hstack(list(topic_checks), wrap=True, justify="start"),
                ]
            )
        }
    )
    return


@app.cell(hide_code=True)
def _(mo):
    pr_type_all_btn = mo.ui.button(label="All types", value=0, on_click=lambda v: v + 1)
    pr_type_none_btn = mo.ui.button(label="None", value=0, on_click=lambda v: v + 1)
    return pr_type_all_btn, pr_type_none_btn


@app.cell(hide_code=True)
def _(available_pr_types, mo, pr_type_all_btn, pr_type_none_btn):
    _default = pr_type_all_btn.value >= pr_type_none_btn.value
    pr_type_checks = mo.ui.array(
        [mo.ui.checkbox(value=_default, label=t) for t in available_pr_types]
    )
    return (pr_type_checks,)


@app.cell(hide_code=True)
def _(
    available_pr_types,
    mo,
    pr_type_all_btn,
    pr_type_checks,
    pr_type_none_btn,
):
    _n_selected = sum(1 for v in pr_type_checks.value if v)
    mo.accordion(
        {
            f"PR-type filter ({_n_selected}/{len(available_pr_types)} selected)": mo.vstack(
                [
                    mo.hstack([pr_type_all_btn, pr_type_none_btn], justify="start"),
                    mo.hstack(list(pr_type_checks), wrap=True, justify="start"),
                ]
            )
        }
    )
    return


@app.cell(hide_code=True)
def _(
    TOPIC_NONE,
    available_pr_types,
    available_topics,
    cohort,
    datetime,
    pl,
    pr_type_checks,
    prs_enriched,
    t_intervals,
    timezone,
    topic_checks,
):
    """Apply cohort + topic + PR-type filters to `prs_enriched`."""
    _COHORT_BOUNDS = {
        "post_mm": (datetime(2024, 2, 15, tzinfo=timezone.utc), None),
        "pre_mm": (None, datetime(2024, 2, 15, tzinfo=timezone.utc)),
        "pre_bors": (None, datetime(2022, 8, 1, tzinfo=timezone.utc)),
        "bors_pre_mm": (
            datetime(2022, 8, 1, tzinfo=timezone.utc),
            datetime(2024, 2, 15, tzinfo=timezone.utc),
        ),
        "2024": (
            datetime(2024, 2, 15, tzinfo=timezone.utc),
            datetime(2025, 1, 1, tzinfo=timezone.utc),
        ),
        "2025": (
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        "all": (None, None),
    }
    _lo, _hi = _COHORT_BOUNDS[cohort.value]
    _expr = pl.lit(True)
    if _lo is not None:
        _expr = _expr & (pl.col("gh_created_at") >= _lo)
    if _hi is not None:
        _expr = _expr & (pl.col("gh_created_at") < _hi)
    prs_cohort = prs_enriched.filter(_expr)

    _selected_topics = [t for t, v in zip(available_topics, topic_checks.value) if v]
    if len(_selected_topics) < len(available_topics):
        _selected_real = [t for t in _selected_topics if t != TOPIC_NONE]
        _none_selected = TOPIC_NONE in _selected_topics
        _real_ids = (
            t_intervals.filter(pl.col("label_name").is_in(_selected_real))
            .get_column("pull_request_id")
            .unique()
        )
        _keep_expr = pl.col("id").is_in(_real_ids)
        if _none_selected:
            _all_t_ids = t_intervals.get_column("pull_request_id").unique()
            _keep_expr = _keep_expr | ~pl.col("id").is_in(_all_t_ids)
        prs_cohort = prs_cohort.filter(_keep_expr)

    _selected_types = [t for t, v in zip(available_pr_types, pr_type_checks.value) if v]
    if len(_selected_types) < len(available_pr_types):
        prs_cohort = prs_cohort.filter(pl.col("pr_type").is_in(_selected_types))

    cohort_label = cohort.value
    return (prs_cohort,)


@app.cell
def _(events, pl, prs_cohort):
    """`events` filtered to the cohort PRs. All downstream cells consume
    `events_cohort` rather than `events` so the filter UI gates everything."""
    _pr_ids = prs_cohort.get_column("id").unique()
    events_cohort = events.filter(pl.col("pull_request_id").is_in(_pr_ids))
    return (events_cohort,)


@app.cell
def _(mo, prs_cohort):
    mo.md(f"""
    **Cohort size**: {prs_cohort.height:,} PRs after filters "
        f"(of {prs_cohort.height:,} candidates).
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## §1 — Review-request response rate and latency

    For each `REVIEW_REQUESTED` event, we look for the earliest
    subsequent `REVIEW_*` event by the requested reviewer on the same
    PR. If we find one, the request was *responded* and we record the
    gap; otherwise it sits unanswered (the reviewer never engaged,
    someone else merged, the PR closed, etc.).

    Team-targeted requests have no individual responder to match
    against, so they're tracked separately.
    """)
    return


@app.cell
def _(events_cohort, pl, review_request_responses):
    """Per-request response frame, with a `team_request` flag for the
    null-reviewer-login rows (team-targeted requests)."""
    rr_responses = review_request_responses(events_cohort).with_columns(
        pl.col("requested_reviewer_login").is_null().alias("team_request"),
    )
    return (rr_responses,)


@app.cell
def _(mo, pl, rr_responses):
    """Headline counts: total requests, team vs individual, response rate."""
    _n_total = rr_responses.height
    _n_team = rr_responses.filter(pl.col("team_request")).height
    _individual = rr_responses.filter(~pl.col("team_request"))
    _n_individual = _individual.height
    _n_responded = _individual.filter(pl.col("responded")).height
    _rate = _n_responded / _n_individual if _n_individual else 0.0

    mo.md(
        f"""
        | metric                              | value   |
        |-------------------------------------|---------|
        | total `REVIEW_REQUESTED` events     | {_n_total:,} |
        | individual (named reviewer)         | {_n_individual:,} |
        | team-targeted (no individual login) | {_n_team:,} |
        | individual requests responded       | {_n_responded:,} ({_rate:.1%}) |
        """
    )
    return


@app.cell
def _(np, pl, plt, rr_responses):
    """Response-latency CDF on log-scaled hours, individual requests only."""
    _resp = rr_responses.filter(
        pl.col("responded") & ~pl.col("team_request")
    ).get_column("response_gap_seconds")

    _fig, _ax = plt.subplots(figsize=(8, 4))
    if _resp.is_empty():
        _ax.text(0.5, 0.5, "No responses in cohort", ha="center", va="center")
        _ax.set_axis_off()
    else:
        _hours = _resp.to_numpy() / 3600.0
        _hours.sort()
        _y = np.arange(1, len(_hours) + 1) / len(_hours)
        _ax.plot(_hours, _y, lw=2)
        _ax.set_xscale("log")
        _ax.set_xlabel("Hours from request → first response (log scale)")
        _ax.set_ylabel("CDF")
        _ax.set_title(
            "Review-request response latency (individual requests, responded)"
        )
        for _p, _label in [(0.5, "p50"), (0.75, "p75"), (0.9, "p90")]:
            _v = np.quantile(_hours, _p)
            _ax.axhline(_p, color="gray", alpha=0.3, lw=0.8)
            _ax.axvline(_v, color="gray", alpha=0.3, lw=0.8)
            _ax.annotate(
                f"{_label}: {_v:.1f}h",
                xy=(_v, _p),
                xytext=(6, -10),
                textcoords="offset points",
                fontsize=8,
            )
        _ax.grid(alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    **Stratified response rate.** Same per-request frame, joined back
    to the per-PR shape columns so we can see how response rate varies
    by PR size, area, and PR type. (Cohorts with fewer than 20
    requests are dropped from the table — too noisy to interpret.)
    """)
    return


@app.cell
def _(lines_bucket_order, mo, pl, prs_cohort, rr_responses):
    """Stratified response rate by `lines_bucket` and `pr_type`."""
    _individual = rr_responses.filter(~pl.col("team_request"))
    _shape = prs_cohort.select("id", "lines_bucket", "pr_type").rename(
        {"id": "pull_request_id"}
    )
    _joined = _individual.join(_shape, on="pull_request_id", how="left")

    def _summary(group_col: str, order: list[str] | None = None):
        out = (
            _joined.group_by(group_col)
            .agg(
                pl.len().alias("requests"),
                pl.col("responded").sum().alias("responded"),
                pl.col("response_gap_seconds")
                .filter(pl.col("responded"))
                .median()
                .alias("median_gap_s"),
            )
            .with_columns(
                (pl.col("responded") / pl.col("requests")).alias("response_rate"),
                (pl.col("median_gap_s") / 3600.0).round(1).alias("median_gap_h"),
            )
            .filter(pl.col("requests") >= 20)
            .drop("median_gap_s")
            .sort(group_col)
        )
        if order:
            out = out.with_columns(pl.col(group_col).cast(pl.Enum(order))).sort(
                group_col
            )
        return out

    _by_lines = _summary("lines_bucket", order=lines_bucket_order)
    _by_type = _summary("pr_type")
    mo.hstack(
        [
            mo.vstack([mo.md("**by `lines_bucket`**"), _by_lines]),
            mo.vstack([mo.md("**by `pr_type`**"), _by_type]),
        ],
        justify="start",
        gap=1,
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## §1b — Assignment response: did the assignee do anything?

    Same machinery as §1, but pivoted on `ASSIGNED` events. For each
    assignment we look for the earliest subsequent event on the same
    PR by the *assignee*; the response type is recorded so we can see
    the breakdown of what assignees actually do first (review,
    comment, self-unassign…).

    `UNASSIGNED` is included as a valid response type because a
    self-unassign is a meaningful "decline". Bot vs. manual
    assignments are reported separately — they're qualitatively
    different (the bot rotates the queue, a maintainer assigns when
    they want a specific human on the PR).
    """)
    return


@app.cell
def _(assignment_responses, events_cohort):
    """Per-assignment response frame. `kind` and `response_event_type`
    come directly out of the helper, no extra joins needed."""
    asg_responses = assignment_responses(events_cohort)
    return (asg_responses,)


@app.cell
def _(asg_responses, mo, pl):
    """Headline response rate, split by assignment kind."""

    def _bucket(label: str, expr: pl.Expr):
        sub = asg_responses.filter(expr)
        n = sub.height
        responded = sub.filter(pl.col("responded")).height
        return {
            "stratum": label,
            "assignments": n,
            "responded": responded,
            "response_rate": round(responded / n, 3) if n else None,
        }

    _table = pl.DataFrame(
        [
            _bucket("all", pl.lit(True)),
            _bucket("bot-assigned", pl.col("kind") == "bot"),
            _bucket("self-assigned", pl.col("kind") == "self"),
            _bucket("other-human assigned", pl.col("kind") == "other_human"),
        ]
    )

    mo.vstack(
        [
            mo.md(
                "**Response rate by assignment kind.** *Response* = first event "
                "on the PR by the assignee whose type is in "
                "`{REVIEW_APPROVED, REVIEW_COMMENTED, REVIEW_CHANGES_REQUESTED, "
                "ISSUE_COMMENTED, UNASSIGNED}` at or after the assignment."
            ),
            _table,
        ]
    )
    return


@app.cell
def _(asg_responses, mo, pl):
    """Breakdown of *what* the response was, split by assignment kind."""
    _responded = asg_responses.filter(pl.col("responded"))
    _by_type = (
        _responded.group_by(["kind", "response_event_type"])
        .agg(pl.len().alias("n"))
        .with_columns(
            (pl.col("n") / pl.col("n").sum().over("kind"))
            .round(3)
            .alias("share_within_kind")
        )
        .sort(["kind", "n"], descending=[False, True])
    )
    mo.vstack(
        [
            mo.md(
                "**First-action breakdown.** For responded assignments only — "
                "what was the assignee's first move? `UNASSIGNED` rows are "
                "self-unassigns ('decline'); `ISSUE_COMMENTED` is a substantive "
                "comment short of a formal review."
            ),
            _by_type,
        ]
    )
    return


@app.cell
def _(asg_responses, np, pl, plt):
    """CDF of response latency, faceted by assignment `kind` and overlaid
    by response event type. Log-scale x-axis matches §1's CDF."""
    _resp = asg_responses.filter(pl.col("responded")).with_columns(
        (pl.col("response_gap_seconds") / 3600.0).alias("gap_h"),
    )

    _kinds = ["bot", "self", "other_human"]
    _types = [
        "REVIEW_APPROVED",
        "REVIEW_COMMENTED",
        "REVIEW_CHANGES_REQUESTED",
        "ISSUE_COMMENTED",
        "UNASSIGNED",
    ]
    _palette = {
        "REVIEW_APPROVED": "#2a9d8f",
        "REVIEW_COMMENTED": "#4c72b0",
        "REVIEW_CHANGES_REQUESTED": "#e76f51",
        "ISSUE_COMMENTED": "#8da0cb",
        "UNASSIGNED": "#c44e52",
    }

    _fig, _axes = plt.subplots(1, 3, figsize=(13, 4), sharex=True, sharey=True)
    for _ax, _kind in zip(_axes, _kinds):
        _sub = _resp.filter(pl.col("kind") == _kind)
        if _sub.is_empty():
            _ax.text(0.5, 0.5, f"No {_kind} assignments", ha="center", va="center")
            _ax.set_axis_off()
            continue
        _all = np.sort(_sub.get_column("gap_h").to_numpy())
        _ax.plot(
            _all,
            np.arange(1, len(_all) + 1) / len(_all),
            color="black",
            lw=2,
            label=f"any  (n={len(_all):,})",
        )
        for _t in _types:
            _vals = _sub.filter(pl.col("response_event_type") == _t).get_column("gap_h")
            if _vals.is_empty():
                continue
            _arr = np.sort(_vals.to_numpy())
            _ax.plot(
                _arr,
                np.arange(1, len(_arr) + 1) / len(_arr),
                color=_palette[_t],
                lw=1.2,
                alpha=0.8,
                label=f"{_t.lower()}  (n={len(_arr):,})",
            )
        _ax.set_xscale("log")
        _ax.set_xlabel("hours from ASSIGNED → first action (log)")
        _ax.set_title(f"kind = {_kind}")
        _ax.grid(alpha=0.3)
        _ax.legend(fontsize=7, loc="lower right")
    _axes[0].set_ylabel("CDF")
    _fig.suptitle("Assignment response latency by kind × first-action type")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## §2 — Does a review request accelerate first review?

    **Within-PR** (top): for each PR that ever got a `REVIEW_REQUESTED`,
    compare the rate of non-author non-bot review-events per day in
    the 7-day window *before* the first request vs. *after* it. A
    ratio > 1 means more review activity post-request. We display the
    distribution of log-ratios across PRs.

    **Cross-PR** (bottom): compare the median time from PR open to
    first review for PRs that got a request vs. PRs that didn't,
    matched on `lines_bucket` × primary `t-*` label × `is_first_pr`
    (author cohort). The matched-median delta is the robustness check.
    """)
    return


@app.cell
def _(events_cohort, np, pl, plt, prs_cohort):
    """Within-PR rate-of-engagement ratio (after/before, 7-day windows)."""
    _WINDOW_DAYS = 7
    _WINDOW_S = _WINDOW_DAYS * 86400
    _RESPONSE_TYPES = (
        "REVIEW_APPROVED",
        "REVIEW_COMMENTED",
        "REVIEW_CHANGES_REQUESTED",
        "ISSUE_COMMENTED",
    )
    _BOTS = (
        "github-actions",
        "leanprover-community-mathlib4-bot",
        "leanprover-community-bot-assistant",
        "mathlib-triage",
        "mathlib4-merge-conflict-bot",
        "mathlib4-dependent-issues-bot",
        "dependabot",
        "leanprover-radar",
    )

    _authors = prs_cohort.select(
        pl.col("id").alias("pull_request_id"),
        "author_login",
    )

    # First REVIEW_REQUESTED per PR, cohort-scoped
    _first_req = (
        events_cohort.filter(pl.col("type") == "REVIEW_REQUESTED")
        .group_by("pull_request_id")
        .agg(pl.col("occurred_at").min().alias("first_request_at"))
    )

    # Engagement events: substantive comments / reviews by non-author non-bot
    _engagement = (
        events_cohort.filter(pl.col("type").is_in(_RESPONSE_TYPES))
        .join(_authors, on="pull_request_id", how="left")
        .filter(
            pl.col("actor_login").is_not_null()
            & (pl.col("actor_login") != pl.col("author_login"))
            & ~pl.col("actor_login").is_in(_BOTS)
        )
        .select("pull_request_id", "occurred_at")
    )

    _joined = _engagement.join(_first_req, on="pull_request_id", how="inner")
    _joined = _joined.with_columns(
        (pl.col("occurred_at") - pl.col("first_request_at"))
        .dt.total_seconds()
        .alias("delta_s")
    )

    _before = (
        _joined.filter((pl.col("delta_s") < 0) & (pl.col("delta_s") >= -_WINDOW_S))
        .group_by("pull_request_id")
        .agg(pl.len().alias("n_before"))
    )
    _after = (
        _joined.filter((pl.col("delta_s") >= 0) & (pl.col("delta_s") < _WINDOW_S))
        .group_by("pull_request_id")
        .agg(pl.len().alias("n_after"))
    )
    _both = (
        _first_req.join(_before, on="pull_request_id", how="left")
        .join(_after, on="pull_request_id", how="left")
        .with_columns(
            pl.col("n_before").fill_null(0),
            pl.col("n_after").fill_null(0),
        )
    )

    # Smoothed log-ratio: add 0.5 to both numerator and denominator so
    # PRs with zero before or zero after still produce a finite value.
    _logratio = _both.with_columns(
        ((pl.col("n_after") + 0.5) / (pl.col("n_before") + 0.5))
        .log()
        .alias("log_ratio")
    ).get_column("log_ratio")

    _fig, _ax = plt.subplots(figsize=(8, 4))
    if _logratio.is_empty():
        _ax.text(0.5, 0.5, "No requests in cohort", ha="center", va="center")
        _ax.set_axis_off()
    else:
        _vals = _logratio.to_numpy()
        _ax.hist(_vals, bins=40, color="steelblue", alpha=0.8)
        _med = float(np.median(_vals))
        _ax.axvline(0, color="black", lw=1, label="no change")
        _ax.axvline(
            _med,
            color="crimson",
            lw=2,
            label=f"median = {_med:+.2f} (×{np.exp(_med):.2f})",
        )
        _ax.set_xlabel(
            "log(after / before) — 7-day engagement rate around first request"
        )
        _ax.set_ylabel("PR count")
        _ax.set_title(
            f"Within-PR engagement lift around first REVIEW_REQUESTED  (n={len(_vals):,})"
        )
        _ax.legend()
        _ax.grid(alpha=0.3)
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(events_cohort, mo, pl, prs_cohort, t_intervals):
    """Cross-PR comparison: time-to-first-review for requested vs not,
    matched on (lines_bucket, primary t-*, is_first_pr)."""
    _BOTS = (
        "github-actions",
        "leanprover-community-mathlib4-bot",
        "leanprover-community-bot-assistant",
        "mathlib-triage",
        "mathlib4-merge-conflict-bot",
        "mathlib4-dependent-issues-bot",
        "dependabot",
        "leanprover-radar",
    )
    _RESPONSE_TYPES = (
        "REVIEW_APPROVED",
        "REVIEW_COMMENTED",
        "REVIEW_CHANGES_REQUESTED",
    )

    # First non-author non-bot review event per PR
    _shape = prs_cohort.select(
        pl.col("id").alias("pull_request_id"),
        "author_login",
        "gh_created_at",
        "lines_bucket",
        "is_first_pr",
    )

    _first_rev = (
        events_cohort.filter(pl.col("type").is_in(_RESPONSE_TYPES))
        .join(_shape, on="pull_request_id", how="inner")
        .filter(
            pl.col("actor_login").is_not_null()
            & (pl.col("actor_login") != pl.col("author_login"))
            & ~pl.col("actor_login").is_in(_BOTS)
        )
        .group_by("pull_request_id")
        .agg(pl.col("occurred_at").min().alias("first_review_at"))
    )

    # Primary t-label per PR — earliest applied. Many PRs have no t-label,
    # which we encode as "(none)" so they form their own matching stratum.
    _primary_topic = (
        t_intervals.sort(["pull_request_id", "start"])
        .group_by("pull_request_id", maintain_order=True)
        .agg(pl.col("label_name").first().alias("primary_topic"))
    )

    _per_pr = (
        _shape.join(_first_rev, on="pull_request_id", how="left")
        .join(_primary_topic, on="pull_request_id", how="left")
        .with_columns(
            pl.col("primary_topic").fill_null("(none)"),
            (pl.col("first_review_at") - pl.col("gh_created_at"))
            .dt.total_seconds()
            .alias("ttfr_s"),
        )
    )

    # PRs that got at least one REVIEW_REQUESTED in the cohort
    _requested_ids = (
        events_cohort.filter(pl.col("type") == "REVIEW_REQUESTED")
        .get_column("pull_request_id")
        .unique()
    )
    _per_pr = _per_pr.with_columns(
        pl.col("pull_request_id").is_in(_requested_ids).alias("had_request")
    )

    # Matched-median over (lines_bucket, primary_topic, is_first_pr)
    _matched = (
        _per_pr.drop_nulls("ttfr_s")
        .group_by(["lines_bucket", "primary_topic", "is_first_pr", "had_request"])
        .agg(
            pl.col("ttfr_s").median().alias("median_ttfr_s"),
            pl.len().alias("n_prs"),
        )
        .pivot(
            on="had_request",
            index=["lines_bucket", "primary_topic", "is_first_pr"],
            values=["median_ttfr_s", "n_prs"],
        )
    )

    # Polars pivot generates columns like `median_ttfr_s_true`/`median_ttfr_s_false`.
    # Coalesce both naming conventions defensively (different polars versions
    # produce slightly different column suffixes).
    _cols = _matched.columns
    _req_col = next(
        (c for c in _cols if c.startswith("median_ttfr_s") and "true" in c.lower()),
        None,
    )
    _no_req_col = next(
        (c for c in _cols if c.startswith("median_ttfr_s") and "false" in c.lower()),
        None,
    )
    _n_req_col = next(
        (c for c in _cols if c.startswith("n_prs") and "true" in c.lower()), None
    )
    _n_no_req_col = next(
        (c for c in _cols if c.startswith("n_prs") and "false" in c.lower()), None
    )

    if _req_col and _no_req_col:
        _matched = (
            _matched.rename(
                {
                    _req_col: "median_ttfr_requested_s",
                    _no_req_col: "median_ttfr_no_request_s",
                    _n_req_col: "n_requested",
                    _n_no_req_col: "n_no_request",
                }
            )
            .with_columns(
                (pl.col("median_ttfr_requested_s") / 3600.0)
                .round(1)
                .alias("median_h_requested"),
                (pl.col("median_ttfr_no_request_s") / 3600.0)
                .round(1)
                .alias("median_h_no_request"),
                (pl.col("median_ttfr_requested_s") / pl.col("median_ttfr_no_request_s"))
                .round(2)
                .alias("lift_ratio"),
            )
            .filter((pl.col("n_requested") >= 5) & (pl.col("n_no_request") >= 5))
            .select(
                "lines_bucket",
                "primary_topic",
                "is_first_pr",
                "n_requested",
                "n_no_request",
                "median_h_requested",
                "median_h_no_request",
                "lift_ratio",
            )
            .sort("lift_ratio")
        )
    else:
        _matched = pl.DataFrame()

    # Headline cross-PR comparison (un-matched) for context
    _headline = (
        _per_pr.drop_nulls("ttfr_s")
        .group_by("had_request")
        .agg(
            pl.col("ttfr_s").median().alias("median_ttfr_s"),
            pl.len().alias("n_prs"),
        )
        .with_columns(
            (pl.col("median_ttfr_s") / 3600.0).round(1).alias("median_ttfr_h")
        )
        .drop("median_ttfr_s")
        .sort("had_request")
    )

    mo.vstack(
        [
            mo.md(
                "**Un-matched cross-PR baseline** (TTFR = open → first non-author non-bot review event):"
            ),
            _headline,
            mo.md(
                "**Matched (`lines_bucket` × `primary_topic` × `is_first_pr`)**. "
                "`lift_ratio = median_h_requested / median_h_no_request` — values < 1 "
                "mean the requested cohort got faster first review."
            ),
            _matched,
        ]
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## §2b — Engagement lift around the first assignment

    Same within-PR before/after log-ratio plot as §2's top panel, but
    pivoted on each PR's first `ASSIGNED` event. Split by `kind` of
    that first assignment:

    - **bot**: queue-rotation by `leanprover-community-bot-assistant`
      / `mathlib-triage`. Heavy left tail expected because the bot
      typically assigns when the PR is *already* on the queue
      awaiting reviewer attention — there's often nothing to
      "trigger" past the existing state.
    - **manual** (`self` + `other_human` collapsed): a human chose
      this assignee. More likely to coincide with a genuine attention
      shift, so we'd expect a higher median lift if assignments
      "work".

    Engagement = same definition as §2 (non-author, non-bot
    `REVIEW_*` / `ISSUE_COMMENTED` events). 7-day windows on either
    side; values smoothed with +0.5 in both numerator and denominator.
    """)
    return


@app.cell
def _(ASSIGNMENT_BOT_ACTORS, events_cohort, np, pl, plt, prs_cohort):
    """Within-PR engagement lift, pivoted on first ASSIGNED, split bot vs.
    manual. Mirrors the §2 cell at line ~570."""
    _WINDOW_DAYS = 7
    _WINDOW_S = _WINDOW_DAYS * 86400
    _RESPONSE_TYPES = (
        "REVIEW_APPROVED",
        "REVIEW_COMMENTED",
        "REVIEW_CHANGES_REQUESTED",
        "ISSUE_COMMENTED",
    )
    _BOTS = (
        "github-actions",
        "leanprover-community-mathlib4-bot",
        "leanprover-community-bot-assistant",
        "mathlib-triage",
        "mathlib4-merge-conflict-bot",
        "mathlib4-dependent-issues-bot",
        "dependabot",
        "leanprover-radar",
    )

    _authors = prs_cohort.select(
        pl.col("id").alias("pull_request_id"),
        "author_login",
    )

    # First ASSIGNED per PR + the kind of that first assignment.
    _asg = events_cohort.filter(pl.col("type") == "ASSIGNED").with_columns(
        pl.when(pl.col("actor_login").is_in(list(ASSIGNMENT_BOT_ACTORS)))
        .then(pl.lit("bot"))
        .otherwise(pl.lit("manual"))
        .alias("first_kind"),
    )
    _first_asg = (
        _asg.sort(["pull_request_id", "occurred_at"])
        .group_by("pull_request_id", maintain_order=True)
        .agg(
            pl.col("occurred_at").first().alias("first_assigned_at"),
            pl.col("first_kind").first().alias("first_kind"),
        )
    )

    _engagement = (
        events_cohort.filter(pl.col("type").is_in(_RESPONSE_TYPES))
        .join(_authors, on="pull_request_id", how="left")
        .filter(
            pl.col("actor_login").is_not_null()
            & (pl.col("actor_login") != pl.col("author_login"))
            & ~pl.col("actor_login").is_in(_BOTS)
        )
        .select("pull_request_id", "occurred_at")
    )

    _joined = _engagement.join(_first_asg, on="pull_request_id", how="inner")
    _joined = _joined.with_columns(
        (pl.col("occurred_at") - pl.col("first_assigned_at"))
        .dt.total_seconds()
        .alias("delta_s")
    )

    _before = (
        _joined.filter((pl.col("delta_s") < 0) & (pl.col("delta_s") >= -_WINDOW_S))
        .group_by("pull_request_id")
        .agg(pl.len().alias("n_before"))
    )
    _after = (
        _joined.filter((pl.col("delta_s") >= 0) & (pl.col("delta_s") < _WINDOW_S))
        .group_by("pull_request_id")
        .agg(pl.len().alias("n_after"))
    )
    _both = (
        _first_asg.join(_before, on="pull_request_id", how="left")
        .join(_after, on="pull_request_id", how="left")
        .with_columns(
            pl.col("n_before").fill_null(0),
            pl.col("n_after").fill_null(0),
        )
        .with_columns(
            ((pl.col("n_after") + 0.5) / (pl.col("n_before") + 0.5))
            .log()
            .alias("log_ratio")
        )
    )

    _fig, _axes = plt.subplots(1, 2, figsize=(11, 4), sharex=True, sharey=True)
    for _ax, _kind, _color in [
        (_axes[0], "bot", "#7570b3"),
        (_axes[1], "manual", "#d95f02"),
    ]:
        _vals = _both.filter(pl.col("first_kind") == _kind).get_column("log_ratio")
        if _vals.is_empty():
            _ax.text(0.5, 0.5, f"No {_kind} assignments", ha="center", va="center")
            _ax.set_axis_off()
            continue
        _arr = _vals.to_numpy()
        _ax.hist(_arr, bins=40, color=_color, alpha=0.8)
        _med = float(np.median(_arr))
        _ax.axvline(0, color="black", lw=1, label="no change")
        _ax.axvline(
            _med,
            color="crimson",
            lw=2,
            label=f"median = {_med:+.2f} (×{np.exp(_med):.2f})",
        )
        _ax.set_xlabel("log(after / before) — 7-day engagement")
        _ax.set_ylabel("PR count")
        _ax.set_title(f"first ASSIGNED = {_kind}  (n={len(_arr):,})")
        _ax.legend(fontsize=8)
        _ax.grid(alpha=0.3)
    _fig.suptitle("Within-PR engagement lift around first ASSIGNED")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo):
    mo.md("""
    ## §3 — Assignment-policy outcome

    The policy: an **assignee** is supposed to see their PR through to
    `maintainer-merge` (or closure). We use
    `attribute_label_events` to infer the human who triggered the
    first `maintainer-merge` label per PR, then check whether **any**
    historical assignee of that PR matches the trigger.

    Stratified by automatic vs. manual assignment — a PR is *manually
    assigned* if any non-bot actor ever called `ASSIGNED` on it. PRs
    can be both: bot-assigned first, then a maintainer adds a manual
    assignee later. The strata aren't mutually exclusive, so we report
    each as a separate row.
    """)
    return


@app.cell
def _(
    MAINTAINER_MERGE_LABEL,
    assignment_policy_outcome,
    attribute_label_events,
    classify_assignment_events,
    events_cohort,
    mo,
    pl,
):
    """Per-PR policy frame + headline + manual-vs-bot stratification."""
    _mm = attribute_label_events(events_cohort, MAINTAINER_MERGE_LABEL)
    policy = assignment_policy_outcome(events_cohort, _mm)

    _asg = classify_assignment_events(events_cohort)
    _pr_kinds = _asg.group_by("pull_request_id").agg(
        (pl.col("kind") == "bot").any().alias("had_bot_assign"),
        ((pl.col("kind") == "self") | (pl.col("kind") == "other_human"))
        .any()
        .alias("had_manual_assign"),
        (pl.col("kind") == "other_human").any().alias("had_other_human_assign"),
    )

    _enriched = policy.join(_pr_kinds, on="pull_request_id", how="left")

    _headline_n = _enriched.height
    _headline_pass = _enriched.filter(pl.col("assignee_triggered_mm")).height
    _headline_rate = _headline_pass / _headline_n if _headline_n else 0.0

    def _bucket(label: str, expr: pl.Expr):
        sub = _enriched.filter(expr)
        n = sub.height
        passed = sub.filter(pl.col("assignee_triggered_mm")).height
        return {
            "stratum": label,
            "n_prs": n,
            "passed": passed,
            "policy_rate": round(passed / n, 3) if n else None,
        }

    _strat = pl.DataFrame(
        [
            _bucket("all", pl.lit(True)),
            _bucket("any bot assignment", pl.col("had_bot_assign")),
            _bucket("any manual assignment", pl.col("had_manual_assign")),
            _bucket("manual by other human", pl.col("had_other_human_assign")),
            _bucket(
                "only bot (never manual)",
                pl.col("had_bot_assign") & ~pl.col("had_manual_assign"),
            ),
            _bucket(
                "only manual (never bot)",
                ~pl.col("had_bot_assign") & pl.col("had_manual_assign"),
            ),
        ]
    )

    mo.vstack(
        [
            mo.md(
                f"**Headline**: {_headline_pass:,} / {_headline_n:,} "
                f"({_headline_rate:.1%}) of PRs with an ASSIGNED event and an "
                f"attributed MM had at least one ever-assignee end up as the MM trigger."
            ),
            mo.md(
                "**Stratified by assignment kind** (not mutually exclusive — see strata definitions):"
            ),
            _strat,
        ]
    )
    return


@app.cell
def _(
    ASSIGNMENT_BOT_ACTORS,
    classify_assignment_events,
    events_cohort,
    mo,
    pl,
):
    """When a PR has a manual assignment by another human, who tends to
    be doing the assigning (the user explicitly asked)? Top-N table of
    the actor who applied a non-bot, non-self ASSIGNED event."""
    _other = classify_assignment_events(events_cohort).filter(
        pl.col("kind") == "other_human"
    )
    _top = (
        _other.group_by("actor_login")
        .agg(
            pl.len().alias("manual_assigns_made"),
            pl.col("pull_request_id").n_unique().alias("distinct_prs"),
        )
        .sort("manual_assigns_made", descending=True)
        .head(15)
    )
    _ = ASSIGNMENT_BOT_ACTORS  # threaded to keep the actor set visible to readers
    mo.vstack(
        [
            mo.md(
                "**Who assigns whom?** Top-15 actors making `other_human` "
                "(non-bot, non-self) ASSIGNED calls in the cohort."
            ),
            _top,
        ]
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## §4 — Churn

    Three sub-views:

    - **Request removal / re-request churn**: distribution of
      `REVIEW_REQUEST_REMOVED` and re-request counts per PR.
    - **Unassignment kind breakdown**: `self` / `bot` / `other_human`,
      with cohort-relative shares.
    - **Unassignment × downstream TTM**: for *merged* PRs that had at
      least one UNASSIGNED event, median time-to-merge by the kind of
      the *last* UNASSIGNED event. The recent `mathlib-triage`
      inactivity-unassign automation is the main `bot` source here.
    """)
    return


@app.cell
def _(events_cohort, mo, pl):
    """Per-PR request churn: REVIEW_REQUESTED, REVIEW_REQUEST_REMOVED,
    and re-request counts (= n_requested - n_distinct_reviewers)."""
    _reqs = events_cohort.filter(pl.col("type") == "REVIEW_REQUESTED")
    _removed = events_cohort.filter(pl.col("type") == "REVIEW_REQUEST_REMOVED")

    _per_pr = (
        _reqs.group_by("pull_request_id")
        .agg(
            pl.len().alias("n_requests"),
            pl.col("requested_reviewer_login").n_unique().alias("n_distinct_reviewers"),
        )
        .join(
            _removed.group_by("pull_request_id").agg(pl.len().alias("n_removed")),
            on="pull_request_id",
            how="left",
        )
        .with_columns(
            pl.col("n_removed").fill_null(0),
            (pl.col("n_requests") - pl.col("n_distinct_reviewers")).alias(
                "n_re_requests"
            ),
        )
    )

    _summary = pl.DataFrame(
        {
            "metric": [
                "PRs with ≥1 review request",
                "  ... with ≥1 REVIEW_REQUEST_REMOVED",
                "  ... with ≥1 re-request (same reviewer requested twice)",
                "  ... with ≥3 distinct reviewers requested",
            ],
            "n_prs": [
                _per_pr.height,
                _per_pr.filter(pl.col("n_removed") >= 1).height,
                _per_pr.filter(pl.col("n_re_requests") >= 1).height,
                _per_pr.filter(pl.col("n_distinct_reviewers") >= 3).height,
            ],
        }
    )

    _distribution = (
        _per_pr.group_by("n_requests")
        .agg(pl.len().alias("n_prs"))
        .sort("n_requests")
        .head(10)
    )

    mo.hstack(
        [
            mo.vstack([mo.md("**Request churn summary**"), _summary]),
            mo.vstack(
                [
                    mo.md("**Requests-per-PR distribution** (top of long tail)"),
                    _distribution,
                ]
            ),
        ],
        gap=1,
    )
    return


@app.cell
def _(classify_unassign_events, events_cohort, mo, pl, plt):
    """Unassignment kind breakdown — cohort-relative counts."""
    _una = classify_unassign_events(events_cohort)
    _by_kind = (
        _una.group_by("kind")
        .agg(
            pl.len().alias("events"),
            pl.col("pull_request_id").n_unique().alias("distinct_prs"),
        )
        .sort("events", descending=True)
    )

    _fig, _ax = plt.subplots(figsize=(6, 3.5))
    if _by_kind.is_empty():
        _ax.text(0.5, 0.5, "No UNASSIGNED events in cohort", ha="center", va="center")
        _ax.set_axis_off()
    else:
        _kinds = _by_kind.get_column("kind").to_list()
        _counts = _by_kind.get_column("events").to_list()
        _ax.bar(_kinds, _counts, color=["#4c72b0", "#dd8452", "#55a868"])
        for _i, _c in enumerate(_counts):
            _ax.text(_i, _c, f" {_c:,}", ha="center", va="bottom", fontsize=10)
        _ax.set_ylabel("UNASSIGNED events")
        _ax.set_title("UNASSIGNED kind breakdown")
        _ax.grid(axis="y", alpha=0.3)
    _fig.tight_layout()

    mo.vstack(
        [
            mo.md(
                "Counts per kind. `bot` here is the `mathlib-triage` inactivity-unassign "
                "automation — a relatively recent addition."
            ),
            _fig,
            _by_kind,
        ]
    )
    return


@app.cell
def _(classify_unassign_events, events_cohort, mo, pl, prs_cohort):
    """For *merged* PRs that had at least one UNASSIGNED event, what's
    the median time-to-merge by the *kind* of the last UNASSIGNED event?"""
    _una = classify_unassign_events(events_cohort)
    _last_kind = (
        _una.sort(["pull_request_id", "occurred_at"])
        .group_by("pull_request_id", maintain_order=True)
        .agg(pl.col("kind").last().alias("last_unassign_kind"))
    )

    _merged = (
        prs_cohort.filter(pl.col("is_merged"))
        .select(
            pl.col("id").alias("pull_request_id"),
            "gh_created_at",
            "merged_at_effective",
        )
        .with_columns(
            (pl.col("merged_at_effective") - pl.col("gh_created_at"))
            .dt.total_seconds()
            .alias("ttm_s")
        )
    )

    _joined = _merged.join(_last_kind, on="pull_request_id", how="inner")
    _baseline = _merged.join(
        _una.select("pull_request_id").unique(),
        on="pull_request_id",
        how="anti",
    ).with_columns(pl.lit("no_unassign").alias("last_unassign_kind"))

    _both = pl.concat(
        [
            _joined.select("last_unassign_kind", "ttm_s"),
            _baseline.select("last_unassign_kind", "ttm_s"),
        ]
    )

    _table = (
        _both.group_by("last_unassign_kind")
        .agg(
            pl.len().alias("n_prs"),
            pl.col("ttm_s").median().alias("median_ttm_s"),
            pl.col("ttm_s").quantile(0.9).alias("p90_ttm_s"),
        )
        .with_columns(
            (pl.col("median_ttm_s") / 86400.0).round(2).alias("median_ttm_days"),
            (pl.col("p90_ttm_s") / 86400.0).round(2).alias("p90_ttm_days"),
        )
        .drop("median_ttm_s", "p90_ttm_s")
        .sort("last_unassign_kind")
    )

    mo.vstack(
        [
            mo.md(
                "**Merged PRs by last-UNASSIGNED kind.** Compares TTM for PRs "
                "that ended with a self / bot / other-human unassign against "
                "PRs that had no UNASSIGNED events at all (`no_unassign` row). "
                "Big gaps suggest the unassign mechanism correlates with a "
                "different downstream trajectory; this is descriptive, not causal."
            ),
            _table,
        ]
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## §5 — Who uses manual assignment and review requests most?

    Manual assignments are the human override of the automation —
    knowing who reaches for it is useful for understanding how the
    policy is enforced in practice. Review requests have no policy
    attached but their distribution is similarly diagnostic.
    """)
    return


@app.cell
def _(classify_assignment_events, events_cohort, mo, pl):
    """Top requesters and top manual-assigners in the cohort."""
    _top_requesters = (
        events_cohort.filter(pl.col("type") == "REVIEW_REQUESTED")
        .group_by("actor_login")
        .agg(
            pl.len().alias("requests_made"),
            pl.col("pull_request_id").n_unique().alias("distinct_prs"),
            pl.col("requested_reviewer_login").n_unique().alias("distinct_reviewers"),
        )
        .sort("requests_made", descending=True)
        .head(15)
    )

    _manual = classify_assignment_events(events_cohort).filter(pl.col("kind") != "bot")
    _top_manual_assigners = (
        _manual.group_by("actor_login")
        .agg(
            pl.len().alias("manual_assigns"),
            pl.col("pull_request_id").n_unique().alias("distinct_prs"),
            (pl.col("kind") == "self").sum().alias("self_assigns"),
            (pl.col("kind") == "other_human").sum().alias("other_human_assigns"),
        )
        .sort("manual_assigns", descending=True)
        .head(15)
    )

    mo.hstack(
        [
            mo.vstack(
                [
                    mo.md("**Top REVIEW_REQUESTED actors**"),
                    _top_requesters,
                ]
            ),
            mo.vstack(
                [
                    mo.md("**Top manual ASSIGNED actors** (self + other-human)"),
                    _top_manual_assigners,
                ]
            ),
        ],
        gap=1,
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## §6 — TTM and queue-cycle distributions by intervention

    Overlaid log-scale histograms of **time-to-merge** (PR open →
    merge) and **queue-window duration** (one row per queue cycle on
    `rule_set_id=3`), faceted by:

    - **review-request status**: PRs with at least one
      `REVIEW_REQUESTED` event vs. PRs that never received one.
    - **assignment kind** at the PR level: `manual` (any non-bot
      ASSIGNED event), `bot only`, `none`.

    Density-normalized so groups with different N are comparable.

    **Selection-bias caveat (repeat)**: PRs that attract manual
    assignments are typically already stuck — a maintainer reaches
    for the assign button when a PR has been sitting. So the manual
    bucket is *expected* to skew toward longer durations even if the
    assignment itself helps on the margin. The §2 within-PR /
    matched-median panels are the causal-leaning reads; what follows
    is descriptive.
    """)
    return


@app.cell
def _(ASSIGNMENT_BOT_ACTORS, events_cohort, pl, prs_cohort):
    """Per-PR intervention flags: had_request, assign_kind (none / bot
    only / manual). Shared by the two §6 histogram cells."""
    _requested = (
        events_cohort.filter(pl.col("type") == "REVIEW_REQUESTED")
        .get_column("pull_request_id")
        .unique()
    )
    _asg = events_cohort.filter(pl.col("type") == "ASSIGNED")
    _per_pr_asg = _asg.group_by("pull_request_id").agg(
        pl.col("actor_login")
        .is_in(list(ASSIGNMENT_BOT_ACTORS))
        .any()
        .alias("had_bot_assign"),
        (~pl.col("actor_login").is_in(list(ASSIGNMENT_BOT_ACTORS)))
        .any()
        .alias("had_manual_assign"),
    )

    pr_intervention = (
        prs_cohort.select(pl.col("id").alias("pull_request_id"))
        .with_columns(
            pl.col("pull_request_id").is_in(_requested).alias("had_request"),
        )
        .join(_per_pr_asg, on="pull_request_id", how="left")
        .with_columns(
            pl.col("had_bot_assign").fill_null(False),
            pl.col("had_manual_assign").fill_null(False),
        )
        .with_columns(
            pl.when(pl.col("had_manual_assign"))
            .then(pl.lit("manual"))
            .when(pl.col("had_bot_assign"))
            .then(pl.lit("bot only"))
            .otherwise(pl.lit("none"))
            .alias("assign_kind"),
        )
    )
    return (pr_intervention,)


@app.cell
def _(np, pl, plt, pr_intervention, prs_cohort):
    """TTM histograms, log-scale, faceted by review-request status and
    by assignment kind. Density-normalized."""
    _merged = (
        prs_cohort.filter(pl.col("is_merged"))
        .select(
            pl.col("id").alias("pull_request_id"),
            "gh_created_at",
            "merged_at_effective",
        )
        .join(pr_intervention, on="pull_request_id", how="left")
        .with_columns(
            (
                (
                    pl.col("merged_at_effective") - pl.col("gh_created_at")
                ).dt.total_seconds()
                / 86400.0
            ).alias("ttm_days")
        )
        .filter(pl.col("ttm_days") > 0)
    )

    _fig, _axes = plt.subplots(1, 2, figsize=(13, 4.5))
    if _merged.is_empty():
        for _ax in _axes:
            _ax.text(0.5, 0.5, "No merged PRs in cohort", ha="center", va="center")
            _ax.set_axis_off()
    else:
        _vals_all = _merged.get_column("ttm_days").to_numpy()
        _lo = max(_vals_all.min(), 1 / 24.0)
        _hi = _vals_all.max()
        _bins = np.logspace(np.log10(_lo), np.log10(_hi), 40)

        _ax = _axes[0]
        for _label, _expr, _color in [
            ("no request", ~pl.col("had_request"), "#7f7f7f"),
            ("had REVIEW_REQUESTED", pl.col("had_request"), "#1f77b4"),
        ]:
            _vals = _merged.filter(_expr).get_column("ttm_days").to_numpy()
            if len(_vals) == 0:
                continue
            _med = float(np.median(_vals))
            _ax.hist(
                _vals,
                bins=_bins,
                density=True,
                alpha=0.55,
                color=_color,
                label=f"{_label}  (n={len(_vals):,}, med={_med:.1f}d)",
            )
            _ax.axvline(_med, color=_color, lw=1.5, linestyle="--", alpha=0.9)
        _ax.set_xscale("log")
        _ax.set_xlabel("time-to-merge (days, log)")
        _ax.set_ylabel("density")
        _ax.set_title("TTM by review-request status")
        _ax.legend(fontsize=8)
        _ax.grid(alpha=0.3)

        _ax = _axes[1]
        for _label, _color in [
            ("none", "#7f7f7f"),
            ("bot only", "#7570b3"),
            ("manual", "#d95f02"),
        ]:
            _vals = (
                _merged.filter(pl.col("assign_kind") == _label)
                .get_column("ttm_days")
                .to_numpy()
            )
            if len(_vals) == 0:
                continue
            _med = float(np.median(_vals))
            _ax.hist(
                _vals,
                bins=_bins,
                density=True,
                alpha=0.5,
                color=_color,
                label=f"{_label}  (n={len(_vals):,}, med={_med:.1f}d)",
            )
            _ax.axvline(_med, color=_color, lw=1.5, linestyle="--", alpha=0.9)
        _ax.set_xscale("log")
        _ax.set_xlabel("time-to-merge (days, log)")
        _ax.set_title("TTM by assignment kind")
        _ax.legend(fontsize=8)
        _ax.grid(alpha=0.3)
    _fig.suptitle("Merged-PR TTM distributions  (descriptive — heavy selection bias)")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(asof, np, pl, plt, pr_intervention, queue_windows):
    """Queue-window-duration histograms (one row per cycle on ruleset 3),
    same two facets as the TTM panel."""
    from qb_notebook.review_states import queue_window_intervals

    _qw = queue_window_intervals(queue_windows, rule_set_id=3, asof=asof).join(
        pr_intervention, on="pull_request_id", how="inner"
    )
    _qw = _qw.with_columns(
        (pl.col("duration_hours") / 24.0).alias("duration_days")
    ).filter(pl.col("duration_days") > 0)

    _fig, _axes = plt.subplots(1, 2, figsize=(13, 4.5))
    if _qw.is_empty():
        for _ax in _axes:
            _ax.text(0.5, 0.5, "No queue windows in cohort", ha="center", va="center")
            _ax.set_axis_off()
    else:
        _all = _qw.get_column("duration_days").to_numpy()
        _lo = max(_all.min(), 1 / 24.0)
        _hi = _all.max()
        _bins = np.logspace(np.log10(_lo), np.log10(_hi), 40)

        _ax = _axes[0]
        for _label, _expr, _color in [
            ("no request", ~pl.col("had_request"), "#7f7f7f"),
            ("had REVIEW_REQUESTED", pl.col("had_request"), "#1f77b4"),
        ]:
            _vals = _qw.filter(_expr).get_column("duration_days").to_numpy()
            if len(_vals) == 0:
                continue
            _med = float(np.median(_vals))
            _ax.hist(
                _vals,
                bins=_bins,
                density=True,
                alpha=0.55,
                color=_color,
                label=f"{_label}  (n={len(_vals):,}, med={_med:.1f}d)",
            )
            _ax.axvline(_med, color=_color, lw=1.5, linestyle="--", alpha=0.9)
        _ax.set_xscale("log")
        _ax.set_xlabel("queue-window duration (days, log)")
        _ax.set_ylabel("density")
        _ax.set_title("Queue cycle by review-request status")
        _ax.legend(fontsize=8)
        _ax.grid(alpha=0.3)

        _ax = _axes[1]
        for _label, _color in [
            ("none", "#7f7f7f"),
            ("bot only", "#7570b3"),
            ("manual", "#d95f02"),
        ]:
            _vals = (
                _qw.filter(pl.col("assign_kind") == _label)
                .get_column("duration_days")
                .to_numpy()
            )
            if len(_vals) == 0:
                continue
            _med = float(np.median(_vals))
            _ax.hist(
                _vals,
                bins=_bins,
                density=True,
                alpha=0.5,
                color=_color,
                label=f"{_label}  (n={len(_vals):,}, med={_med:.1f}d)",
            )
            _ax.axvline(_med, color=_color, lw=1.5, linestyle="--", alpha=0.9)
        _ax.set_xscale("log")
        _ax.set_xlabel("queue-window duration (days, log)")
        _ax.set_title("Queue cycle by assignment kind")
        _ax.legend(fontsize=8)
        _ax.grid(alpha=0.3)
    _fig.suptitle("Queue-window duration distributions  (one row per cycle, ruleset 3)")
    _fig.tight_layout()
    _fig
    return


@app.cell
def _(mo, pl, pr_intervention, prs_cohort):
    """PR-level summary table: median TTM and median total queue time per
    intervention group, with sample sizes. Numerical companion to the
    histograms above."""
    _merged = prs_cohort.filter(pl.col("is_merged")).select(
        pl.col("id").alias("pull_request_id"),
        "gh_created_at",
        "merged_at_effective",
    )
    _merged = _merged.join(
        pr_intervention, on="pull_request_id", how="left"
    ).with_columns(
        (
            (pl.col("merged_at_effective") - pl.col("gh_created_at")).dt.total_seconds()
            / 86400.0
        ).alias("ttm_days"),
    )

    _by_request = (
        _merged.group_by("had_request")
        .agg(
            pl.len().alias("n_prs"),
            pl.col("ttm_days").median().round(2).alias("median_ttm_d"),
            pl.col("ttm_days").quantile(0.9).round(2).alias("p90_ttm_d"),
        )
        .sort("had_request")
    )
    _by_kind = (
        _merged.group_by("assign_kind")
        .agg(
            pl.len().alias("n_prs"),
            pl.col("ttm_days").median().round(2).alias("median_ttm_d"),
            pl.col("ttm_days").quantile(0.9).round(2).alias("p90_ttm_d"),
        )
        .sort("assign_kind")
    )

    mo.vstack(
        [
            mo.md(
                "**Per-PR TTM summary.** Same groupings as the histograms; "
                "useful for reading off concrete numbers."
            ),
            mo.hstack(
                [
                    mo.vstack([mo.md("**by review-request status**"), _by_request]),
                    mo.vstack([mo.md("**by assignment kind**"), _by_kind]),
                ],
                justify="start",
                gap=1,
            ),
        ]
    )
    return


if __name__ == "__main__":
    app.run()

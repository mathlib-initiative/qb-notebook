# Review Analysis — Notebook Guide

A reader's guide to the marimo notebooks that analyze mathlib4's review
process. All nine are shipped; this file says what each one answers, its
headline findings, and the helpers it's built on.

- **Run a notebook**: `uv run marimo edit marimo/<name>.py` (or `run`
  for read-only). Conventions and gotchas live in
  [`marimo/AGENTS.md`](../../marimo/AGENTS.md).
- **Project background, data caveats, and the cross-cutting helper
  inventory** live in the [plan index](../review-analysis-plan.md).
- **Remaining work** (unbuilt gaps + synthesis stories) lives in
  [`backlog.md`](backlog.md).

Empirical numbers below are headline figures from the analysis at a
point-in-time data snapshot; they move as `data/` refreshes. Open each
notebook for the current cut.

---

## `review_state_machine.py` — review-state sojourn, ping-pong, delegated path

How much of a PR's life is spent in each label-based review state
(`awaiting-review` / `awaiting-author` / `WIP`), how often it bounces
between them, and how the delegated-merge path compares to the standard
`maintainer-merge → bors` route.

- Sojourn in `awaiting-review` grows modestly with size (median 0.37d at
  0–10 lines → 0.89d at 51–200); bigger PRs ping-pong ~4× as often
  (share with multiple cycles 5% → 20%+).
- Delegated path is **~40% faster at the median, ~45% faster at p90**
  than `maintainer-merge → bors`; **76%** of delegated PRs are
  author-self-merged. Delegation is the path for ~40% of post-cutover
  sign-offs, not a legacy niche.
- The `awaiting-review` label was retired 2024-07-10, so label-based
  state covers historical cohorts only — `queue_window_state.py` is the
  modern substitute.

Built on: `label_intervals`, `attribute_label_events`, `DEFAULT_BOT_ACTORS`,
`pr_shape`.

## `queue_window_state.py` — review-state machine via queue windows (full history)

The same state-machine questions, reconstructed from the analyzer's
queue-window ruleset 3 ("in reviewers' court") instead of the retired
`awaiting-review` label — so it covers 2021-05 → present.

- Queue-window sojourn distribution, monthly quantiles, and a stuck-open
  table, as direct analogues of the label-based plots.
- On the 11.9k PRs (2022-11 → 2024-07) that carry both signals, the
  per-PR overlap with `awaiting-review` has **median Jaccard ≈ 0.98** —
  ruleset 3 is a near-faithful successor to the retired label.
- Queue-cycle multi-cycle share rises 18% (0–10 lines) → 59% (201–1000)
  → 44% (1001+) — the same "very large PRs get fast-tracked" inversion
  seen throughout.

Built on: `queue_window_intervals`, `pr_shape`.

## `reviewer_load.py` — who reviews, concentration, first-touch latency

Who does the review work, how concentrated it is, whether the active
pool is growing, how long authors wait for *any* engagement, and how
reviewer subpopulations differ in responsiveness.

- Trigger attribution covers ~99% of `maintainer-merge` and ~98% of
  `ready-to-merge` labels. Active-reviewer trend, Lorenz/Gini
  concentration, and bus factor, split by team tier.
- **First-touch latency**: broad median **9.1h** (p90 ~13.6d);
  REVIEW_*-only median 15.6h. First-time authors get *faster* first
  touch (7.8h vs 9.1h) — the "newcomers wait longer" hypothesis does
  **not** hold here. `feat:` PRs are the slowest type (~22× `perf:`).
- Subpopulation-responsiveness section (§6): per-cohort review-request
  response rate and manual-assignment follow-through by team / volume /
  tenure.

Built on: `attribute_label_events`, `first_review_touch`, `teams`,
`pr_shape`.

## `bottleneck_localization.py` — why approved PRs don't merge immediately

Once a PR is approved (`maintainer-merge`), where does the
approved-to-merge latency tail come from?

- Approved-to-merge is ~0.35d median / 4.3d p90 / 30d p99. A
  `merge-conflict` shifts the median ~14× (0.32d → 4.6d);
  `awaiting-author` ~9×.
- Stall signals × size: merge-conflict prevalence climbs 0.7% → 11.2%
  across size buckets (16× gradient); `awaiting-CI` is flat (~2.4%), so
  it's not size-driven. No single `t-*` area is a runaway; `t-meta` has
  the highest median `mm→merge` (0.72d).
- **Inline-comment review depth** (§9): a clean ~5× monotonic gradient —
  approved PRs with 0 by-others comments merge in 0.18d median vs 0.92d
  for 11+ (p90 2.76d → 13.0d).

Built on: `label_overlap_seconds`, `labels_active_at`,
`inline_comment_stats`, `pr_shape`, queue windows (ruleset 3).

## `area_health.py` — topic-area (`t-*`) throughput and reviewer coverage

Are any topic areas under-reviewed, slower than others, or losing
reviewer coverage?

- Open backlog, throughput heatmap (area × month), and per-area
  median/p90 TTM. ~17% of merged PRs carry no `t-*` label at merge, so
  per-area totals are a lower bound (the untagged share is stable, so
  relative rankings hold).
- Team-annotated reviewer × area bipartite matrix: distinct
  maintainer-team reviewers per area range 0–17 (`t-algebra` highest).
- A 30d-vs-prior-30d sweep flags areas with declining attributed-trigger
  coverage.

Built on: `labels_active_at`, `label_overlap_seconds`,
`reviewers_court_intervals`, `attribute_label_events`, `teams`.

## `pr_shape_effects.py` — size, author cohort, draft / WIP, PR type

Do bigger PRs take disproportionately longer, and do first-time
contributors, drafts, or PR-type prefixes fare differently?

- Median TTM scales 0.4d → 1.5d → 3.6d → 4.4d across size buckets, then
  *drops* to 2.4d at 1001+ (large refactors get fast-tracked).
- First-time authors merge at **57.5%** vs **82%** for returning.
- Started-as-draft PRs merge at 48% (vs 83%), median TTM 6.2d (vs 1.7d).
  WIP-at-open is a comparable but *orthogonal* signal — only ~5% overlap
  with draft.
- PR type matters: `chore:` merges in 0.66d median vs `feat:` 4.32d
  (~6.5×); following the convention predicts merge (`doc:` 93% vs
  `unparsed` 44%).

Built on: `pr_shape` (`size_buckets` / `author_cohort` /
`started_as_draft` / `pr_type` / `had_wip_label_at_open`),
`reviewers_court_intervals`.

## `temporal_patterns.py` — time-of-day, weekday, seasonality, timezone

When do things happen — are there review-desert hours, weekend
penalties, seasonal rhythm, and does author/reviewer timezone alignment
matter?

- First-touch carries a clean **~2× weekend penalty** at the median
  (8.6h weekday vs 16.4h weekend); Fri-evening opens are ~3× slower than
  Mon-morning.
- Seasonality is a **Nov–Dec peak** (Dec ~2.2× the uniform month), not a
  holiday dip; April is the trough (~0.67×), and there's no summer dip.
- **Timezone overlap does not predict first-touch latency** (medians
  flat across all overlap buckets) — review is a long-running async
  queue, so per-PR TZ alignment isn't the lever. Reframes any "timezone
  gap" framing.

Built on: `temporal` (`with_temporal_columns` / `weekday_hour_histogram`
/ `actor_activity_window` / `hour_set_overlap`), `first_review_touch`.

## `anatomy_of_a_merge.py` — end-to-end lifecycle (synthesis story A)

The full PR milestone funnel — open → first touch → maintainer-merge →
ready-to-merge → merged — as a Sankey plus per-stage duration
decomposition.

- Post-MM merged cohort: median TTM **2.28d** (p90 37.61d).
  **open→first-touch owns ~62%** of the summed-median TTM — reviewer
  attention onset, not the bors queue, is the biggest single contributor.
- **71% of merges skip the `maintainer-merge` label** (bors `r+`
  directly); 27% go via the delegated path.
- Funnel: 91% reach first touch, 80% merge, 12% close unmerged.

Built on: `pipeline_stages` (wraps `first_review_touch` +
`stage_timestamps`), `labels_active_at`, `pr_shape`. First plotly usage
in the set (Sankey); log-binned histograms with lognormal-fit overlay.

## `request_effectiveness.py` — review requests & assignment policy

Do `REVIEW_REQUESTED` events and assignments actually accelerate review,
and do assignees drive their PRs through to `maintainer-merge` the way
the policy expects?

- §1/§1b: request and assignment response rate + latency, with the
  assignee's first action broken out (review / comment / self-unassign).
- §2/§2b: latency lift — within-PR before/after a request (headline) and
  a cross-PR cohort matched on size × area × author cohort (robustness).
- §3: assignment-policy outcome — did *any* assignee end up the inferred
  `maintainer-merge` trigger, split automatic vs. manual.
- §4–§6: request/assignment churn, who drives the process, and
  distributions by intervention (with an explicit selection-bias warning
  — stuck PRs attract manual assignment, so naive overlays mislead).

Note: ~65% of `ASSIGNED` events are automation
(`leanprover-community-bot-assistant` + `mathlib-triage`);
`REVIEW_REQUESTED` is 100% manual.

Built on: `qb_notebook.assignments` (`classify_assignment_events` /
`review_request_responses` / `assignment_policy_outcome`),
`attribute_label_events`, `pr_shape`.

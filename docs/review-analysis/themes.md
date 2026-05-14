# Review Analysis — Themes 1-5

The five original themes of the review analysis project, all shipped.
This file is the historical record + design notes; see the
[plan index](../review-analysis-plan.md) for cross-cutting helper
inventory, roadmap, and pointers to follow-up sessions in
[`sessions.md`](sessions.md) and the active backlog in
[`backlog.md`](backlog.md).

## Theme 1 — Review state machine (sojourn times & ping-pong)

**Status: shipped (label-based: `marimo/review_state_machine.py`; queue-window
companion: `marimo/queue_window_state.py`,
`qb_notebook.review_states.queue_window_intervals`).**

**Question**: How much of a PR's life is spent in each review state, and how
often do PRs bounce between states before merging?

**Plots / metrics**:

- Per-PR sojourn time in `awaiting-review`, `awaiting-author`, `WIP` —
  reconstruct intervals from `LABELED` / `UNLABELED` events, then aggregate
  to distributions (overall and by year).
- Ping-pong count: number of `awaiting-review → awaiting-author → awaiting-review`
  cycles per PR; distribution and correlation with total time-to-merge.
- Stage-by-stage cumulative latency for merged PRs:
  open → first `awaiting-review` → first `maintainer-merge` →
  `ready-to-merge` → merge. Stacked area or box plot by stage.
- Median + p75 + p90 sojourn per state, monthly rolling, to see trends.
- "Stuck" PRs: open PRs currently in `awaiting-review` for > p90 of the
  historical distribution.

**Data**: `prs`, `events` (LABELED/UNLABELED only), `label_defs`.

**Output**: new `review_state_machine.ipynb` + helper functions in a new
`qb_notebook/review_states.py` module (label-interval reconstruction is
reusable). Add 2–3 plots to the plot site.

**Open questions**:
- How do we treat draft PRs in state accounting?
- Should we exclude PRs that never received a review-relevant label?
- Post-2024-07 PRs have no `awaiting-review` events at all (label
  retired); the original notebook tracks the label as-is, which is
  fine for historical cohorts but silently empty on recent PRs.
  The queue-window companion (below) is the working substitute for
  current PRs.

**Queue-window companion** (`marimo/queue_window_state.py`): mirrors
the label-based notebook but reconstructs the "in reviewers' court"
state from `analyzer_prqueuewindow` ruleset 3 via
`qb_notebook.review_states.queue_window_intervals` (same column shape
as `label_intervals`, plus `cycle_index`, `window_count`,
`opened_by_event_type`, `closed_by_event_type`). Provides:

- Queue-window sojourn distribution, long-tail summary, monthly
  quantiles, stuck-open table — direct visual analogues of the
  label-based plots, but covering all of 2021-05 → present.
- Ping-pong-equivalent: distribution of `max(cycle_index)` per PR.
- Stage-by-stage cumulative latency uses `first_on_queue_ts` in place
  of first `awaiting-review`.
- **Overlap-cohort cell** (2022-11-01 → 2024-07-10): for the 11.9k
  PRs that saw both signals, compares per-PR `awaiting-review` seconds
  vs. queue-open seconds. Empirical Jaccard distribution is heavily
  right-shifted (median ≈ 0.98; ≈70% of PRs above 0.8), confirming
  ruleset 3 is a near-faithful successor to the retired label. The
  unified-intervals helper proposed in the cross-cutting infrastructure
  list ([plan index](../review-analysis-plan.md#cross-cutting-infrastructure))
  can lean on this with high confidence.

---

## Theme 2 — Reviewer & maintainer load

**Status: shipped (`marimo/reviewer_load.py`, `qb_notebook/teams.py`,
`qb_notebook.review_states.attribute_label_events`).**

**Question**: Who is doing the review work, how concentrated is it, and is
the active-reviewer pool growing or shrinking?

**Attribution model**: `maintainer-merge` and `ready-to-merge` are
bot-applied. We attribute each `LABELED` event back to the human who
triggered it via `attribute_label_events`: the most recent non-bot
`ISSUE_COMMENTED` / `REVIEW_*` event on the same PR within a configurable
window (default 10 min). Coverage is ~99 % for `maintainer-merge` and
~98 % for `ready-to-merge`; the unattributed remainder shows up in a
coverage cell up front.

**Plots / metrics**:

- Attribution coverage cell + trigger→label gap histogram.
- Top-N reviewers + long-tail histogram (per-reviewer counts table
  annotated with team membership).
- **Active reviewer count per week**: distinct attributed reviewers in
  a trailing rolling window. Trend over project history.
- **Concentration**: Lorenz curve + Gini coefficient per year on
  attributed triggers.
- **Bus factor**: smallest N reviewers covering 50 % / 80 % per year.
- **Bors-trigger attribution**: same heuristic applied to
  `ready-to-merge`. Top maintainers by inferred bors triggers, with
  coverage against the team-membership snapshot.

**Data**: `events` (LABELED + ISSUE_COMMENTED + REVIEW_*), team-membership
YAML.

**Output**: `marimo/reviewer_load.py`,
`qb_notebook.review_states.attribute_label_events` (reused by future
themes that need to credit bot-applied labels).

**Open questions**:
- ~~Multiple `maintainer-merge` applications to the same PR (after a
  force-push) — count each, or count first only?~~ Resolved: each
  attributed event counts as one trigger, since the heuristic identifies
  one human per `LABELED` event independently.
- How to anonymize/aggregate when showing per-person plots publicly?
- Could we increase coverage of the residual 1–2 % via comment-body
  parsing? Would require exporting body text from the syncer.

**Notes from implementation**:
- `qb_notebook/teams.py` reads `data/people.yaml` + `data/teams.yaml`
  directly from a sibling `leanprover-community.github.io` checkout;
  there is also a `python -m qb_notebook.teams` CLI for dumping a JSON
  snapshot. Logins are lowercased before set ops.
- The `maintainer-merge` label only entered use around mid-2024, so the
  active-reviewer rolling chart spans ~2 years. `ready-to-merge`
  triggers go back to 2021 and are richer for the bors overlay.
- The earlier version of this theme used `actor_login` on the LABELED
  event directly — which is the bot, not the reviewer. That version was
  scrapped after the upstream TimelineEvent expansion landed.

---

## Theme 3 — Bottleneck localization

**Status: shipped (`marimo/bottleneck_localization.py`,
`qb_notebook.review_states.label_overlap_seconds`).**

**Question**: Once a PR is approved, why doesn't it merge immediately?
Where does the tail of "approved but not merged" latency come from?

**Approach**: For each merged-to-master PR that ever got
`maintainer-merge`, define the *approved window* as
`[first_maintainer_merge, merged_at_effective]`. Within that window,
flag overlap with the stall labels (`merge-conflict`, `awaiting-CI`,
`awaiting-author`, `ready-to-merge`) using
`label_overlap_seconds(intervals, windows)` — a generic per-PR
interval-window overlap helper added to `qb_notebook.review_states`.
Join in `analyzer_prqueuewindow.cycle_index` (ruleset 3) to get the
bors-queue bounce count per PR.

**Plots / metrics**:

- Approved-to-merge cohort summary: median / p75 / p90 / p99 of
  `mm → merge`. Empirical baseline on the current artifact: ~0.35d
  median, 4.3d p90, 30d p99.
- Signal prevalence + conditional latency table. `had_merge_conflict`
  shifts the median from 0.32d → 4.6d (≈14×), `had_awaiting_author`
  from 0.29d → 2.7d (≈9×), `had_awaiting_CI` from 0.34d → 0.80d
  (≈2.4×). Roughly 87 % of approved PRs make it into the bors queue
  at least once.
- Approved-to-merge histogram (≤14d) + long-tail summary.
- Queue-bounce-rate bar chart over `cycle_index + 1`, plus a
  per-bucket latency table.
- Queue close-reason stacked-share over time
  (`closed_by_event_type` monthly). Picks up shifts like CI_FAILED
  vs FORBIDDEN_LABEL_ADDED dominance.
- Currently-stuck approved-but-unmerged PRs: per-PR table of age
  since `maintainer-merge`, current `head_ci_state`, and which stall
  labels are still applied.
- `awaiting-CI` sojourn histogram (closed intervals, ≤72h) +
  `head_ci_state` snapshot for currently-open PRs.

**Data**: `prs`, `events`, `queue_windows` (ruleset 3). No need for
`check_runs` / `status_contexts` at this granularity — the label
intervals already cover the CI-stall signal.

**Output**: `marimo/bottleneck_localization.py` +
`qb_notebook.review_states.label_overlap_seconds` (reusable by
Theme 4 / Theme 5 for any "did label X overlap interval Y" question).

**Notes from implementation**:

- The `awaiting-review` label was retired around 2024-07-10 (last
  event in the dump), shortly after `maintainer-merge` was rolled
  out in 2024-02. It is no longer in `syncer_labeldef`. Historical
  intervals (2021-08 → 2024-07) are still in `events` and remain
  useful for retrospectives. For PRs created after the cutover the
  "in reviewers' court" state is implicit (PR open + not
  `awaiting-author` / `WIP`), with `analyzer_prqueuewindow` ruleset
  3 as the closest machine-defined proxy. Bottleneck localization
  here does not consume `awaiting-review` directly.
- ~3 % of cohort PRs are filtered out by `mm_to_merge_days < 0` —
  these are the cases where `maintainer-merge` was (re)applied
  *after* the bors merge, typically as part of a maintainer
  cleanup. Two PRs total had negative gaps in the current data.
- The "approved window" picks the *first* `maintainer-merge`
  application. Multiple applications after force-push are not
  treated as resetting the clock.

---

## Theme 4 — Topic-area health (`t-*` labels)

**Status: shipped (`marimo/area_health.py`,
`qb_notebook.review_states.labels_active_at`).**

**Question**: Are any topic areas under-reviewed, slower than others, or
losing reviewer coverage?

**Attribution model**: per-PR per-area intervals come from
`label_intervals(events, t_labels)` over `LABELED`/`UNLABELED` events
on the 24 `t-*` labels in `syncer_labeldef`. A PR with multiple `t-*`
labels contributes to every area it carries (resolved open question).
Throughput and reviewer-trigger attribution use the area(s) **active
at the relevant timestamp** — merge time for throughput, attributed
trigger time for reviewer activity — via
`labels_active_at(intervals, points)`.

**Plots / metrics**:

- Current open backlog by area: PR count, median age, p90 age
  (joined from `prlabel` current-state).
- Throughput heatmap: area × month, color = log1p(merges).
- Per-area throughput summary (last 365d) with median + p90 TTM.
- Reviewer-court latency by area: for each `t-*` application,
  `label_overlap_seconds` against `reviewers_court_intervals`; per-area
  median / p90 court-days + share of applications that overlap the
  reviewer's court at all.
- Active reviewer coverage by area (last 30d): distinct attributed
  `maintainer-merge` trigger actors per area at trigger time.
- Top-N reviewers × top-N areas matrix (color = log1p of attributed
  triggers all-time).
- Declining coverage sweep: 30d vs prior 30d attributed-trigger counts
  per area; areas with ≥30 % drop and ≥5 prior-window triggers are
  flagged.

**Data**: `prs`, `prlabel`, `label_defs` (filtered to `t-*`), `events`,
`queue_windows`. Team-membership YAML is consumed in the reviewer ×
area matrix (Session 7 follow-up): y-tick labels are colored by team
and a per-area maintainer / reviewer / other coverage table is
rendered below the matrix. The attribution heuristic itself is shared
with Theme 2 and exposes the human trigger directly.

**Output**: `marimo/area_health.py` +
`qb_notebook.review_states.labels_active_at` (reused by future themes
that need "which label was active when event E fired" lookups, e.g.
Theme 5 for size buckets per area).

**Notes from implementation**:

- ~17 % of merged-to-master PRs in the last year carry no `t-*` label
  at merge time. They drop out of all per-area throughput / latency
  numbers. The per-area totals are therefore a lower bound on real
  area work; the un-tagged share is roughly stable month-over-month
  so the *relative* area rankings are still meaningful.
- Two `t-*` labels (`t-condensed`, `t-geometric-group-theory`) have
  zero current open PRs. They still appear in the throughput table
  but with very small N — sort by activity so they fall to the
  bottom rather than dropping them.
- The `t-*` taxonomy was rolled out incrementally: initial batch in
  2023-07 (`t-algebra`, `t-topology`, `t-analysis`, `t-number-theory`,
  `t-measure-probability`; `t-meta` predates by ~6 months), `t-data`
  in 2024-08, then `t-ring-theory` and `t-group-theory` (carved out
  of `t-algebra`) in 2025-08. The heatmap overlays a white circle on
  each row at the area's first-LABELED month so the dark left edge
  isn't misread as inactivity. `label_defs.created_at` is **not** a
  reliable introduction date — it's just when the syncer inserted
  the row.
- The bipartite matrix uses *attributed* trigger counts only — the
  bot `LABELED` actors are excluded by `attribute_label_events` upstream.
- "Reviewer-court latency by area" restricts to area applications that
  overlap the last 2 years to keep the metric representative of modern
  review tempo; the helper itself has no time filter so callers can
  widen the window if needed.
- 30d-vs-prior-30d declining-coverage sweep is intentionally noisy —
  the 30-day window was chosen for reactivity even though smaller
  areas can swing wildly. Areas with a *zero* prior-window count get
  `pct_change = null` rather than ±∞, and are excluded from the
  `declining` flag.

---

## Theme 5 — PR-shape effects (size, author type)

**Status: shipped (`marimo/pr_shape_effects.py`, `qb_notebook/pr_shape.py`).**

**Question**: Do bigger PRs take disproportionately longer? Do
first-time contributors wait longer?

**Attribution model**: per-PR shape attributes are precomputed once into
a single `prs`-shaped frame via three helpers in `qb_notebook/pr_shape.py`:

- `size_buckets(df_prs)` — adds `lines_changed`, `lines_bucket`
  (default breaks `(10, 50, 200, 1000)`), `files_bucket` (default
  `(1, 3, 10, 30)`).
- `author_cohort(df_prs)` — adds `author_first_pr_at`,
  `author_pr_seq` (1-indexed), `is_first_pr`. Null `author_id` gets
  null cohort columns.
- `started_as_draft(df_prs, df_events)` — first
  `READY_FOR_REVIEW`/`CONVERT_TO_DRAFT` event determines initial state;
  PRs with no draft events fall back to the current `prs.is_draft`
  snapshot to catch drafts that never marked ready.
- `pr_type(df_prs)` — conventional-commit prefix on the title
  (`feat:`, `chore:`, `fix:`, `refactor:`, `doc:`, `perf:`, `ci:`,
  `style:`, `test:`), bucketed into 9 canonical types + `other`
  (parsed but non-canonical, e.g. `experiment:`/`wip:`) + `unparsed`
  (no prefix). The bors `[Merged by Bors] -` prefix is stripped first;
  `feature` aliases to `feat`, `docs` to `doc`.

All five sub-analyses share the same per-PR row so cuts are directly
comparable.

**Plots / metrics**:

- Size × TTM and size × time-to-first-reviewer-court-exit box plots
  by `lines_bucket` (and a per-bucket summary by `files_bucket`).
  Empirical: median TTM scales 0.4d → 1.5d → 3.6d → 4.4d → 2.4d
  across `lines_bucket`; the 1001+ bucket is faster than 201-1000,
  consistent with the long tail being large-but-easy refactor PRs
  that get fast-tracked.
- First-time vs returning contributor funnel: merged / abandoned /
  open / reviewed rates per cohort, plus a TTM-CDF overlay and a
  `author_pr_seq` bucket table (1 / 2-5 / 6-20 / 21+). Empirical:
  first-time merge-rate 57.5 % vs returning 82.0 %; first-time
  reviewed-rate (got `maintainer-merge`) 16.7 % vs returning 21.5 %.
- Author → reviewer concentration: Lorenz curve + Gini of attributed
  `maintainer-merge` triggers, first-time vs returning author cohorts.
  Empirical on current artifact: 49 distinct reviewers cover the
  returning-author cohort vs 28 for first-time-authors — a real
  "newcomer reviewer bench" pattern (plus a top-15 table of who
  reviews first-time-author PRs).
- Draft start: outcome funnel + TTM-CDF + percentile table by
  `started_as_draft`. Empirical: started-as-draft merge-rate 48 %
  (vs 83 % non-draft), median TTM 6.2d (vs 1.7d), p90 67d (vs 31d).
  Drafts are dramatically slower and less likely to merge.
- PR type: per-type cohort sizes, outcome rates (merged / reviewed /
  abandoned), TTM percentile table + box plot, plus a type × lines
  bucket cross-tab so per-type latency differences can be sanity-
  checked against size mix. Empirical on the current artifact:
  `chore:` PRs merge in 0.66d median vs `feat:` 4.32d (~6.5×); `feat:`
  has highest absolute volume (n=20k) and dominates the long tail.
  `unparsed` PRs merge at 44 % vs `feat` 81 % / `chore` 87 % / `doc`
  93 % — following the convention is a strong signal of intentful
  authorship.

**Data**: `prs`, `events`, `queue_windows` (ruleset 3 via
`reviewers_court_intervals`). Team-membership YAML not consumed in this
first cut.

**Output**: `marimo/pr_shape_effects.py` + `qb_notebook/pr_shape.py`
(reused wherever shape cuts come up later — e.g. plot-site polish for
Session 6).

**Notes from implementation**:

- `prs.is_draft` arrives as a Postgres `t`/`f` string in the parquet
  export, not a real bool — the helper takes `draft_true="t"` to make
  this explicit and overridable. The existing `filters.expr_is_draft`
  compares to `True` (Bool), which silently mismatches the String
  column; not fixed here since nothing in the codebase actually calls
  it on real data, but worth a follow-up.
- "First-time" author is keyed off the dataset snapshot, not the
  GitHub-wide history. An author whose first mathlib4 PR was in 2021
  is "returning" on their 2025 PR; an author whose first ever PR is
  in this artifact is "first-time" even if they have years of OSS
  history elsewhere.
- The author-sequence bin (`1`, `2-5`, `6-20`, `21+`) is the closest
  thing to "early-career-in-mathlib4". Use it instead of
  `is_first_pr` for a smoother trend across the first 20 PRs rather
  than the binary first-vs-rest split.
- Time-to-first-court-exit excludes PRs whose first court interval
  is still open at `asof` so the metric stays well defined. Bigger
  PRs have proportionally more open first intervals than smaller
  ones; the box-plot Ns reflect that selection.
- The `WIP` label start was added in Session 9 as Section 4b via
  `had_wip_label_at_open`. Empirically WIP-at-open is a strong but
  *distinct* signal from `started_as_draft`: only ~5 % of WIP-at-open
  merged PRs also started as draft. The earlier framing ("overlaps
  heavily with draft in practice") was wrong — the two cuts capture
  largely different populations. PRs titled `wip:` still land in the
  `other` PR-type bucket and surface a slice of the signal through
  the type axis as well.
- The PR-type prefix is parsed from the title only, after stripping
  the bors `[Merged by Bors] -` rewrite. A handful of `(scope)`
  forms are tolerated (`feat(Algebra/X): ...`); titles that don't
  match the conventional pattern land in `unparsed` rather than
  guessing. The `test:` bucket is tiny (~130 PRs, ~4 % merge-rate)
  and dominated by experimental / WIP titles that happen to start
  with the word "test"; treat its summary stats with caution.

# Review Analysis Plan

This doc enumerates the analyses we want to build on top of the `qb-notebook`
data to understand mathlib4's review process — efficiency, bottlenecks,
reviewer/maintainer load, and area-level health. Each theme is scoped to be
buildable in its own session.

## Background

- **Reviewer team** (~50 people): can sign off PRs by leaving the
  `maintainer-merge` comment, which results in the `maintainer-merge` label
  being applied.
- **Maintainers** (~30 people): can trigger the final bors merge to master.
- Team membership lives at <https://leanprover-community.github.io/teams.html>;
  the canonical source is in the `leanprover-community.github.io` repo
  (expected sibling checkout at `../leanprover-community.github.io/`):
  - `data/people.yaml` — list of `{name, github, descr, img}`, mapping
    full name → GitHub login. ~400 lines.
  - `data/teams.yaml` — list of `{name, members: [full name, ...], url, ...}`.
    Relevant team names: **`Maintainer team`** (~30 people, can trigger
    bors merge), **`Mathlib reviewers`** (~50 people, can apply
    `maintainer-merge`), `Admin team`, `Continuous integration`,
    `Code of conduct`, `Website/blog`.
  - Process these with a small script rather than reading them into LLM
    context. The join is `teams.members[*]` → `people.name` →
    `people.github`. Be defensive about missing entries (people listed on
    a team but not in `people.yaml`).
- Existing analyses cover **throughput** (`pr_merge_throughput.ipynb`),
  **PR open durations** (`pr_open_durations.ipynb`), and **queue windows**
  (`queue_windows.ipynb`). Plot site renders a subset of these.

## Data caveats

1. `syncer_prtimelineevent.type` covers `LABELED` / `UNLABELED` / `CLOSED`
   / `READY_FOR_REVIEW` / `REOPENED` / `ASSIGNED` / `UNASSIGNED` /
   `CONVERT_TO_DRAFT` / `HEAD_FORCE_PUSHED` plus, since queueboard-core
   #164 (2026-05), `ISSUE_COMMENTED`, `REVIEW_APPROVED`,
   `REVIEW_COMMENTED`, `REVIEW_CHANGES_REQUESTED`, `REVIEW_DISMISSED`,
   `REVIEW_REQUESTED`, and `REVIEW_REQUEST_REMOVED`. Comment **bodies**
   are not exported, only event metadata + actor + timestamps.
2. `syncer_prreviewinlinecomment.parquet` is also exported (FK to the
   parent `PullRequestReview` event), but again without comment bodies.
3. `syncer_pullrequest.approvals` carries the GitHub-native approving
   reviewers (independent of the `maintainer-merge` label workflow).
   Useful as a secondary signal; ~52 % coverage on maintainer-merged PRs.
4. Snapshot tables (queue/area/reviewer-assignment) are not exported.
5. **Bot-applied labels need attribution.** `maintainer-merge` and
   `ready-to-merge` are added by bots (`github-actions`,
   `leanprover-community-mathlib4-bot`, `mathlib-triage`) in response to
   human comments (`maintainer merge`, `bors r+`, etc.). Use
   `qb_notebook.review_states.attribute_label_events` to credit the
   human who triggered each label: it picks the most recent non-bot
   `ISSUE_COMMENTED` / `REVIEW_*` event on the same PR within a
   configurable window (default 10 min). Empirical coverage with the
   new TimelineEvent ingest: **>99 % for `maintainer-merge`**, **~98 %
   for `ready-to-merge`** in mathlib4. The residual ~1–2 % are mostly
   labels applied long after the trigger comment (e.g. waiting on CI).
6. Team-membership YAML from the leanprover-community website is
   required to split metrics by *reviewer team* vs *maintainer team* vs
   *other contributors*. See `qb_notebook.teams`.

## Key mathlib labels (workflow state machine)

- `awaiting-review` — historically marked "PR is in reviewers' court"
  (used 2021-08 → 2024-07-10, then retired and removed from
  `syncer_labeldef`; a tombstone `awaiting-review-DONT-USE` label
  exists). After retirement the "in reviewers' court" state is
  implicit (PR open + not `awaiting-author` / `WIP`); the analyzer's
  queue-window ruleset 3 (`analyzer_prqueuewindow`) is the closest
  modern, machine-defined proxy.
- `awaiting-author` — PR is back in author's court (changes requested).
- `WIP` — author marks as work-in-progress.
- `maintainer-merge` — reviewer sign-off applied (label introduced
  2024-02-15; overlaps with `awaiting-review` for ~5 months).
- `delegated` — author has been granted delegated-merge permission.
- `auto-merge-after-CI` — queued to merge once CI passes.
- `ready-to-merge` — in bors queue.
- `t-*` — topic-area labels (algebra, analysis, etc.).
- `merge-conflict`, `blocked-by-other-PR`, `awaiting-CI` — stall states.

The exact set should be confirmed against `syncer_labeldef.parquet` when
implementing each theme.

---

## Themes

### Theme 1 — Review state machine (sojourn times & ping-pong)

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
  unified-intervals helper proposed under
  [Cross-cutting infrastructure](#cross-cutting-infrastructure) can
  lean on this with high confidence.

---

### Theme 2 — Reviewer & maintainer load

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

### Theme 3 — Bottleneck localization

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

### Theme 4 — Topic-area health (`t-*` labels)

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

### Theme 5 — PR-shape effects (size, author type)

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

---

## Cross-cutting infrastructure

These keep showing up in multiple themes and should be implemented once:

- **`label_intervals(events, pr_id, label_name) -> [(start, end, actor)]`** —
  reconstruct intervals when a label was applied, including actor on
  apply/remove. Lives in `qb_notebook/review_states.py`. Used by
  Themes 1, 3, 4, 5. ✅ shipped.
- **`label_overlap_seconds(intervals, windows) -> windows + overlap_seconds + had_overlap`** —
  generic per-PR interval-vs-window overlap. Given the output of
  `label_intervals` for one label and a per-PR window frame, sums
  intersections in seconds and adds a `had_overlap` flag. Lives in
  `qb_notebook/review_states.py`. Used by Themes 3 and 4 (per-area
  reviewer-court latency); ready for Theme 5 wherever a "did label X
  happen during interval Y" question comes up. ✅ shipped.
- **`labels_active_at(intervals, points) -> points + label_name`** —
  for each `(pull_request_id, timestamp)` point in `points`, return one
  row per label interval active at that time (half-open
  `[start, end_effective)`). Points with no active interval drop out;
  points with multiple matching labels emit one row each (count-each
  semantics). Lives in `qb_notebook/review_states.py`. Used by Theme 4
  to attribute merges and reviewer triggers to active `t-*` areas;
  ready for Theme 5 for "which area was the PR in when X happened"
  size/contributor cuts. ✅ shipped.
- **`teams.load(repo_path) -> {reviewers, maintainers, admins, ...: set[login]}`** —
  parse `data/people.yaml` + `data/teams.yaml` from a checkout of
  `leanprover-community.github.io` and return sets of GitHub logins per
  team. Should warn (not error) on team members missing from
  `people.yaml`. Likely lives in `qb_notebook/teams.py`. Used by
  Themes 2, 4. Probably also expose a CLI entry point
  (`python -m qb_notebook.teams --repo ../leanprover-community.github.io`)
  that dumps a JSON snapshot, so notebooks don't need the sibling
  checkout at runtime.
- **`actor_counts(events, label_name, freq='1mo')`** — group LABELED
  events by actor and time bucket. Used by Themes 2, 4.
- **`reviewers_court_intervals(events, queue_windows, *, asof, label_asof)`** —
  unified per-PR "in reviewers' court" intervals. Per-PR, queue-window
  (ruleset 3) is primary; `awaiting-review` label intervals fall back
  for the ~219 PRs (2022-12 → 2024-07) whose data the analyzer
  missed. Returns the common interval columns
  (`start`/`end`/`is_open`/`end_effective`/`duration*`) plus a
  `source` column ("queue_window" | "label"). The optional
  `label_asof` parameter clamps label-source intervals to a retirement
  date (mathlib: `datetime(2024, 7, 10, UTC)`) since label deletion
  doesn't emit `UNLABELED` events. Lives in
  `qb_notebook/review_states.py`. Reused by Themes 2, 4, and 5.
  ✅ shipped.
- **PR-shape helpers in `qb_notebook/pr_shape.py`** —
  `size_buckets(df_prs)` adds `lines_changed` / `lines_bucket` /
  `files_bucket`; `author_cohort(df_prs)` adds `author_first_pr_at` /
  `author_pr_seq` / `is_first_pr`; `started_as_draft(df_prs, df_events)`
  adds `started_as_draft` from `READY_FOR_REVIEW`/`CONVERT_TO_DRAFT`
  events with `is_draft` snapshot fallback; `pr_type(df_prs)` adds
  `pr_type` from the title's conventional-commit prefix (bors prefix
  stripped, `feature`/`docs` aliased, non-canonical → `other`, no
  prefix → `unparsed`). `bucket_labels(breaks)` exposes size-axis
  ordering and `pr_type_order()` exposes type-axis ordering for plots.
  Used by Theme 5; ready for plot-site polish (Session 6) wherever
  shape cuts come up. ✅ shipped.

A nice-to-have upstream change: an explicit `queueboard-core`
ruleset preserving the original `awaiting-review` semantics, so the
choice of ruleset_id encodes "court" rather than living in helper
code.

---

## Post-Theme-5 cleanup pass

After all five themes shipped, a survey across the codebase surfaced
consolidation opportunities and cross-cuts that earlier themes can borrow
from later ones. Tracked separately so they can be picked up incrementally.

### Boilerplate consolidation (Session 6)

- **`merged_prs_frame(prs, *, effective_col=...)`** in
  `qb_notebook/data_io.py` to replace the 5-notebook duplicated
  `prs.filter(expr_merged_to_master()).with_columns(
  expr_merged_at_effective().alias(...))` pattern. Standardizes the
  subtle name drift between callsites (`"merged_at"` in
  `area_health.py` / `pr_shape_effects.py` vs `"merged_at_effective"`
  elsewhere).
- **`MAINTAINER_MERGE_LABEL` / `READY_TO_MERGE_LABEL` constants** in
  `qb_notebook/review_states.py` to replace string literals scattered
  across notebooks (typo-safety; not aggressively migrated).
- **`DEFAULT_ATTRIBUTION_WINDOW_SECONDS = 600`** constant in
  `qb_notebook/review_states.py`, used as the default of
  `attribute_label_events`. Two notebooks currently pass `600`
  explicitly — drop those.
- **`expr_is_draft` fix**: currently exported but unused, and compares
  to `True` (Bool) while the parquet column is Postgres-style
  `"t"`/`"f"` string (flagged in Theme 5 notes). Update the helper to
  take an overridable `draft_true="t"` default and adopt it where
  `review_state_machine.py` inlines `~pl.col("is_draft").fill_null(False)`.

The marimo notebook bootstrap (`sys.path` + data dir) is deliberately
**not** extracted: `qb_notebook` is not installed as a package (see
`pyproject.toml` `[tool.uv] package = false`), so the bootstrap creates
its own `sys.path` entry — a helper inside `qb_notebook` would not be
reachable until after. Flipping `package = true` and editable-installing
is a bigger architectural change tracked separately.

### Theme-4 follow-up: team-annotated reviewer × area matrix (Session 7) — shipped

The bipartite reviewer × area matrix in `marimo/area_health.py` now
overlays team membership: y-tick labels are colored by team
(maintainer / reviewer / other) when the sibling
`leanprover-community.github.io` checkout is present, with a small
legend in the upper-right. A new per-area team-coverage table below
the matrix breaks out how many distinct reviewers from each tier have
triggered a `maintainer-merge` in that area all-time. The
`teams = load(...)` load cell mirrors the `marimo/reviewer_load.py`
pattern and falls through gracefully (warn-and-render-empty) if the
checkout is missing. Empirically on the current artifact the
maintainer-team count per area ranges 0–17 (`t-algebra` highest, with
`t-geometric-group-theory` having no maintainer-team triggers at
all); the `other` column is near-zero, confirming
`attribute_label_events` cleanly excludes the bots.

### Cross-cuts: shape and area effects on review state (Session 8)

Earlier themes ran before `pr_shape` and `labels_active_at` shipped.
Adding facets:

- **Theme 1 sojourn × shape**: `review_state_machine.py` /
  `queue_window_state.py` sojourn distributions and ping-pong counts
  faceted by `pr_type` and `lines_bucket`. Does `feat:` ping-pong more
  than `chore:`? Are 1000+-line PRs in `awaiting-review` longer?
- **Theme 3 stall signals × area**: `bottleneck_localization.py`
  `had_merge_conflict` / `had_awaiting_author` / `had_awaiting_CI`
  prevalence broken down by area (via `labels_active_at(t_intervals,
  approved_window_starts)`). Are merge-conflicts disproportionate in
  any single `t-*` area?
- **Theme 3 stall signals × shape**: same signals broken down by
  `pr_type` and `lines_bucket`. A 1000+-line `feat:` likely has very
  different stall-prevalence than a 1-line `chore:`.

### Theme-2 active-reviewer trend × team (Session 9) — shipped

`marimo/reviewer_load.py` now renders a second active-reviewer
rolling chart immediately after the aggregate one, splitting three
ways by team membership (maintainer / reviewer / other) when the
sibling `leanprover-community.github.io` checkout is present. Same
trailing-window helper (`rolling_distinct_actors`), same colour
palette as the team overlay added in Session 7. Empirical on the
current artifact: reviewer-tier dominates (~26 distinct attributed
actors over the project lifetime), maintainer-tier second (~20),
"other" is tiny (3 actors, 4 events) — mostly stale or
not-yet-listed logins. Most-recent 28-day window: 17 reviewers + 4
maintainers, no "other".

### Theme-5 follow-ups (Session 9) — shipped

- **`had_wip_label_at_open(df_prs, df_events)`** added to
  `qb_notebook/pr_shape.py`: a PR is flagged True when the earliest
  `LABELED(WIP)` event fires within 10 minutes of `gh_created_at`
  (default, configurable via `open_window_seconds`). The 10-minute
  window matches `attribute_label_events` and captures ~77 % of
  LABELED(WIP) events on mathlib4 — the apply-time distribution is
  bimodal (sharp mode under 1 min, then a long tail of PRs converted
  to WIP later). Five unit tests cover the within/outside-window,
  first-LABELED-wins, no-events, and other-labels-ignored cases.
- Wired into `marimo/pr_shape_effects.py` as Section 4b: outcome
  funnel, TTM percentile table, and a 2×2 overlap matrix against
  `started_as_draft`. Empirical findings:
  - WIP-at-open merged-rate **73 %** vs **82 %** non-WIP; reviewed
    rate **12 %** vs **22 %**; abandon rate **17 %** vs **11 %**.
  - TTM median **3.9 d** vs **1.7 d** non-WIP; p90 **53 d** vs
    **30 d**. WIP PRs are substantially slower and less likely to
    merge — comparable in magnitude to the draft signal.
  - 2×2 against `started_as_draft` is the surprise: only ~5 % of
    WIP-at-open merged PRs also started as draft, and ~13 % of
    started-as-draft merged PRs also had WIP-at-open. **The two
    cuts are largely orthogonal**, not redundant — they capture
    distinct populations. Earlier plan-doc framing ("overlaps
    heavily with draft in practice") was wrong; corrected in the
    notebook intro and Notes block.
- **`expr_is_draft` bool fix** — already shipped under Session 6
  boilerplate consolidation.

### Plot site (Session 10)

The 6 marimo notebooks build their plots as inline `plt.subplots()`
cells. `qb_notebook/generate_plot_site.py` expects
`render_*(ctx) -> Figure` functions registered via `PlotDefinition`.
Promotion requires each notebook's headline plots extracted into a
renderer module (e.g. `qb_notebook/plotting/theme_4.py`) and
registered. Doing one notebook end-to-end first will set the pattern
for the rest.

## Roadmap

| Session | Theme                          | Deliverable                                              | Status   |
| ------- | ------------------------------ | -------------------------------------------------------- | -------- |
| 1       | Theme 1: state machine (labels)| `marimo/review_state_machine.py` + `review_states.py`    | shipped  |
| 2       | Theme 2: reviewer load         | `marimo/reviewer_load.py` + `qb_notebook/teams.py`       | shipped  |
| 3       | Theme 3: bottlenecks           | `marimo/bottleneck_localization.py`                      | shipped  |
| 3.5     | Theme 1 companion (queue)      | `marimo/queue_window_state.py` + `queue_window_intervals`| shipped  |
| 4       | Theme 4: area health           | `marimo/area_health.py` + `labels_active_at`             | shipped  |
| 5       | Theme 5: PR shape              | `marimo/pr_shape_effects.py` + `qb_notebook/pr_shape.py` | shipped  |
| 6       | Cleanup: boilerplate           | `merged_prs_frame`, label/window constants, `expr_is_draft` fix | planned |
| 7       | Theme 4: team × area matrix    | team annotation on reviewer × area matrix in `area_health.py` | shipped |
| 8       | Cross-cuts: shape × area       | Theme 1/3 sojourn & stall signals × `pr_type`/`lines_bucket`/area | planned |
| 9       | Theme 2/5: tier + WIP follow-ups | active-reviewer trend split by team; `had_wip_label_at_open` cut | shipped |
| 10      | Plot site polish               | promote best plots from each notebook                    | planned  |

Order is flexible — Themes 1 and 2 were the highest-value starting points;
the post-Theme-5 sessions (6+) are cleanups and cross-cuts unlocked by the
shipped helpers.

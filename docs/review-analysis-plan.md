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

**Status: planned. Requires area-label whitelist.**

**Question**: Are any topic areas under-reviewed, slower than others, or
losing reviewer coverage?

**Plots / metrics**:

- Per-area: current open PR count, median age, throughput (merges/month),
  active reviewer count over the last 90 days.
- Heatmap: area × month, cell = median time-to-merge.
- Per-area reviewer overlap: which reviewers sign off PRs in which areas?
  Bipartite graph or matrix.
- Areas with declining `maintainer-merge` activity over the last 6 months.

**Data**: `prs`, `prlabel`, `label_defs` (filtered to `t-*`), `events`,
`queue_windows`, team-membership YAML.

**Output**: `area_health.ipynb` + per-area summary table on the plot site.

**Open questions**:
- A PR can have multiple `t-*` labels — count toward each, or pick one
  (e.g. first applied)?

**Notes**:
- Use `reviewers_court_intervals` (cross-cutting infra below) for the
  "active reviewer count" and any per-area reviewer-court latency
  computations, so the metric is comparable across mathlib4's full
  history rather than disappearing after the `awaiting-review`
  retirement.

---

### Theme 5 — PR-shape effects (size, author type)

**Status: planned.**

**Question**: Do bigger PRs take disproportionately longer? Do
first-time contributors wait longer?

**Plots / metrics**:

- Size buckets (`additions + deletions`, `changed_files_count`) vs
  median time-to-first-reviewer-court-exit (use
  `reviewers_court_intervals` so the metric survives the
  `awaiting-review` retirement) and time-to-merge. Box plots by bucket.
- First-time vs returning contributors (first-seen `author_id` in
  `syncer_pullrequest`): merge-time distributions, ratio of PRs reviewed,
  ratio merged vs abandoned.
- Author → reviewer concentration: do new authors get reviewed by a
  narrow subset of reviewers?
- Effect of `WIP`/draft start: do PRs that begin as drafts merge faster
  or slower than ones that don't?

**Data**: `prs`, `events`, `prlabel`.

**Output**: `pr_shape_effects.ipynb`. Probably no plot-site additions
unless a clear top-level chart emerges.

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
  `qb_notebook/review_states.py`. Used by Theme 3; ready for Themes
  4 / 5 wherever a "did label X happen during interval Y" question
  comes up. ✅ shipped.
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
  `qb_notebook/review_states.py`. To be reused by Themes 2, 4, and
  5. ✅ shipped.

A nice-to-have upstream change: an explicit `queueboard-core`
ruleset preserving the original `awaiting-review` semantics, so the
choice of ruleset_id encodes "court" rather than living in helper
code.

## Roadmap

| Session | Theme                          | Deliverable                                              | Status   |
| ------- | ------------------------------ | -------------------------------------------------------- | -------- |
| 1       | Theme 1: state machine (labels)| `marimo/review_state_machine.py` + `review_states.py`    | shipped  |
| 2       | Theme 2: reviewer load         | `marimo/reviewer_load.py` + `qb_notebook/teams.py`       | shipped  |
| 3       | Theme 3: bottlenecks           | `marimo/bottleneck_localization.py`                      | shipped  |
| 3.5     | Theme 1 companion (queue)      | `marimo/queue_window_state.py` + `queue_window_intervals`| shipped  |
| 4       | Theme 4: area health           | `area_health.ipynb`                                      | planned  |
| 5       | Theme 5: PR shape              | `pr_shape_effects.ipynb`                                 | planned  |
| 6       | Plot site polish               | promote best plots from each notebook                    | planned  |

Order is flexible — Themes 1 and 2 are the highest-value starting points.

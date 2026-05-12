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

1. `syncer_prtimelineevent.type` does **not** include
   `REVIEWED` / `APPROVED` / `CHANGES_REQUESTED` events. Available types are
   `ASSIGNED`, `CLOSED`, `CONVERT_TO_DRAFT`, `HEAD_FORCE_PUSHED`, `LABELED`,
   `READY_FOR_REVIEW`, `REOPENED`, `UNASSIGNED`, `UNLABELED`. **Mathlib's
   review handoff is encoded as labels**, so this is fine — `LABELED` /
   `UNLABELED` rows with `label_name` + `actor_login` give us the
   review state machine.
2. `syncer_pullrequest.approvals` carries the GitHub-native approving
   reviewers (independent of the `maintainer-merge` label workflow). Useful
   as a secondary signal.
3. Snapshot tables (queue/area/reviewer-assignment) are not exported.
4. Bors itself shows up as an actor on merges via the
   `[Merged by Bors]` title prefix and the `CLOSED` / merged-at columns;
   the human who triggered bors is not directly recorded in the timeline.
   Proxy: actor on the last `ready-to-merge` LABELED event, or commenter
   that issued `bors merge`/`bors r+` (would require comment-body data we
   don't currently export).
5. Team-membership YAML from the leanprover-community website is required
   to split metrics by *reviewer team* vs *maintainer team* vs *other
   contributors*. Without it, we have to use observed-behavior proxies
   (e.g. "anyone who has ever applied `maintainer-merge`").

## Key mathlib labels (workflow state machine)

- `awaiting-review` — PR is in reviewers' court.
- `awaiting-author` — PR is back in author's court (changes requested).
- `WIP` — author marks as work-in-progress.
- `maintainer-merge` — reviewer sign-off applied.
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

**Status: next up.**

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

---

### Theme 2 — Reviewer & maintainer load

**Status: shipped (`marimo/reviewer_load.py`, `qb_notebook/teams.py`).**

**Question**: Who is doing the review work, how concentrated is it, and is
the active-reviewer pool growing or shrinking?

**Plots / metrics**:

- Maintainer-merge actor counts per reviewer per month (top-N table, plus
  long-tail histogram).
- **Active reviewer count per week**: number of distinct `actor_login`
  values on `LABELED(maintainer-merge)` events in a rolling 7-day or
  28-day window. Trend line over project history.
- **Concentration**: Lorenz curve + Gini coefficient of `maintainer-merge`
  applications by reviewer, computed yearly.
- **Bus factor**: smallest N reviewers covering 50 % / 80 % of
  `maintainer-merge` events, plotted over time.
- Bors-trigger proxy: actor on `ready-to-merge` LABELED events — counts
  per maintainer, and overlap with reviewer-team list.
- Per-reviewer: median time from PR's first `awaiting-review` to their
  `maintainer-merge` label (only counting PRs they signed off).
- Reviewer "fan-out": for each author, how many distinct reviewers have
  signed off their PRs?

**Data**: `events` (LABELED only, filtered to relevant label names),
`label_defs`, `core_user`, team-membership YAML.

**Output**: `reviewer_load.ipynb` + helpers in
`qb_notebook/reviewer_metrics.py`. Several plot-site additions.

**Open questions**:
- ~~Multiple `maintainer-merge` applications to the same PR (after a
  force-push) — count each, or count first only?~~ Resolved: notebook
  computes **both** ("per application" and "first per PR") side-by-side
  so the gap surfaces re-sign-off load explicitly.
- How to anonymize/aggregate when showing per-person plots publicly?

**Notes from implementation**:
- `qb_notebook/teams.py` reads `data/people.yaml` + `data/teams.yaml`
  directly from a sibling `leanprover-community.github.io` checkout;
  there is also a `python -m qb_notebook.teams` CLI for dumping a JSON
  snapshot. Logins are lowercased before set ops.
- The `maintainer-merge` label only entered use around mid-2024, so
  the active-reviewer rolling chart spans ~2 years and yearly Lorenz
  starts at 2024. `ready-to-merge` LABELED events go back to 2021 and
  are richer for the bors-trigger overlay.

---

### Theme 3 — Bottleneck localization

**Status: planned.**

**Question**: Once a PR is approved, why doesn't it merge immediately?
Where does the tail of "approved but not merged" latency come from?

**Plots / metrics**:

- `maintainer-merge` → `merged_at` latency distribution. Split by:
  whether `ready-to-merge` was applied, whether CI failed in between,
  whether `merge-conflict` appeared.
- Queue-bounce rate: per PR, number of times it entered and exited the
  queue (via existing `analyzer_prqueuewindow.cycle_index`).
- Top reasons a queue window closed (already partly in `queue_windows.ipynb`
  via `closed_by_event_type`) — broken down by time period to spot
  regressions.
- For approved-but-unmerged PRs currently open: histogram of age since
  `maintainer-merge` and which stall labels they carry.
- CI-related stalls: distribution of time spent with a `awaiting-CI` or
  failing `head_ci_state`.

**Data**: `prs`, `events`, `queue_windows`, `check_runs`, `status_contexts`.

**Output**: `bottleneck_localization.ipynb`. May extend
`qb_notebook/intervals.py` with `time_in_label` helpers (overlaps with
Theme 1 helpers — implement once, reuse).

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
team-membership YAML.

**Output**: `area_health.ipynb` + per-area summary table on the plot site.

**Open questions**:
- A PR can have multiple `t-*` labels — count toward each, or pick one
  (e.g. first applied)?

---

### Theme 5 — PR-shape effects (size, author type)

**Status: planned.**

**Question**: Do bigger PRs take disproportionately longer? Do
first-time contributors wait longer?

**Plots / metrics**:

- Size buckets (`additions + deletions`, `changed_files_count`) vs
  median time-to-first-`awaiting-review`-removal and time-to-merge. Box
  plots by bucket.
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
  apply/remove. Belongs in `qb_notebook/intervals.py` or a new
  `review_states.py`. Used by Themes 1, 3, 4, 5.
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

## Roadmap

| Session | Theme                          | Deliverable                                              | Status   |
| ------- | ------------------------------ | -------------------------------------------------------- | -------- |
| 1       | Theme 1: state machine         | `marimo/review_state_machine.py` + `review_states.py`    | shipped  |
| 2       | Theme 2: reviewer load         | `marimo/reviewer_load.py` + `qb_notebook/teams.py`       | shipped  |
| 3       | Theme 3: bottlenecks           | `bottleneck_localization.ipynb`                          | planned  |
| 4       | Theme 4: area health           | `area_health.ipynb`                                      | planned  |
| 5       | Theme 5: PR shape              | `pr_shape_effects.ipynb`                                 | planned  |
| 6       | Plot site polish               | promote best plots from each notebook                    | planned  |

Order is flexible — Themes 1 and 2 are the highest-value starting points.

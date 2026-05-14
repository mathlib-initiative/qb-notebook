# Review Analysis Plan

This doc and its companions enumerate the analyses we want to build on
top of the `qb-notebook` data to understand mathlib4's review process —
efficiency, bottlenecks, reviewer/maintainer load, and area-level
health. Each theme is scoped to be buildable in its own session.

## How this doc is organized

- **This file** (`docs/review-analysis-plan.md`) is the index: project
  background, data caveats, key labels, the cross-cutting helper
  inventory, and the master roadmap.
- [`review-analysis/themes.md`](review-analysis/themes.md) — the five
  original themes (state machine, reviewer load, bottlenecks, area
  health, PR shape), all shipped. Historical record + design notes.
- [`review-analysis/sessions.md`](review-analysis/sessions.md) —
  follow-up sessions (Sessions 6+: cleanups, cross-cuts, gap fills).
  Append-only as new sessions ship.
- [`review-analysis/backlog.md`](review-analysis/backlog.md) — the
  prioritized backlog of remaining gaps and synthesis stories.
  **Read this first when picking up new work.**

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
- **`first_review_touch(df_prs, df_events, *, event_types, bot_actors)`** —
  per-PR earliest non-author, non-bot review/comment event. Returns
  one row per PR with `first_touch_at` / `first_touch_actor` /
  `first_touch_event_type` / `first_touch_seconds_from_open`; PRs with
  no qualifying event get nulls. Requires `df_prs` to carry
  `author_login` (join `core_user.github_login` on `prs.author_id`
  upstream — `load_pr_interval_data` already casts `author_id` to
  `Int64` for that join). Default `event_types` covers `REVIEW_*` +
  `ISSUE_COMMENTED`; pass a stricter tuple for a "substantive review"
  variant. Lives in `qb_notebook/review_states.py`. Used by Session 11
  (first-touch latency cells in `marimo/reviewer_load.py`); ready for
  Stories A (anatomy-of-a-merge) and D (newcomer experience). ✅ shipped.
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

## Roadmap

| Session | Theme                            | Deliverable                                                     | Status   |
| ------- | -------------------------------- | --------------------------------------------------------------- | -------- |
| 1       | Theme 1: state machine (labels)  | `marimo/review_state_machine.py` + `review_states.py`           | shipped  |
| 2       | Theme 2: reviewer load           | `marimo/reviewer_load.py` + `qb_notebook/teams.py`              | shipped  |
| 3       | Theme 3: bottlenecks             | `marimo/bottleneck_localization.py`                             | shipped  |
| 3.5     | Theme 1 companion (queue)        | `marimo/queue_window_state.py` + `queue_window_intervals`       | shipped  |
| 4       | Theme 4: area health             | `marimo/area_health.py` + `labels_active_at`                    | shipped  |
| 5       | Theme 5: PR shape                | `marimo/pr_shape_effects.py` + `qb_notebook/pr_shape.py`        | shipped  |
| 6       | Cleanup: boilerplate             | `merged_prs_frame`, label/window constants, `expr_is_draft` fix | shipped  |
| 7       | Theme 4: team × area matrix      | team annotation on reviewer × area matrix in `area_health.py`   | shipped  |
| 8       | Cross-cuts: shape × area         | Theme 1/3 sojourn & stall signals × `pr_type` / `lines_bucket`  | shipped  |
| 9       | Theme 2/5: tier + WIP follow-ups | active-reviewer trend by team; `had_wip_label_at_open` cut      | shipped  |
| 10      | Plot-site polish                 | promote best plots from each notebook                           | planned  |
| 11      | Gap: first-touch latency         | `first_review_touch` helper + section in `reviewer_load.py`     | shipped  |
| 12+     | Gaps & stories                   | see [backlog](review-analysis/backlog.md)                       | planned  |

Order is flexible — Themes 1 and 2 were the highest-value starting points;
the post-Theme-5 sessions (6+) are cleanups and cross-cuts unlocked by the
shipped helpers. Sessions 11+ ship gaps autonomously and pause for
review after each story; full session detail lives in
[`review-analysis/sessions.md`](review-analysis/sessions.md) and the
prioritized future work lives in
[`review-analysis/backlog.md`](review-analysis/backlog.md).

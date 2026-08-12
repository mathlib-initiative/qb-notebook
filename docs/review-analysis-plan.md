# Review Analysis Plan

This doc and its companions enumerate the analyses we want to build on
top of the `qb-notebook` data to understand mathlib4's review process —
efficiency, bottlenecks, reviewer/maintainer load, and area-level
health. Each theme is scoped to be buildable in its own session.

## How this doc is organized

- **This file** (`docs/review-analysis-plan.md`) is the index: project
  background, data caveats, key labels, and the cross-cutting helper
  inventory.
- [`review-analysis/notebooks.md`](review-analysis/notebooks.md) — the
  reader's guide to the shipped notebooks: what each one answers, its
  headline findings, and the helpers it's built on.
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

These are used across multiple notebooks. All are shipped and live in
`qb_notebook/`; each notebook's [guide entry](review-analysis/notebooks.md)
lists which it leans on.

- **`label_intervals(events, pr_id, label_name) -> [(start, end, actor)]`** —
  reconstruct intervals when a label was applied, including actor on
  apply/remove. Lives in `qb_notebook/review_states.py`.
- **`label_overlap_seconds(intervals, windows) -> windows + overlap_seconds + had_overlap`** —
  generic per-PR interval-vs-window overlap. Given the output of
  `label_intervals` for one label and a per-PR window frame, sums
  intersections in seconds and adds a `had_overlap` flag. Lives in
  `qb_notebook/review_states.py`.
- **`labels_active_at(intervals, points) -> points + label_name`** —
  for each `(pull_request_id, timestamp)` point in `points`, return one
  row per label interval active at that time (half-open
  `[start, end_effective)`). Points with no active interval drop out;
  points with multiple matching labels emit one row each (count-each
  semantics). Lives in `qb_notebook/review_states.py`.
- **`teams.load(repo_path) -> {reviewers, maintainers, admins, ...: set[login]}`** —
  parse `data/people.yaml` + `data/teams.yaml` from a checkout of
  `leanprover-community.github.io` and return sets of GitHub logins per
  team. Warns (does not error) on team members missing from
  `people.yaml`. Lives in `qb_notebook/teams.py`. A CLI entry point
  (`python -m qb_notebook.teams --repo ../leanprover-community.github.io`)
  dumps a JSON snapshot, so notebooks don't need the sibling checkout at
  runtime.
- **`first_review_touch(df_prs, df_events, *, event_types, bot_actors)`** —
  per-PR earliest non-author, non-bot review/comment event. Returns
  one row per PR with `first_touch_at` / `first_touch_actor` /
  `first_touch_event_type` / `first_touch_seconds_from_open`; PRs with
  no qualifying event get nulls. Requires `df_prs` to carry
  `author_login` (join `core_user.github_login` on `prs.author_id`
  upstream — `load_pr_interval_data` already casts `author_id` to
  `Int64` for that join). Default `event_types` covers `REVIEW_*` +
  `ISSUE_COMMENTED`; pass a stricter tuple for a "substantive review"
  variant. Lives in `qb_notebook/review_states.py`.
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
  `qb_notebook/review_states.py`.
- **`pipeline_stages(df_prs, df_events, *, asof=None, pr_merged_col="merged_at", ...)`** —
  per-PR five-stage milestone frame: `opened_at`, `first_touch_at`,
  `first_maintainer_merge_at`, `first_ready_to_merge_at`,
  `merged_at_effective`, plus the four sequential stage deltas in
  seconds (`seconds_open_to_first_touch` /
  `seconds_first_touch_to_maintainer_merge` /
  `seconds_maintainer_merge_to_ready_to_merge` /
  `seconds_ready_to_merge_to_merged`) and the total
  `seconds_open_to_merged`. Wraps `first_review_touch` +
  `stage_timestamps` internally; non-monotonic deltas null rather
  than going negative so log-scale plots stay clean. Caller supplies
  the bors-aware merge timestamp (typically via
  `pl.when(expr_merged_to_master()).then(expr_merged_at_effective()).otherwise(None)`
  upstream). Lives in `qb_notebook/review_states.py`.
- **`inline_comment_stats(df_inline, df_prs, *, bot_actors)`** —
  per-PR rollup of `syncer_prreviewinlinecomment` rows: total
  comments, comments-by-others (author + bots excluded — the
  headline review-depth signal), distinct threads
  (`thread_root_node_id`), thread replies, distinct non-author
  reviewers, distinct files touched, first/last inline comment
  timestamps. Comment bodies aren't exported so this is the
  best *substantive* review-depth proxy the dataset offers. Follows
  the `first_review_touch` pattern: `df_prs` carries
  `author_login` (join `core_user.github_login` upstream); author
  comparison is case-insensitive. PRs with no inline comments are
  absent from the output. Lives in `qb_notebook/review_states.py`.
- **Temporal helpers in `qb_notebook/temporal.py`** —
  `with_temporal_columns(df, ts_col, *, prefix="")` adds UTC
  `hour_utc` / `weekday` (0=Mon, 6=Sun) / `is_weekend` / `month` /
  `year` / `year_month` derivations from any datetime column;
  `weekday_hour_histogram(df, *, ts_col)` produces the dense
  7×24 = 168-cell zero-filled count frame heatmaps render off;
  `actor_activity_window(events, *, window_hours=8, min_events=20)`
  infers each actor's best contiguous UTC activity window
  (returns `peak_hour`, `window_start`, `window_end`,
  `active_hours: list[int]`, `active_hours_share`); and
  `hour_set_overlap(a, b)` is the trivial pairwise comparator.
  Plus `WEEKDAY_LABELS` / `MONTH_LABELS` / `WEEKEND_DAYS` constants.
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
- **Cohort helpers in `qb_notebook/cohorts.py`** — `CohortSpec` is a
  declarative "which PRs" description (inclusive day bounds + `t-*`
  topics + PR types + size buckets + author cohort, empty meaning "no
  filter on this dimension") and `filter_cohort(df, spec, *,
  t_intervals=...)` applies one to a wide per-PR frame;
  `cohort_frames(df, specs)` slices once per spec into a
  `{name: frame}` mapping (names uniquified by `with_unique_names`).
  The aggregates take that mapping and return one tidy row per cohort
  (`milestone_summary` — milestone counts, shares, median/p90 TTM) or
  per cohort × stage (`stage_quantiles`, with `stage_series` for the
  raw day-valued durations behind it). `Stage` / `DEFAULT_STAGES` name
  the `pipeline_stages` duration columns and carry the optional
  `min_seconds` floor (`MAINTAINER_FIRST_SECONDS` = 60 s for the
  stage-2 maintainer-first regime). Because `pipeline_stages` is a pure
  per-PR transform, callers run it **once** over the whole corpus and
  slice the result — not once per cohort.

A nice-to-have upstream change: an explicit `queueboard-core`
ruleset preserving the original `awaiting-review` semantics, so the
choice of ruleset_id encodes "court" rather than living in helper
code.

## Status

The ten notebooks in the [notebook guide](review-analysis/notebooks.md)
are shipped, along with the helper inventory above. One plot-site-polish
item — cross-linking the plot site and the `/notebooks/` landing page for
discoverability — is still planned (tracked under
[`wasm-export.md`](wasm-export.md#remaining-work)). Remaining gaps and
synthesis stories are tracked in
[`backlog.md`](review-analysis/backlog.md): gaps ship autonomously, and
synthesis stories pause for review on completion. Per-session provenance
lives in git history.

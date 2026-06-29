# Review Analysis — Backlog

Remaining work, in rough priority order. Picking up something new? Start
here, then see the [notebook guide](notebooks.md) for what already exists
and the [plan index](../review-analysis-plan.md) for background and the
cross-cutting helper inventory.

Two kinds of work:

- **Gaps** — small/medium follow-ups that close a specific blind spot.
  Ship autonomously (helper + notebook cells + tests); pause for review
  only if a finding reframes downstream work.
- **Stories** — synthesis notebooks combining helpers across notebooks
  into a single narrative. Pause for review after each one.

## Gaps

1. **Reviewer cohort survival / churn** — of reviewers active in year X,
   what fraction are still active in year Y? `reviewer_load.py` has trend
   totals but no retention curves.
2. **Reviewer specialization** — entropy of areas-per-reviewer, top-3
   area share, generalists vs. area-locked. `area_health.py`'s bipartite
   matrix has the raw data; this needs summary metrics.
3. **Force-push impact** — `HEAD_FORCE_PUSHED` is in the event stream but
   unused. Does a force-push reset reviewer attention (next-touch
   latency)? Per-PR penalty.
4. **PR dependency graph** — `analyzer_prdependency` /
   `analyzer_prdependencystate` parquets are exported but untouched.
   Chains of stuck PRs, fan-in/out, cascading unlocks. Larger scope (new
   tables); split if it overflows one session.
5. **CI failures deep-dive** — `syncer_commitcheckrun` /
   `syncer_commitstatuscontext` parquets, deliberately punted in
   `bottleneck_localization.py`. Which checks fail most, time-to-red→green,
   failure clustering. Larger scope.

## Stories

Order TBD; current best guess listed first. (Story A — anatomy of a merge
— shipped as `marimo/anatomy_of_a_merge.py`.)

- **B. Where does latency hide** — stacked decomposition of TTM into
  (author-court / reviewer-court / approved-but-stuck / bors-queue)
  seconds per PR. Builds on `review_state_machine.py` +
  `bottleneck_localization.py` + the queue-window helper. Owns the deeper
  court-vs-author split of the first-touch → MM stage that
  `anatomy_of_a_merge.py` left as a single delta.
- **C. Anatomy of a stuck PR** — operational dashboard for the currently
  open backlog, classifying each PR by *why* it's stuck (CI red, awaiting
  author, merge conflict, no review yet, in bors loop). Complements
  `bottleneck_localization.py`'s stuck table with structured causes.
- **D. Newcomer experience** — first-time-author cohort × area × reviewer
  attribution. Note: first-touch analysis refuted "newcomers wait longer
  for first touch" (first-timers actually get *faster* median first
  touch), so this is reframed onto the downstream funnel —
  reviewed-rate, merge-rate, cycle counts — where the real gap lives.
- **E. Sustainability** — reviewer bus-factor trend + cohort survival
  (gap 1) + area-coverage decline. Is the project's reviewer bench
  renewing itself?
- **F. Bors queue health** — bors-specific deep-dive: queue length over
  time, retry cycles, close-reason mix, `cycle_index` trends. The helpers
  exist; deserves its own notebook.

## Notes

- New helpers land in `qb_notebook/review_states.py` (or
  `qb_notebook/pr_shape.py` for per-PR decorators); when a helper gets
  reused across notebooks, add a bullet to the
  [cross-cutting infrastructure](../review-analysis-plan.md#cross-cutting-infrastructure)
  section of the plan index.
- When a gap or story ships, record it in the
  [notebook guide](notebooks.md) (new entry or findings under the
  relevant notebook) and prune it from this list. Per-session provenance
  lives in git history.

# Review Analysis — Backlog

Prioritized future work. Picking up new work? Start here.

A survey after the five themes + Sessions 6-11 surfaced two buckets:

- **Gaps**: small/medium follow-ups that close a specific blind spot.
  Ship through autonomously; pause for review only if a major design
  question arises.
- **Stories**: synthesis notebooks that combine helpers across themes
  into a single narrative. Pause for review after each story before
  the next ships.

When a gap or story ships, move its detailed write-up to
[`sessions.md`](sessions.md) (as a `Session N — ... — shipped` entry)
and prune the bullet here to a one-line reference.

See [plan index](../review-analysis-plan.md) for project background,
cross-cutting helpers already in place, and the master roadmap;
[`themes.md`](themes.md) for the original Themes 1-5.

## Gaps

In rough priority order (highest leverage / smallest first):

1. ~~First-touch latency~~ — **shipped as Session 11**
   ([`sessions.md`](sessions.md#session-11--first-touch-latency-gap--shipped)).
2. **Inline-comment review-depth signal** —
   `syncer_prreviewinlinecomment.parquet` is loaded by `data_io` but no
   theme touches it. Per-PR comment volume, cuts by `lines_bucket` /
   `pr_type`, correlation with TTM and ping-pong. Only proxy for
   *substantive* review the dataset has (bodies aren't exported).
3. **Approval-source disagreement** — `prs.approvals` (GitHub-native)
   has ~52 % coverage on maintainer-merged PRs. Characterize the
   non-overlap: native approvals without `maintainer-merge` label, and
   vice versa. Sanity-check on the attribution heuristic.
4. **Reviewer cohort survival / churn** — of reviewers active in year
   X, what fraction still active in year Y? Theme 2 has trend totals
   but no retention curves.
5. **Reviewer specialization** — entropy of areas-per-reviewer, top-3
   area share per reviewer, generalists vs. area-locked. Theme 4's
   bipartite matrix has the raw data; needs summary metrics.
6. **Force-push impact** — `HEAD_FORCE_PUSHED` is in the event stream
   but unused. Does a force-push reset reviewer attention (next-touch
   latency)? Per-PR penalty.
7. **Delegated-merge path** — the `delegated` label is in the workflow
   list but no theme analyzes it. Volume, who delegates, latency vs.
   the standard `maintainer-merge` route.
8. **Time-of-day / timezone patterns** — global community, no
   time-of-day analysis yet. Review-desert windows, weekend latency,
   author/reviewer activity-window correlations.
9. **PR dependency graph** — `analyzer_prdependency` /
   `analyzer_prdependencystate` parquets are exported but untouched
   here. Chains of stuck PRs, fan-in/fan-out, cascading unlocks.
   Larger scope (new tables); split if it overflows one session.
10. **CI failures deep-dive** — `syncer_commitcheckrun` /
    `syncer_commitstatuscontext` parquets, deliberately punted in
    Theme 3. Which checks fail most, time-to-red→green, failure
    clustering. Larger scope.

## Stories

Synthesis notebooks; each pauses for review before the next starts.
Order TBD — current best guess listed first.

- **A. Anatomy of a merge** — end-to-end lifecycle waterfall: open →
  first-touch → first `maintainer-merge` → bors queue → merge. Median
  / p90 per stage + fraction of total time each stage owns. The
  best front-page chart; ties Themes 1+2+3 + first-touch gap.
- **B. Where does latency hide** — stacked decomposition of TTM into
  (author-court / reviewer-court / approved-but-stuck / bors-queue)
  seconds per PR. Themes 1+3 + the queue-window helper.
- **C. Anatomy of a stuck PR** — operational dashboard for the
  currently-open backlog, classifying each PR by *why* it's stuck
  (CI red, awaiting author, merge conflict, no review yet, in bors
  loop). Complements Theme 3's stuck table with structured causes.
- **D. Newcomer experience** — Theme-5 first-time-author cohort ×
  Theme-4 area × Theme-2 reviewer attribution. Note: Session 11
  refuted the "newcomers wait longer for first touch" framing
  (first-timers actually get *faster* median first touch). Reframed
  to focus on the downstream funnel — reviewed-rate, merge-rate,
  cycle counts — where Theme 5 already saw the real gap.
- **E. Sustainability** — reviewer bus factor trend (Theme 2) +
  cohort survival (gap 4) + area coverage decline (Theme 4). Is the
  project's reviewer bench renewing itself?
- **F. Bors queue health** — bors-specific deep-dive: queue length
  over time, retry cycles, close-reason mix, cycle_index trends.
  Theme 3 has the helpers; deserves its own notebook.

## Workflow notes

- **Gaps** are autonomous: spin one out, ship the helper + notebook
  cells + tests, record empirical findings in [`sessions.md`](sessions.md)
  under a new `Session N` entry, and move on. Only pause for review if
  a finding meaningfully reframes a downstream gap or story (e.g.
  Session 11's first-touch result reframed Story D).
- **Stories** pause for review on completion. The synthesis design
  involves more aesthetic / framing decisions than a typical gap, so
  treat it as a checkpoint.
- New helpers land in `qb_notebook/review_states.py` (or
  `qb_notebook/pr_shape.py` for per-PR decorators); the
  [Cross-cutting infrastructure](../review-analysis-plan.md#cross-cutting-infrastructure)
  section of the plan index gets a new bullet when a helper is reused
  across multiple sessions.

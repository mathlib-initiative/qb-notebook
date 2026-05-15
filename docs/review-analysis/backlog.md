# Review Analysis — Backlog

Prioritized future work. Picking up new work? Start here.

A survey after the five themes + Sessions 6-12 surfaced two buckets:

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
2. ~~Inline-comment review-depth signal~~ — **shipped as Session 12**
   ([`sessions.md`](sessions.md#session-12--inline-comment-review-depth-gap--shipped)).
   Helper `inline_comment_stats` + Section 9 cells in
   `bottleneck_localization.py`. Inline-comment volume × approved-to-merge
   latency has a clean ~5× monotonic gradient (0 → 11+ comments-by-others).
3. ~~Approval-source disagreement~~ — **dropped**. `prs.approvals`
   (GitHub-native) is used inconsistently in mathlib4 (the maintainer
   workflow leans on `maintainer merge` comments rather than the GitHub
   Approve button), so the ~48 % non-overlap on maintainer-merged PRs
   is dominated by workflow preference rather than heuristic error.
   It can't function as a sanity check on `attribute_label_events`.
4. **Reviewer cohort survival / churn** — of reviewers active in year
   X, what fraction still active in year Y? Theme 2 has trend totals
   but no retention curves.
5. **Reviewer specialization** — entropy of areas-per-reviewer, top-3
   area share per reviewer, generalists vs. area-locked. Theme 4's
   bipartite matrix has the raw data; needs summary metrics.
6. **Force-push impact** — `HEAD_FORCE_PUSHED` is in the event stream
   but unused. Does a force-push reset reviewer attention (next-touch
   latency)? Per-PR penalty.
7. ~~Delegated-merge path~~ — **shipped as Session 13**
   ([`sessions.md`](sessions.md#session-13--delegated-merge-path-gap--shipped)).
   New Section 8 in `marimo/review_state_machine.py`. Headline:
   delegated path is ~40 % faster at the median and ~45 % faster at
   p90 than the maintainer-merge → bors route; 76 % of delegated PRs
   are author-self-merged. Bonus: fixes a `DEFAULT_BOT_ACTORS` bug
   where `mathlib-bors` / `bors` / `leanprover-radar` weren't excluded,
   which had been mis-attributing ~98 % of `delegated` and ~1 % of
   `ready-to-merge` events.
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

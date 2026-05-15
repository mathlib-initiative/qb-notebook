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
8. ~~Time-of-day / timezone patterns~~ — **shipped as Session 14**
   ([`sessions.md`](sessions.md#session-14--temporal-patterns-gap--shipped)).
   New `qb_notebook/temporal.py` helpers + `marimo/temporal_patterns.py`.
   Headlines: ~2× weekend-open first-touch penalty at the median;
   Fri-evening opens 3× slower than Mon-morning at the median;
   **Nov-Dec are 1.8-2.2× busier than the uniform month** (peak,
   not dip) while April is the trough at 0.67×; **author × reviewer
   TZ overlap does NOT predict first-touch latency** (medians flat
   across all overlap buckets) — reframes any future "timezone gap"
   framing in stories B/D.
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

- ~~**A. Anatomy of a merge**~~ — **shipped as Session 15**
  ([`sessions.md`](sessions.md#session-15--story-a-anatomy-of-a-merge--shipped)).
  New `pipeline_stages` helper + `marimo/anatomy_of_a_merge.py`
  (plotly Sankey + 4-panel log-binned stage histograms + slices by
  pr_type / lines / topic / cohort + path classification).
  Headlines: post-MM merged PRs have median TTM **2.28d** (p90
  37.61d); open→first-touch owns ~62 % of summed median TTM;
  **71 % of merges skip the maintainer-merge label** (bors r+
  directly); 27 % of merges go via the delegated path.
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

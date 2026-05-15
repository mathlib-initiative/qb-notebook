# Review Analysis — Sessions 6+

Cleanups, cross-cuts, and gap fills shipped after the five original
themes (which live in [`themes.md`](themes.md)). Append-only: each
shipped session adds an entry here. Planned-but-not-shipped sessions
are tracked in [`backlog.md`](backlog.md); when a session ships, its
detail moves here.

See the [plan index](../review-analysis-plan.md) for project background,
cross-cutting helper inventory, and the master roadmap.

## Post-Theme-5 cleanup pass overview

After all five themes shipped, a survey across the codebase surfaced
consolidation opportunities and cross-cuts that earlier themes can borrow
from later ones. Tracked separately so they can be picked up incrementally.

## Session 6 — boilerplate consolidation — shipped

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

## Session 7 — Theme-4 follow-up: team-annotated reviewer × area matrix — shipped

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

## Session 8 — cross-cuts: shape and area effects on review state — shipped

Earlier themes ran before `pr_shape` and `labels_active_at` shipped.
Session 8 retrofits those helpers back into Themes 1 and 3 by
decorating each notebook's per-PR frame once at the top, then adding
faceted sections that don't disturb the existing plots.

**Shared decorator pattern** (mirrors `marimo/pr_shape_effects.py`):
join `lines_bucket` and `pr_type` onto the per-PR analysis frame via
`size_buckets(prs)` / `pr_type(prs)`, then group-by in the new cells.
No new helper functions needed — everything wires up from existing
`qb_notebook/pr_shape.py` and `qb_notebook.review_states`.

- **Theme 1 sojourn × shape**:
  `marimo/review_state_machine.py` and `marimo/queue_window_state.py`.
  Decorate the `intervals` frame with `lines_bucket` / `pr_type`
  (left-join on `pull_request_id`). New cells:
  - Sojourn boxplot by `lines_bucket`, one panel per state label
    (label-based notebook) or one panel total (queue-window
    companion).
  - Sojourn boxplot by `pr_type` (canonical types only, drop
    `unparsed` to keep the axis readable).
  - Ping-pong cycle-count distribution by `lines_bucket` and by
    `pr_type` — same shape as the existing ping-pong histogram cell,
    just grouped. For the queue-window notebook this is
    `max(cycle_index) per PR` instead of label cycles.
  Questions the cuts should answer: does `feat:` ping-pong more than
  `chore:`? Are 1000+-line PRs in `awaiting-review` longer? Open the
  notebook to the relevant cell at end of session and record headline
  numbers in the notes block (same convention as Sessions 5/9).
- **Theme 3 stall signals × shape**:
  `marimo/bottleneck_localization.py`. Decorate the `first_mm` /
  `flags` join with `lines_bucket` and `pr_type`. New cells:
  - Stall-signal prevalence table by `lines_bucket` (rows = size
    bucket, columns = `had_merge_conflict` / `had_awaiting_author` /
    `had_awaiting_CI` / `had_ready_to_merge`, cells = prevalence %
    + n).
  - Same table by `pr_type`.
  - Approved-to-merge latency boxplot faceted by `lines_bucket`
    (existing latency cell repeated with a group-by).
- **Theme 3 stall signals × area**:
  Continuing in `marimo/bottleneck_localization.py`. Reconstruct
  `t_intervals` via `label_intervals(events, t_label)` for the 24
  `t-*` labels (same as `area_health.py`), then attribute each
  `first_mm_at` point to its active areas with
  `labels_active_at(t_intervals, first_mm_pts)`. PRs with multiple
  active `t-*` labels contribute to each — same count-each semantics
  as Theme 4. New cells:
  - Stall-signal prevalence × area table (areas as rows, signals as
    columns, sorted by total approved-PR volume).
  - Per-area median / p90 `mm_to_merge_days` (conditional latency
    table — drop areas with n < 20 from the headline).
  Expected output: surface whether merge-conflicts cluster in any
  `t-*` area (analysis-heavy areas like `t-algebra` vs. infrastructure
  areas like `t-meta`).

**Out of scope for Session 8**: extending `area_health.py` with shape
cuts. Area × shape is a natural next step but doubles the surface; if
the Session 8 findings make it interesting, spin out as Session 8b.

**Empirical findings on the current artifact**:

- **Sojourn × size** (label-based, `awaiting-review`): median 0.37d
  (0-10 lines) → 0.89d (51-200) → 0.79d (1001+). A real but modest
  size effect — the 1001+ bucket doesn't dominate the way it does for
  end-to-end TTM. Reviewers spend disproportionately more time on
  mid-sized PRs (51-200) than on the very largest.
- **Ping-pong × size** (label-based, `awaiting-review` cycles per
  PR): `share_multi_cycle` 5 % (0-10) → 12 % (11-50) → 20-22 %
  (51+). Bigger PRs ping-pong ~4× as often, as expected.
- **Stall signals × size** (Theme 3): merge-conflict prevalence
  climbs 0.7 % → 1.6 % → 4.0 % → 7.2 % → 11.2 % across the size
  buckets — a clean 16× gradient that confirms merge-conflict is the
  dominant size-correlated stall. `awaiting-CI` prevalence is flat
  (~2.4 % across all sizes) so it's *not* a size-driven signal.
- **Stall signals × area** (Theme 3): `t-algebra` dominates volume
  (n=1937, 27 % of approved-and-merged cohort); per-area
  merge-conflict shares range 2.0–3.9 % (n ≥ 20) — no single area is
  a runaway outlier. Per-area median `mm→merge` is highest in
  `t-meta` (0.72d) — meta-infra changes evidently carry more
  bors-queue overhead than the math content areas.
- **Queue-cycle × size** (queue-window companion): `share_multi`
  (PRs with `cycle_index > 0`) climbs 18 % (0-10) → 59 % (201-1000)
  then drops back to 44 % for 1001+. Same long-tail "large refactor
  PRs get fast-tracked" pattern Theme 5 surfaced for raw TTM.

## Session 9 — Theme-2 active-reviewer trend × team + Theme-5 follow-ups — shipped

### Active-reviewer trend split by team

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

### Theme-5 follow-ups

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

## Session 10 — plot-site polish — planned

The 6 marimo notebooks build their plots as inline `plt.subplots()`
cells. `qb_notebook/generate_plot_site.py` expects
`render_*(ctx) -> Figure` functions registered via `PlotDefinition`.
Promotion requires each notebook's headline plots extracted into a
renderer module (e.g. `qb_notebook/plotting/theme_4.py`) and
registered. Doing one notebook end-to-end first will set the pattern
for the rest.

## Session 11 — first-touch latency (gap) — shipped

**Question**: How long does an author wait before *any* reviewer (or
other non-bot non-author commenter) engages with their PR? Theme 2
measured the *closing* trigger; this is the metric authors actually
feel — and the natural newcomer-experience proxy.

**Approach**: Add `first_review_touch(df_prs, df_events, *,
event_types=..., asof=None)` to `qb_notebook/review_states.py`. For
each PR, return the earliest event matching `event_types` whose actor
is neither the PR author nor a known bot. Default `event_types`
covers `REVIEW_APPROVED` / `REVIEW_COMMENTED` /
`REVIEW_CHANGES_REQUESTED` / `REVIEW_DISMISSED` / `ISSUE_COMMENTED`;
callers can drop `ISSUE_COMMENTED` for a stricter "substantive review"
variant. Returns one row per PR with `first_touch_at`,
`first_touch_actor`, `first_touch_event_type`,
`first_touch_seconds_from_open`. PRs with no qualifying event get
nulls.

The post-#164 ingest is what makes this newly possible (REVIEW_* /
ISSUE_COMMENTED weren't exported before 2026-05). PRs whose first
touch would have predated the ingest cutover are dropped from the
headline distribution; the coverage cell calls this out.

**Plots / metrics** (added to `marimo/reviewer_load.py` as a new
section after the attribution-coverage cells):

- Coverage cell: total PRs eligible for the metric vs. PRs with a
  qualifying first touch vs. PRs that closed/merged without one.
- Distribution of `first_touch_seconds_from_open` — overall, then a
  ≤7d zoom; report median / p50 / p90 headline.
- Two variants side-by-side: `ISSUE_COMMENTED`-allowed (broad) vs
  `REVIEW_*`-only (substantive). Quantify the gap.
- Monthly median / p90 trend over `gh_created_at` cohorts.
- Cuts by `lines_bucket`, `pr_type`, `is_first_pr` via the
  `qb_notebook.pr_shape` decorator pattern from Session 8.
- Per-area latency (recent 12mo): attribute each PR-open point to its
  active `t-*` area via `labels_active_at` on `label_intervals`.

**Data**: `prs` (`gh_created_at`, `author_id`/`author_login`),
`events` (post-#164 review/comment events), `label_defs`, plus
`pr_shape` and `labels_active_at` helpers for cuts.

**Output**: `first_review_touch` helper + 5-6 new cells in
`marimo/reviewer_load.py`. Unit tests in `tests/test_review_states.py`
covering bot exclusion, author exclusion, multi-event first-wins,
no-event PRs, per-PR isolation, and the `event_types` parameter.

**Open questions**:
- Author comments on their own PR — excluded as a touch (we want
  external engagement).
- Re-opened PRs: first touch is keyed off `gh_created_at` rather than
  the last `REOPENED`; the second-life latency is a future variant.
- ~~Pre-#164 cohort right-censoring~~ — resolved: the upstream syncer
  backfilled REVIEW_* / ISSUE_COMMENTED events all the way to 2021-05,
  so there's no censoring window. Coverage is uniformly high.

**Notes from implementation**:

- **Coverage** is high: **93.4 %** of all 38.6k PRs in the snapshot
  have a broad touch (REVIEW_* or ISSUE_COMMENTED by a non-author
  non-bot); **63.5 %** have a strict (REVIEW_*-only) touch. Only
  **0.5 %** merged without any broad touch (likely maintainer
  self-merges or fast-path infra changes); **4.3 %** are
  closed-unmerged with no touch (authors abandoned before anyone
  engaged).
- **Broad headline** (n=36 080): median **9.1 h**, p75 **67.6 h** (~2.8d),
  p90 **325.7 h** (~13.6d), p99 **2 757 h** (~115d).
- **Strict headline** (n=24 533): median **15.6 h**, p75 **115.2 h**
  (~4.8d), p90 **479 h** (~20d), p99 **2 918 h** (~122d). Roughly
  ~6h slower at the median, because dropping ISSUE_COMMENTED loses a
  lot of fast "drive-by" first-touch chatter.
- **Size effect**: median broad touch climbs **2.9 h** (0-10 lines) →
  7.4 h (11-50) → 14.0 h (51-200) → **18.5 h** (201-1000) →
  16.0 h (1001+). Same "1001+ slightly faster than 201-1000"
  inversion as Theme 5's TTM gradient — large refactor PRs really do
  get fast-tracked at the *first-touch* stage too, not just at
  merge time.
- **PR-type effect** is dramatic: `perf:` at **0.85 h** median is
  fastest by far; `feat:` at **18.8 h** is slowest, **~22× slower**.
  `chore:` (n=11.5k, second-largest cohort after `feat:`) lands at
  4.2 h. The `feat:` long tail (p90 ≈ 22d) drives most of the
  project's first-touch p90.
- **First-time-author surprise**: median broad touch is **7.77 h**
  for first-time-authors (n=769) vs **9.13 h** for returning
  (n=35 293) — first-timers actually get **faster** first touch at
  the median, p90 also marginally better (287 h vs 327 h). The
  "newcomers wait longer" hypothesis from the Stories backlog does
  *not* hold for first-touch; if there's a newcomer disadvantage
  it must live downstream (reviewed-rate, merge-rate, time-to-close
  cycles) — which Theme 5 already documented. **Updates the
  newcomer-experience story (D) framing** to focus on the funnel
  beyond first-touch.

## Session 12 — inline-comment review depth (gap) — shipped

**Question**: `syncer_prreviewinlinecomment.parquet` is the only
*substantive* review signal in the dataset (bodies aren't exported,
but per-PR counts of inline comments + distinct reviewers + threads
proxy how much back-and-forth a PR generated). Does inline-review
depth correlate with the approved-to-merge latency tail and with
shape (`lines_bucket` / `pr_type`)?

**Helper**: `inline_comment_stats(df_inline, df_prs, *, bot_actors,
pr_id_col, pr_author_col)` in `qb_notebook/review_states.py`. Returns
one row per PR with at least one inline comment, with columns
`n_inline_comments`, `n_inline_comments_by_others` (excludes the PR
author + known bots — the headline depth signal), `n_inline_threads`
(distinct `thread_root_node_id`), `n_inline_thread_replies`,
`n_inline_authors`, `n_inline_files`, `first_inline_at`,
`last_inline_at`. PRs with no inline comments are absent — left-join
+ `fill_null(0)` to attach. Follows the `df_prs + author exclusion`
pattern of `first_review_touch`. Five unit tests cover thread
counting, author/bot exclusion (case-insensitive), no-comments
absence, multi-PR grouping, and the orphan-PR-no-author edge case.

**Wired into `marimo/bottleneck_localization.py`** as a new
**Section 9 — Inline-comment review depth**, restricted to the
approved-and-merged cohort (`first_mm` from Section 1) so the
latency cuts are directly comparable to the existing stall-signal
sections:

1. Coverage + headline-distribution summary table.
2. `comment_bucket` (0 / 1-2 / 3-5 / 6-10 / 11+) × `mm_to_merge_days`
   median / p75 / p90 table + boxplot (≤14d clip).
3. Cycles-vs-comments table joining the Section-3 `cohort_bounces`
   frame — does ping-pong correlate with deeper inline review?
4. Comment volume × `lines_bucket` (median / p90 / share-any /
   median reviewers).
5. Comment volume × `pr_type` (same shape, includes
   `other`/`unparsed`).

The data-load cell now also joins `core_user.github_login` onto
`prs.author_id` (matching `marimo/reviewer_load.py`) so the helper
can exclude self-comments. `inline_comments` is loaded optionally
(`data.get("inline_comments")`); the Section 9 cells degrade
gracefully when the parquet file isn't present in the artifact.

**Empirical findings on the current artifact** (approved & merged
cohort, n=8 053; ~51.6 % had at least one by-others inline
comment):

- **Headline distribution**: median 1 comment-by-others, p75 3, p90
  10, max 111. Median 1 distinct reviewer per PR; p90 = 2 reviewers.
  Inline review is sparse — most approved PRs get nothing on the
  diff, and the headline metric is dominated by the long tail.
- **Latency × comments — the headline result**: clean ~5×
  monotonic gradient.

  | comments | n    | median mm→merge (d) | p90 (d) |
  | -------- | ---- | ------------------- | ------- |
  | 0        | 3899 | 0.18                | 2.76    |
  | 1-2      | 1815 | 0.42                | 4.14    |
  | 3-5      | 932  | 0.54                | 4.87    |
  | 6-10     | 659  | 0.77                | 7.11    |
  | 11+      | 748  | 0.92                | 13.00   |

  Inline-comment volume is a substantial predictor of the
  approved-to-merge tail — heavier inline-review PRs sit in the
  bors queue ~5× longer at both the median and the p90.
- **Ping-pong correlation**: median comments-by-others climbs
  0 (queue_cycles ≤ 1) → 1 (cycles=2) → 2 (cycles=3) → 4.5
  (cycles=4) → 8 (cycles=5). Heavier bors-queue bouncing is
  strongly correlated with deeper inline review, as expected — the
  two are likely both reflecting the same underlying "this PR needs
  another pass" signal.
- **Shape × comments — `lines_bucket`**: share-any-inline climbs
  22 % (0-10) → 45 % (11-50) → 66 % (51-200) → **73 % (201-1000)**
  → 63 % (1001+). Same "1001+ slightly lower than 201-1000"
  inversion as TTM / first-touch. Median comments-by-others:
  0 / 0 / 2 / 4 / 1 — the 1001+ median is surprisingly low, again
  consistent with "very-large PRs get fast-tracked".
- **Shape × comments — `pr_type`**: `feat:` is the clear outlier —
  median 2 comments-by-others, p90 15, share-any-inline **68 %**
  (n=4 183). Housekeeping types are uniformly low: `chore:`
  median 0 / share-any 31 %, `fix:` median 0 / 29 %, `doc:`
  median 0 / 32 %. `refactor:` median 1 / 55 % sits between.
  `feat:` PRs attract ~2-3× more inline review than `chore:`,
  matching Session 11's first-touch finding that `feat:` is the
  slowest type by ~22×.

**Notes / open follow-ups**:

- Section 9 lives on the approved-and-merged cohort; the helper
  itself returns one row per PR regardless of state, so a future
  session could trivially run the same cuts on the open-PR backlog
  or on closed-unmerged PRs (the latter is interesting for Story C).
- "Files touched by inline comments" (`n_inline_files`) is exposed
  but unused in the current cells; potentially useful as a
  diff-coverage signal in Story A (anatomy of a merge).
- Inline comments coverage backfills to 2021-06-03, so there's no
  censoring window — the gradient above is robust over the full
  history.

## Session 13 — delegated-merge path (gap) — shipped

**Question**: The `delegated` label grants the PR's author permission to
trigger the bors merge themselves. It pre-dates `maintainer-merge` (the
label was introduced 2022-07; `maintainer-merge` 2024-02-15) and runs as
a parallel sign-off pathway. None of Themes 1–5 looked at it directly.
Volume, who delegates, and latency vs the standard
`maintainer-merge → bors` route?

**Helper fix**: `DEFAULT_BOT_ACTORS` in
`qb_notebook/review_states.py` was missing the bors-family bot accounts
(`mathlib-bors`, `bors`, `leanprover-radar`). Those bots post replies to
`bors r+` / `bors delegate=...` commands seconds before the corresponding
`ready-to-merge` / `delegated` label is applied, so
`attribute_label_events` was crediting them as the human "trigger" for
~98 % of `delegated` events (the top "delegators" were `mathlib-bors` at
6 670 and `bors` at 1 340) and for ~1 % of `ready-to-merge` events.
Added them to the default set with a comment explaining the rationale; a
new unit test (`test_attribute_skips_bors_family_bots`) pins the
behaviour. Coverage on the other labels is unaffected:
`maintainer-merge` attribution moved by 2 events out of 8 392.

**Notebook**: New `## 8. Delegated-merge path` in
`marimo/review_state_machine.py` — cohort × era table, monthly
LABELED-volume trend (with mm-cutover marker), attribution coverage,
top-15 delegators (team-annotated when the sibling
`leanprover-community.github.io` checkout is present), sign-off → merge
latency table + ≤7d histogram, and an author self-merge rate cell that
joins `ready-to-merge` attribution back to each delegated PR's author.

**Empirical findings on the current artifact**:

- **Volume and cohorts**: 8 131 PRs ever delegated vs 8 255 with
  `maintainer-merge`. The two paths run in **parallel post-cutover**,
  not sequentially: of the 6 184 post-cutover delegated PRs,
  **4 441 (72 %) are `delegated_only`** (no `maintainer-merge` label
  ever applied). Only 1 743 (28 %) have both labels, and when both are
  present `maintainer-merge` lands first (median 3.7 h before
  `delegated`). Pre-cutover all 1 947 delegated PRs are by definition
  `delegated_only`. So delegated isn't a niche legacy mechanism —
  it's the workflow path for ~40 % of all post-cutover sign-offs.
- **Attribution coverage**: 98.8 % with the fixed default bot set
  (vs 99.3 % when bors-bots were spuriously credited). Median gap
  human-comment → label apply is **12 s**, p90 **25 s** — tight, same
  shape as `maintainer-merge` attribution.
- **Top delegators** (with bors-bots excluded): `jcommelin` (1 200),
  `riccardobrasca` (1 023), `eric-wieser` (867), `kim-em` (545),
  `j-loreaux` (513), `ocfnash` (511), `sgouezel` (487), `Vierkantor`
  (331). All maintainer-team members; the distribution is more
  concentrated than `maintainer-merge` triggers (one or two people
  delegate a lot, then a steep drop).
- **Sign-off → merge latency** (post-cutover, days):

  | path             | n     | median | p75   | p90   |
  | ---------------- | ----- | ------ | ----- | ----- |
  | delegated        | 6 026 | 0.210  | 0.649 | 2.387 |
  | maintainer-merge | 8 053 | 0.349  | 1.253 | 4.279 |

  **Delegated path is ~40 % faster at the median and ~45 % faster at
  p90** than the maintainer-merge → bors route. Likely two effects: the
  author is sitting in front of the PR and triggers bors immediately
  once delegated, whereas mm-sign-off PRs wait for *another* maintainer
  to come along and `bors r+`. Also, "both labels" PRs sit ~3× longer
  than `mm_only` (median mm→merge 0.74 d vs 0.26 d) — they're the harder
  PRs that needed a second pass.
- **Author self-merge rate**: of the 2 894 delegated PRs that
  subsequently received a `ready-to-merge` attribution, **76.4 %** had
  the **PR author** themselves as the bors trigger. The other 23.6 % are
  cases where someone else (typically another maintainer) ended up
  pushing the button — usually because the author didn't come back to
  the PR for a while. So delegation actually delivers on its intent for
  three out of four PRs.

**Notes / open follow-ups**:

- The post-cutover `mm_only` cohort overlaps significantly with the
  `delegated_only` cohort by PR shape — both are dominated by the
  smaller `chore:` / `doc:` / `fix:` types. Per-shape cuts (latency by
  `lines_bucket` × path, `pr_type` × path) are a natural next step but
  out of scope for this gap; Story B (where does latency hide) is the
  natural home.
- Session 13 also fixes the bors-bot attribution bug in
  `DEFAULT_BOT_ACTORS`. The few percent of legacy `ready-to-merge`
  attributions that previously credited `mathlib-bors` / `bors` /
  `leanprover-radar` now correctly fall through to the human comment;
  re-running `marimo/reviewer_load.py` will show those bots dropping
  out of the per-reviewer table (they previously appeared with
  triple-digit "triggers" counts on the bors-trigger chart).

## Session 14 — temporal patterns (gap) — shipped

**Question**: Mathlib4 is a global community on a UTC-stamped event
stream, but no prior session looked at *when* things happen. Are
there review-desert hours? Does opening a PR on a Friday afternoon
cost a weekend? Does the project run with seasonal rhythm (holidays,
summer slowdowns)? Do author/reviewer timezone alignment differences
show up in first-touch latency? Backlog gap #8, extended to also
cover seasonality.

**Helpers** (new file `qb_notebook/temporal.py`, mirrors the
`pr_shape.py` decorator pattern):

- `with_temporal_columns(df, ts_col, *, prefix="")` — append UTC
  `hour_utc` / `weekday` (0=Mon, 6=Sun) / `is_weekend` / `month` /
  `year` / `year_month` derived from any datetime column. Pass a
  prefix to decorate two timestamps on the same frame
  (e.g. `"open_"` vs `"merge_"`).
- `weekday_hour_histogram(df, *, ts_col="occurred_at")` — counts per
  UTC (weekday × hour_utc) cell, **always emits all 168 cells**
  zero-filled so heatmap rendering doesn't have to reindex. Caller
  filters first (event types, bots, date window) — keeps the helper
  schema-agnostic.
- `actor_activity_window(events, *, window_hours=8, min_events=20)` —
  per-actor inferred UTC active window. Builds a 24-bin
  hour-of-day histogram per actor, then finds the contiguous
  `window_hours`-length window of max activity (treating the clock
  as circular so windows can wrap midnight). Returns
  `peak_hour`, `window_start`, `window_end`, `active_hours: list[int]`,
  `active_hours_share`. The "best contiguous window" framing maps
  cleanly to "what timezone is this actor in".
- `hour_set_overlap(hours_a, hours_b) -> int` — pairwise intersection
  size between two `active_hours` lists; trivial helper but keeps the
  per-PR overlap cell readable.
- `WEEKDAY_LABELS` / `MONTH_LABELS` / `WEEKEND_DAYS` constants for
  plot axes. Fifteen unit tests cover known-timestamp decoration,
  null propagation, prefix isolation, zero-fill, bot exclusion,
  event-type filtering, min-events filtering, and the
  midnight-wrap circular window case.

**Notebook** (new `marimo/temporal_patterns.py`):

- **§1** — 2×2 hour × weekday heatmaps for **PR opens**, **first
  touches** (broad: REVIEW_* + ISSUE_COMMENTED, Session 11),
  **`maintainer-merge` sign-offs** (LABELED events, used directly —
  `attribute_label_events` would be a few-second shift to the human
  trigger but doesn't change the rhythm), and **bors merges**
  (`merged_at_effective` on merged PRs). Plus a single-figure
  hour-of-day rollup line chart (one normalised line per stream)
  and per-hour / per-weekday summary tables.
- **§2** — first-touch latency cut by the PR's open hour and
  weekday. Heatmap of median first-touch hours, weekday-only
  summary, weekend-vs-weekday open buckets, and an explicit
  "Friday-evening vs Monday-morning" extreme-corner comparison.
- **§3** — monthly seasonality with YoY overlay (one line per year)
  and a trend-removed **seasonal index** (each month's average
  share of its own year × 12; 1.0 = uniform). Computed across all
  four streams. Years with fewer than 6 months of data dropped.
- **§4** — per-actor inferred UTC active windows (`window_hours=8`,
  `min_events=20`), per-actor peak-hour histogram with a rough
  Europe-daytime band overlay, then per-PR
  author × first-reviewer overlap. Median / p90 first-touch latency
  by overlap bucket.

**Empirical findings on the current artifact**:

- **Hour-of-day** (counts marginalised over weekday, ratio to mean):
  - **PR opens** are flat (0.64-1.41× mean range, no desert hours);
    busiest at 14 UTC. Authors push throughout the day.
  - **First touches** peak at 09 UTC (1.46×), three desert hours
    02-04 UTC (<0.5× mean).
  - **`maintainer-merge` labels** are the most concentrated stream:
    peak 09 UTC at **1.84×** mean, **seven desert hours** below
    0.5×. Reviewers sign off in tight European-morning windows.
  - **Bors merges** peak at 15 UTC (1.51×), only one desert hour
    (03 UTC at 0.47×).
- **Weekday rhythm**:
  - Opens are nearly uniform Mon-Fri (15-16 % each day) with **22.9 %
    weekend share**. Authors work weekends too.
  - **`maintainer-merge` peaks on Friday (17.1 %)**, the only stream
    with a clear within-week peak — reviewers clear the queue
    end-of-week.
  - **Bors merges have the lowest weekend share (17.8 %)** — the
    bors queue empties Mon-Fri even when authors push on weekends.
- **First-touch latency by weekday-of-open** (broad, n=35 169):
  median weekday-open **8.55 h** vs weekend-open **16.36 h** — a
  clean **~2× weekend penalty at the median**. p90 is much closer
  (330 h vs 378 h), so the weekend penalty is a "median delay,"
  not a "PR gets stranded" effect.
- **First-touch latency by hour-of-open**: morning opens
  (06-11 UTC) are fastest at median **6.75 h**; evening opens
  (18-23 UTC) are slowest at **12.63 h**. Cleaner is the corner
  comparison: **Fri 17-23 UTC opens** sit at median **17.0 h** /
  p90 **404 h**; **Mon 09-11 UTC opens** at **5.5 h** / p90 **271 h**.
  That's a **~3× median ratio** between the worst and best open
  times.
- **Monthly seasonality** (trend-removed, 4-5 years contributing
  per month):

  | month | opens | touches | merges |
  | ----- | ----- | ------- | ------ |
  | Apr   | 0.66  | 0.68    | 0.67   |
  | Aug   | 1.05  | 1.07    | 1.03   |
  | Nov   | 1.76  | 1.98    | 1.90   |
  | Dec   | **2.23** | **2.19** | **2.24** |

  The headline is the **Nov-Dec peak, not a dip** — the opposite of
  the intuitive holiday hypothesis. mathlib4 has a strong
  end-of-year push (consistent across opens, touches, and merges).
  **April is the trough** at ~0.67× across all streams. There is
  **no clean summer dip** — Aug is roughly average. The
  `maintainer-merge` seasonal index is unreliable (only 2 years of
  data since the label was introduced Feb 2024); it's reported but
  the Nov-Dec finding stands on the other three streams.
- **Activity windows** — 365 actors have an inferred 8-hour window
  (n_events ≥ 20). Top by volume: `kim-em` (peak 23 UTC, window
  22-06 — atypical late-night), `eric-wieser` (22 UTC, 17-01),
  `YaelDillies` (09 UTC, 07-15), `grunweg` (09 UTC, 08-16),
  `jcommelin` (07 UTC, 07-15), `joelriou` (09 UTC, 09-17). Most
  of the maintainer-team population peaks in 07-16 UTC (European
  workday). Peak-hour distribution: **57 %** of actors peak in
  07-16 UTC; the rest spread across evening / overnight UTC slots.
- **Author × first-reviewer overlap × first-touch latency**
  (n=33 827 PRs with windows on both sides): the overlap
  distribution is **bimodal** — a large mass at 0 h overlap (~30 %
  of PRs) and a second mass at 6-7 h overlap (~27 % combined).
  Real timezone segregation exists.
  But **first-touch latency does *not* show a clean monotonic
  relationship with overlap** — medians range 7.8-12.5 h across all
  overlap buckets with no clear trend (overlap=0 h median **10.3 h**,
  overlap=8 h median **10.7 h**). The natural hypothesis "more
  timezone overlap → faster touch" doesn't hold in this dataset.
  Likely because mathlib4 review is a long-running async queue —
  reviewers see a PR when they next sit down at their machine, not
  when the author is also online. **Reframes any future timezone
  story**: distinguishing reviewer pools by TZ is interesting for
  workload distribution, but TZ alignment per-PR is not predictive
  of speed.

**Notes / open follow-ups**:

- **Europe/Berlin clock overlay** on the hour-axis would make the
  heatmaps more interpretable. UTC-only was the agreed scope for
  this gap, so left to a follow-up.
- **Per-actor nominal-TZ inference**: §4 uses the empirical hour
  distribution directly; a clustering step could assign actors to
  TZ bands and run reviewer-pool composition analysis. The §4
  finding above suggests this is interesting for *who has the
  bench* (reviewer-pool TZ mix) rather than for predicting latency.
- **Holiday-calendar overlay**: the trend-removed seasonal index
  would benefit from explicit Christmas / New Year / Easter
  annotations, especially given the Nov-Dec peak is the headline
  surprise. A small `holidays` package integration would do it.
- The `maintainer-merge` seasonal index has only 2 years of data;
  re-run after another full calendar year for a clean result.
- Headline finding that reframes downstream stories: **TZ overlap
  doesn't predict first-touch latency**. Story D (newcomer
  experience) and any future "where does latency hide" story (B)
  should not assume timezone gap is a primary contributor.

## Session 15 — Story A: anatomy of a merge — shipped

First synthesis story. End-to-end PR lifecycle waterfall combining
Themes 1+2+3 and the first-touch / inline-comment / temporal helpers
into a single milestone-funnel notebook.

**Deliverables**:

- New helper `pipeline_stages(df_prs, df_events, *, ...)` in
  `qb_notebook/review_states.py`. Wraps `first_review_touch` +
  `stage_timestamps` into a per-PR wide frame with the four
  sequential stage deltas: open→first-touch, touch→MM, MM→RTM,
  RTM→merged, plus the total open→merged seconds. Non-monotonic
  deltas (e.g. RTM applied before MM) null rather than going
  negative so log-scale histograms stay clean. 6 unit tests cover
  linear merge, closed-unmerged, direct-bors (no MM), non-monotonic
  ordering, empty-events, and `pr_merged_col=None`.
- New `marimo/anatomy_of_a_merge.py` with the following sections:
  - **Intro + cohort dropdown** (post-MM / 2024 / 2025 / all-time)
    + Sankey-branch-by-cycle-count toggle.
  - **Lifecycle Sankey** (plotly — first plotly usage in the marimo
    set). Nodes: opened, touched, [1 / 2 / 3+ queue cycles when
    toggle on], maintainer-merge, bors r+, delegated, merged,
    closed unmerged, still open. Each PR contributes one path; the
    `bors r+` and `delegated` nodes are independent of MM (a PR
    that skips MM still routes through `bors r+` if RTM was
    applied).
  - **Per-stage duration distributions** — 4-panel log-binned
    histograms (`np.logspace`) + lognormal-fit overlay
    (`scipy.stats.lognorm` with `floc=0`). Lognormal binning is the
    `pr_open_durations.ipynb` convention; stage deltas inherit the
    same shape character.
  - **Stage share of TTM** — stacked horizontal bar of the
    summed-medians, with per-stage median / p90 / n table.
  - **Queue-cycle distribution** — histogram of cycles-before-MM
    on merged PRs + median-TTM-by-bucket bar chart.
  - **Slice tables** (median stage durations × slice value):
    `pr_type`, `lines_bucket`, top-10 topic areas, first-PR vs
    returning author. Topic attribution uses `labels_active_at`
    against unclamped `t-*` intervals (the
    `[start, end_effective)` lookup would miss merges whose
    `merged_at_effective` equals `closed_at` if intervals were
    clamped at close time).
  - **Review activity** — 3-panel log-binned histograms of
    non-author non-bot REVIEW_*, ISSUE_COMMENTED, and inline
    comments per merged PR.
  - **Path classification** — counts and TTM medians by
    `bors` / `delegated` / `mm_no_rtm` / `direct` paths.

**Empirical headlines (post-MM cohort, 27 781 PRs)**:

- **Funnel coverage**: 91 % reach first touch, 80 % merge,
  12 % close unmerged, 8 % still open. The **maintainer-merge
  label is only applied on 29 % of cohort PRs** (8 154 / 27 781) —
  most merges skip it. **RTM is applied on 66 %** (18 375), and
  **delegated on ~26 %** of merges. Of merged PRs: **72 % via
  bors r+** (15 864), **27 % via delegated** (5 941), **1.4 % via
  `direct`** (315 — merged with no MM/RTM/delegated), 0.3 % via
  `mm_no_rtm` (56).
- **Stage medians (merged PRs only)**: open→first-touch **0.45d**
  (p90 16.65d), first-touch→MM **0.06d** (p90 21.58d, very heavy
  tail), MM→RTM **0.22d** (p90 3.77d), RTM→merged **0.03d** (p90
  0.10d — bors is fast once queued). Total median open→merged
  **2.28d** (p90 37.61d).
- **Stage share of summed medians at the merged-PR median**:
  open→first-touch dominates at ~62 % of summed medians; the
  three downstream stages share the remaining ~38 %. The story
  matches Theme 3's finding that **reviewer attention onset is the
  biggest single contributor to median TTM**, not the bors queue.
- **Queue cycles**: median merged PR reaches MM in **1 queue
  cycle**; cycle count is a useful gating signal — 1-cycle PRs
  have much lower median touch→MM latency than 3+-cycle PRs.
- **Topic-area coverage**: 76.7 % of merged PRs have an active
  `t-*` label at merge time. Top areas in the cohort:
  `t-algebra` (4 851), `t-category-theory` (2 113),
  `t-analysis` (1 783), `t-topology` (1 418), `t-data` (1 253).

**Conventions established**:

- Log-binned histograms with lognormal-fit overlay are the
  preferred shape for stage-duration distributions. The
  `np.logspace(np.log10(lo), np.log10(hi), bins+1)` recipe
  matches `pr_open_durations.ipynb`.
- For merge-time topic-area attribution via `labels_active_at`,
  **do not pass `df_pr_close`** — the half-open
  `[start, end_effective)` lookup misses the merge instant when
  `merged_at_effective == closed_at`. Convention: leave intervals
  open through `asof`, mirroring `area_health.py`.
- Plotly Sankey is the canonical choice for lifecycle flow viz.
  `go.Figure(...)` renders directly as a marimo cell output; no
  wrapper needed.

**Notes / follow-ups**:

- Headline that **most merges skip MM** (`bors` path with no prior
  MM is 9 287 PRs vs 6 231 with MM in the cohort) is an
  interesting reframe for any "MM as universal sign-off" framing.
  Worth flagging in Theme 2 / reviewer load follow-ups.
- The TTM stage share above is at the **median**; at p90 the bors
  queue contributes more (still-modest tails on RTM→merged) and
  touch→MM blows up further. A p90-share variant is a small follow-up.
- Story B (latency decomposition) owns the deeper court-vs-author
  split of the `first-touch → MM` stage. We exposed only queue-cycle
  count as the queue-aware signal here.

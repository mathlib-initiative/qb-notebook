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

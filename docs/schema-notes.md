# Schema Notes

`qb_notebook.data_io.DEFAULT_DATETIME_COLUMNS` is a practical default for current queueboard parquet exports. It is not intended to represent all possible analytics schemas.

## Current default datetime columns
- `created_at`
- `updated_at`
- `gh_created_at`
- `gh_updated_at`
- `last_synced_at`
- `commits_earliest_synced_at`
- `engagement_synced_at`
- `occurred_at`
- `closed_at`
- `merged_at`
- `from_ts`
- `to_ts`
- `first_on_queue_ts`

## Maintenance policy
- If queueboard exports add/rename datetime fields, update `DEFAULT_DATETIME_COLUMNS`.
- Keep parser behavior backward-compatible: missing configured columns should continue to be ignored.
- For non-queueboard datasets, callers should pass `datetime_columns=` directly instead of mutating global defaults.

## Checklist when defaults change
1. Update the tuple in `qb_notebook/data_io.py`.
2. Update this file if semantics changed.
3. Update tests in `tests/test_data_io.py`.

## Actor typing on `syncer_prtimelineevent`

`syncer_prtimelineevent` carries two columns for classifying the acting
account, added upstream by queueboard-core design doc 051. The production
drain finished **2026-08-24**.

| column | meaning |
| --- | --- |
| `actor_type` | GitHub's `__typename` for the account: `User`, `Bot`, `Mannequin`, or NULL |
| `actor_node_id` | the account's GraphQL node id — stable across login renames |

Upstream has no export README, so this file is where the export schema gets
documented; doc 051 names that as a cross-repo action item.

### `NULL` means unknown, never `User`

7.0 % of mathlib4 rows (42,289 of 607,585 on the 2026-08-24 export) have
`actor_type IS NULL`, permanently. Treating null as `User` reintroduces the
exact bug the columns exist to fix.

### Bot classification is a union of three tests

Use `qb_notebook.review_states.bot_actor_expr`, which returns a
`pl.Expr` composing whichever legs the frame supports. Do not hand-roll a
`actor_type == "Bot"` filter — each leg is load-bearing:

```
actor_type == "Bot"                        # 225,706 events
or actor_node_id in MACHINE_USER_NODE_IDS  #  32,036 events, 6 frozen accounts
or actor_login in DEFAULT_BOT_ACTORS       # ~42,009 events with no type at all
```

1. **The type leg catches GitHub Apps** with no list to maintain. This is the
   win: the next App the maintainers add is classified for free. It found two
   nobody had listed — `mathlib-update-dependencies` and `mathlib-nolints`.
2. **Machine users report `User`** and are indistinguishable from humans by
   type, so they still need a list — keyed on `actor_node_id`, because all six
   accounts are historical and frozen, which makes the key permanent.
3. **The login leg is not vestigial.** Of the 42,289 null-typed rows, only
   **148** have no actor at all; **42,141** carry a login. 41,329 of those
   belong to a single account, `leanprover-community-mathlib4-bot` (active
   2023-07 → 2026-02), which has **no `actor_node_id` either** — it is
   login-keyable or nothing. Across all known automation the login leg is the
   only thing typing ~42,009 bot events, **3,067** of them first-touch-eligible
   `ISSUE_COMMENTED` rows.

Note that doc 051's Consequences section puts leg 3 at 678 events. That
undercounts by ~60×; it appears to classify the `leanprover-community-mathlib4-bot`
block as "GitHub reports no actor" when those rows do carry a login. Measured
locally against the 2026-08-24 export.

### Three artifact shapes must all work

`bot_actor_expr` inspects the frame and silently drops legs whose column is
absent or not `String`-typed, so callers never branch on artifact vintage:

1. **columns absent** — any export predating the deploy; login-only.
2. **columns present but all-null `Float64`** — an export that ran mid-drain.
   `pl.Float64` on `actor_type` means "no data", **not** "no bots". This is
   not hypothetical: `requested_team_slug` still has exactly this shape in
   current artifacts, and comparing such a column to `"Bot"` raises
   `ComputeError: cannot compare string with numeric type (f64)`.
3. **columns present with string values** — post-drain, the target.

### Known-good facts (2026-08-24 export)

- No login maps to more than one `actor_node_id`, so node-id keying is
  currently unambiguous. A login with two node ids would be a rename or an
  account replacement — that is a feature of keeping `actor_login` as-of
  ingest, and the 2026-02-03 signature.
- The 2026-02-03 changeover was an account **replacement**: the `mathlib4-*`
  machine users were replaced by `mathlib-*` GitHub Apps. No key, node id
  included, bridges the substitution — both spellings must stay listed.
- `PRReviewInlineComment` has no `author_type`; typing it is an explicit
  upstream non-goal because an inline comment's author *is* the parent
  review's author. Verified locally on the **full** population rather than
  upstream's 33-comment sample: all 96,406 rows join
  `review_node_id` → `syncer_prtimelineevent.github_node_id`, 99.95 % resolve
  a type, and there are **zero** author mismatches. The join would also catch
  **zero** additional bots over the login filter, so `inline_comment_stats`
  deliberately does not do it.
- `bottine` and `guptbot` are human contributors whose logins merely look
  bot-like. Never add them.

### Deliberate non-consumers

`assignments.ASSIGNMENT_BOT_ACTORS` is **not** wired to `bot_actor_expr`. It
answers a narrower question — "which bots *assign* reviewers" — and folding in
the generic union would erode that on purpose-built ground. It is also already
exact: `mathlib-triage` is the only `Bot`-typed account emitting
`ASSIGNED`/`UNASSIGNED`, and it is listed; `leanprover-community-bot-assistant`
reports `User` and is listed too.

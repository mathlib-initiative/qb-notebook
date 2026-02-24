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

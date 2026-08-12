from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from qb_notebook.zulip_io import (
    MESSAGE_SCHEMA,
    ZulipCredentials,
    ZulipError,
    _error_detail,
    _retry_after,
    channel_slug,
    default_cache_path,
    extract_pr_references,
    label_top_senders,
    load_cached_messages,
    load_credentials,
    mention_summary,
    messages_to_frame,
    pr_mentions_per_day,
    pr_numbers_in_html,
    with_rolling_mean,
)


def _link(url: str, text: str | None = None) -> str:
    return f'<p><a href="{url}">{text or url}</a></p>'


# --------------------------------------------------------------------------- #
# pr_numbers_in_html
# --------------------------------------------------------------------------- #


def test_extracts_plain_pr_link():
    html = _link("https://github.com/leanprover-community/mathlib4/pull/30070")
    assert pr_numbers_in_html(html) == [30070]


def test_accepts_http_and_www_and_trailing_path_or_fragment():
    html = "".join(
        [
            _link("http://github.com/leanprover-community/mathlib4/pull/1"),
            _link("https://www.github.com/leanprover-community/mathlib4/pull/2"),
            _link("https://github.com/leanprover-community/mathlib4/pull/3/files"),
            _link(
                "https://github.com/leanprover-community/mathlib4/pull/4#discussion_r2455167829"
            ),
        ]
    )
    assert pr_numbers_in_html(html) == [1, 2, 3, 4]


def test_linkifier_expansions_are_picked_up_from_rendered_html():
    """Shorthand like ``#12345`` arrives already expanded by the server."""
    html = (
        '<p>Reviewed <a href="https://github.com/leanprover-community/mathlib4/pull/12345">'
        "#12345</a> and "
        '<a href="https://github.com/leanprover-community/mathlib4/pull/678">mathlib#678</a></p>'
    )
    assert pr_numbers_in_html(html) == [12345, 678]


def test_deduplicates_within_a_message_preserving_first_appearance():
    html = "".join(
        [
            _link("https://github.com/leanprover-community/mathlib4/pull/99"),
            _link("https://github.com/leanprover-community/mathlib4/pull/12"),
            _link("http://github.com/leanprover-community/mathlib4/pull/99", "again"),
        ]
    )
    assert pr_numbers_in_html(html) == [99, 12]


def test_other_repos_are_excluded_by_default_but_reachable_with_repo_none():
    html = "".join(
        [
            _link("https://github.com/leanprover-community/mathlib4/pull/5"),
            _link("https://github.com/leanprover/lean4/pull/6"),
            _link("https://github.com/leanprover-community/batteries/pull/7"),
        ]
    )
    assert pr_numbers_in_html(html) == [5]
    assert pr_numbers_in_html(html, repo="leanprover/lean4") == [6]
    assert sorted(pr_numbers_in_html(html, repo=None)) == [5, 6, 7]


def test_repo_match_is_case_insensitive():
    html = _link("https://github.com/LeanProver-Community/MathLib4/pull/8")
    assert pr_numbers_in_html(html) == [8]


def test_issue_links_and_bare_repo_links_are_not_prs():
    html = "".join(
        [
            _link("https://github.com/leanprover-community/mathlib4/issues/40"),
            _link("https://github.com/leanprover-community/mathlib4"),
            _link("https://github.com/leanprover-community/mathlib4/pull/abc"),
        ]
    )
    assert pr_numbers_in_html(html) == []


def test_pr_number_must_be_complete():
    """`pull/123` inside `pull/1234` must not match as 123."""
    html = _link("https://github.com/leanprover-community/mathlib4/pull/1234")
    assert pr_numbers_in_html(html) == [1234]


def test_html_entities_in_href_are_unescaped():
    html = _link(
        "https://github.com/leanprover-community/mathlib4/pull/9?foo=1&amp;bar=2"
    )
    assert pr_numbers_in_html(html) == [9]


def test_text_mentioning_a_pr_without_a_link_is_ignored():
    """Headings such as ``## 10 Aug 2026`` are why we read hrefs, not raw text."""
    assert pr_numbers_in_html("<h2>10 Aug 2026</h2><p>see PR 30070</p>") == []


def test_empty_and_null_bodies():
    assert pr_numbers_in_html(None) == []
    assert pr_numbers_in_html("") == []


# --------------------------------------------------------------------------- #
# Frame-level extraction
# --------------------------------------------------------------------------- #


def _messages(*specs: tuple[int, str, str, str]) -> pl.DataFrame:
    """Build a message frame from ``(id, iso_datetime, sender, html)`` tuples."""
    rows = [
        {
            "message_id": mid,
            "sent_at": datetime.fromisoformat(when).replace(tzinfo=timezone.utc),
            "stream_id": 526094,
            "channel": "personal logs",
            "topic": sender,
            "sender_id": abs(hash(sender)) % 1000,
            "sender_full_name": sender,
            "content_html": html,
        }
        for mid, when, sender, html in specs
    ]
    return pl.DataFrame(rows, schema=MESSAGE_SCHEMA)


def test_extract_pr_references_explodes_to_one_row_per_pair():
    messages = _messages(
        (
            1,
            "2026-01-01T10:00:00",
            "Ann",
            _link("https://github.com/leanprover-community/mathlib4/pull/10")
            + _link("https://github.com/leanprover-community/mathlib4/pull/11"),
        ),
        (
            2,
            "2026-01-01T12:00:00",
            "Bo",
            _link("https://github.com/leanprover-community/mathlib4/pull/10"),
        ),
        (3, "2026-01-02T09:00:00", "Cy", "<p>no links today</p>"),
    )
    refs = extract_pr_references(messages)

    assert refs.height == 3
    assert refs["pr_number"].to_list() == [10, 11, 10]
    assert refs["date"].to_list() == [date(2026, 1, 1)] * 3
    assert 3 not in refs["message_id"].to_list()


def test_extract_pr_references_on_empty_input():
    refs = extract_pr_references(pl.DataFrame([], schema=MESSAGE_SCHEMA))
    assert refs.is_empty()
    assert "pr_number" in refs.columns


def test_date_is_the_utc_calendar_date():
    messages = _messages(
        (
            1,
            "2026-03-02T23:30:00",
            "Ann",
            _link("https://github.com/leanprover-community/mathlib4/pull/1"),
        ),
    )
    assert extract_pr_references(messages)["date"].to_list() == [date(2026, 3, 2)]


# --------------------------------------------------------------------------- #
# Daily series
# --------------------------------------------------------------------------- #


def _refs(*specs: tuple[str, str, int]) -> pl.DataFrame:
    """Build a reference frame from ``(iso_date, sender, pr_number)`` tuples."""
    messages = []
    for index, (day, sender, pr) in enumerate(specs, start=1):
        messages.append(
            (
                index,
                f"{day}T12:00:00",
                sender,
                _link(f"https://github.com/leanprover-community/mathlib4/pull/{pr}"),
            )
        )
    return extract_pr_references(_messages(*messages))


def test_pr_mentions_per_day_fills_quiet_days_with_zeros():
    refs = _refs(("2026-01-01", "Ann", 1), ("2026-01-05", "Bo", 2))
    daily = pr_mentions_per_day(refs)

    assert daily["date"].to_list() == [date(2026, 1, d) for d in range(1, 6)]
    assert daily["prs_mentioned"].to_list() == [1, 0, 0, 0, 1]
    assert daily["messages"].to_list() == [1, 0, 0, 0, 1]


def test_pr_mentions_per_day_counts_distinct_prs_and_pairs_separately():
    # Same PR cited by two people on one day: one distinct PR, two mentions.
    refs = _refs(("2026-01-01", "Ann", 7), ("2026-01-01", "Bo", 7))
    daily = pr_mentions_per_day(refs)

    assert daily["prs_mentioned"].to_list() == [1]
    assert daily["mentions"].to_list() == [2]
    assert daily["messages"].to_list() == [2]
    assert daily["senders"].to_list() == [2]


def test_pr_mentions_per_day_honours_explicit_window():
    refs = _refs(("2026-01-03", "Ann", 1))
    daily = pr_mentions_per_day(refs, start=date(2026, 1, 1), end=date(2026, 1, 4))

    assert daily.height == 4
    assert daily["prs_mentioned"].to_list() == [0, 0, 1, 0]


def test_pr_mentions_per_day_grouped_is_dense_per_group_and_reconciles():
    refs = _refs(
        ("2026-01-01", "Ann", 1),
        ("2026-01-03", "Bo", 2),
        ("2026-01-03", "Bo", 3),
    )
    daily = pr_mentions_per_day(refs, group_by=["sender_full_name"])

    assert daily.height == 6  # 3 days x 2 senders
    assert daily["mentions"].sum() == refs.height
    ann = daily.filter(pl.col("sender_full_name") == "Ann")
    assert ann["prs_mentioned"].to_list() == [1, 0, 0]


def test_pr_mentions_per_day_on_empty_input_keeps_group_columns():
    empty = extract_pr_references(pl.DataFrame([], schema=MESSAGE_SCHEMA))
    daily = pr_mentions_per_day(empty, group_by=["sender_full_name"])
    assert daily.is_empty()
    assert daily.columns[:2] == ["date", "sender_full_name"]


def test_pr_mentions_per_day_empty_schema_matches_the_populated_one():
    """A non-string group key must not come back as String just because it's empty."""
    refs = _refs(("2026-01-01", "Ann", 1))
    populated = pr_mentions_per_day(refs, group_by=["sender_id"])
    empty = pr_mentions_per_day(refs.clear(), group_by=["sender_id"])

    assert empty.schema == populated.schema
    assert empty.schema["sender_id"] == pl.Int64


# --------------------------------------------------------------------------- #
# Rolling mean
# --------------------------------------------------------------------------- #


def test_with_rolling_mean_averages_over_calendar_days_including_zeros():
    refs = _refs(("2026-01-01", "Ann", 1), ("2026-01-04", "Bo", 2))
    daily = with_rolling_mean(pr_mentions_per_day(refs), window=2)

    # Zero-filled days participate: [1, 0, 0, 1] -> [null, 0.5, 0.0, 0.5].
    assert daily["prs_mentioned_2d_avg"].to_list() == [None, 0.5, 0.0, 0.5]


def test_with_rolling_mean_sorts_before_rolling():
    refs = _refs(
        ("2026-01-01", "Ann", 1), ("2026-01-02", "Bo", 2), ("2026-01-02", "Bo", 3)
    )
    shuffled = pr_mentions_per_day(refs).sort("date", descending=True)
    rolled = with_rolling_mean(shuffled, window=2)

    assert rolled["date"].to_list() == [date(2026, 1, 1), date(2026, 1, 2)]
    assert rolled["prs_mentioned_2d_avg"].to_list() == [None, 1.5]


def test_with_rolling_mean_does_not_bleed_across_groups():
    refs = _refs(
        ("2026-01-01", "Ann", 1),
        ("2026-01-02", "Ann", 2),
        ("2026-01-01", "Bo", 3),
        ("2026-01-02", "Bo", 4),
    )
    daily = pr_mentions_per_day(refs, group_by=["sender_full_name"])
    rolled = with_rolling_mean(daily, window=2, over=["sender_full_name"])

    # Each group independently gets exactly one leading null.
    nulls = (
        rolled.group_by("sender_full_name")
        .agg(pl.col("prs_mentioned_2d_avg").null_count().alias("nulls"))
        .sort("sender_full_name")
    )
    assert nulls["nulls"].to_list() == [1, 1]


def test_with_rolling_mean_handles_multiple_columns():
    refs = _refs(("2026-01-01", "Ann", 1), ("2026-01-02", "Bo", 2))
    rolled = with_rolling_mean(
        pr_mentions_per_day(refs), columns=("prs_mentioned", "messages"), window=2
    )
    assert "prs_mentioned_2d_avg" in rolled.columns
    assert "messages_2d_avg" in rolled.columns


# --------------------------------------------------------------------------- #
# Sender helpers
# --------------------------------------------------------------------------- #


def test_label_top_senders_folds_the_tail_into_other():
    refs = _refs(
        ("2026-01-01", "Ann", 1),
        ("2026-01-01", "Ann", 2),
        ("2026-01-01", "Bo", 3),
        ("2026-01-01", "Cy", 4),
    )
    labelled = label_top_senders(refs, limit=1)
    groups = dict(
        zip(labelled["sender_full_name"].to_list(), labelled["sender_group"].to_list())
    )
    assert groups == {"Ann": "Ann", "Bo": "Other", "Cy": "Other"}


def test_mention_summary_ranks_by_distinct_prs():
    refs = _refs(
        ("2026-01-01", "Ann", 1),
        ("2026-01-02", "Ann", 1),  # same PR again -> 1 distinct, 2 mentions
        ("2026-01-01", "Bo", 2),
        ("2026-01-01", "Bo", 3),
    )
    summary = mention_summary(refs)

    assert summary["sender_full_name"].to_list() == ["Bo", "Ann"]
    ann = summary.filter(pl.col("sender_full_name") == "Ann")
    assert ann["prs_mentioned"].item() == 1
    assert ann["mentions"].item() == 2
    assert ann["first_log"].item() == date(2026, 1, 1)
    assert ann["last_log"].item() == date(2026, 1, 2)


# --------------------------------------------------------------------------- #
# Credentials / cache paths
# --------------------------------------------------------------------------- #


def test_load_credentials_reads_api_section(tmp_path):
    rc = tmp_path / "zuliprc"
    rc.write_text(
        "[api]\nemail=bot@example.com\nkey=secret\nsite=https://example.zulipchat.com\n"
    )
    creds = load_credentials(rc)

    assert creds.email == "bot@example.com"
    assert creds.api_key == "secret"
    assert creds.auth_header.startswith("Basic ")


def test_credentials_repr_hides_the_api_key():
    creds = ZulipCredentials(email="bot@example.com", api_key="super-secret")
    assert "super-secret" not in repr(creds)


def test_load_credentials_errors_are_actionable(tmp_path):
    missing = tmp_path / "nope"
    with pytest.raises(ZulipError, match="no zuliprc found"):
        load_credentials(missing)

    no_section = tmp_path / "bad"
    no_section.write_text("[other]\nfoo=bar\n")
    with pytest.raises(ZulipError, match="no \\[api\\] section"):
        load_credentials(no_section)

    no_key = tmp_path / "partial"
    no_key.write_text("[api]\nemail=bot@example.com\n")
    with pytest.raises(ZulipError, match="missing \\[api\\] key"):
        load_credentials(no_key)


def test_messages_to_frame_sorts_and_types(tmp_path):
    frame = messages_to_frame(
        [
            {
                "id": 20,
                "timestamp": 1786351367,
                "stream_id": 526094,
                "display_recipient": "personal logs",
                "subject": "Ann",
                "sender_id": 1,
                "sender_full_name": "Ann",
                "content": "<p>hi</p>",
            },
            {
                "id": 10,
                "timestamp": 1786335667,
                "stream_id": 526094,
                "display_recipient": "personal logs",
                "subject": "Bo",
                "sender_id": 2,
                "sender_full_name": "Bo",
                "content": "<p>yo</p>",
            },
        ]
    )
    assert frame["message_id"].to_list() == [10, 20]
    assert frame.schema == pl.Schema(MESSAGE_SCHEMA)
    assert frame["sent_at"].dtype.time_zone == "UTC"


def test_messages_to_frame_on_empty_iterable():
    frame = messages_to_frame([])
    assert frame.is_empty()
    assert frame.schema == pl.Schema(MESSAGE_SCHEMA)


# --------------------------------------------------------------------------- #
# HTTP error handling
# --------------------------------------------------------------------------- #


def test_error_detail_surfaces_the_server_message():
    body = b'{"result": "error", "msg": "Invalid narrow operator: unknown channel"}'
    assert _error_detail(body) == " — Invalid narrow operator: unknown channel"


def test_error_detail_tolerates_non_json_and_empty_bodies():
    assert _error_detail(b"<html>502 Bad Gateway</html>") == ""
    assert _error_detail(b"") == ""
    assert _error_detail(b'{"result": "error"}') == ""


def test_retry_after_prefers_the_json_body():
    assert _retry_after(b'{"retry-after": 2.5}', {"Retry-After": "99"}) == 2.5


def test_retry_after_falls_back_to_the_header_then_a_default():
    assert _retry_after(b"not json", {"Retry-After": "7"}) == 7.0
    assert _retry_after(b"not json", {}) == 5.0
    assert _retry_after(b"not json", None) == 5.0


def test_load_cached_messages_on_a_missing_file_is_an_empty_typed_frame(tmp_path):
    """The notebook relies on this to render before the cache exists."""
    frame = load_cached_messages(tmp_path / "not-there.parquet")
    assert frame.is_empty()
    assert frame.schema == pl.Schema(MESSAGE_SCHEMA)


def test_load_cached_messages_round_trips_a_written_cache(tmp_path):
    path = tmp_path / "cache.parquet"
    original = messages_to_frame(
        [
            {
                "id": 7,
                "timestamp": 1786335667,
                "stream_id": 526094,
                "display_recipient": "personal logs",
                "subject": "Ann",
                "sender_id": 1,
                "sender_full_name": "Ann",
                "content": "<p>hi</p>",
            }
        ]
    )
    original.write_parquet(path)
    assert load_cached_messages(path).equals(original)


def test_channel_slug_and_default_cache_path():
    assert channel_slug("personal logs") == "personal_logs"
    assert channel_slug("Mathlib reviews!") == "mathlib_reviews"
    path = default_cache_path("personal logs", cache_dir="/tmp/zc")
    assert path.as_posix() == "/tmp/zc/personal_logs.parquet"

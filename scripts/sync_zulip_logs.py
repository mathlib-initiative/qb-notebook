#!/usr/bin/env python
"""Refresh the local parquet cache of a Zulip channel.

Run from the repo root::

    uv run python -m scripts.sync_zulip_logs                  # 'personal logs'
    uv run python -m scripts.sync_zulip_logs --list-channels   # what the bot can see
    uv run python -m scripts.sync_zulip_logs --full-refresh    # redownload from scratch

Needs a ``zuliprc`` with bot credentials (``$ZULIPRC``, ``./zuliprc``, or
``~/.zuliprc``). The cache lands in the gitignored ``zulip_cache/`` — *not* in
``data/``, which the artifact download deletes wholesale — and holds people's
personal work logs verbatim, so keep it local.

The sync is incremental: only messages newer than the highest cached id are
fetched, so edits to already-cached messages need ``--full-refresh``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

from qb_notebook.zulip_io import (
    DEFAULT_CHANNEL,
    DEFAULT_REPO,
    ZulipClient,
    ZulipError,
    default_cache_path,
    extract_pr_references,
    load_credentials,
    mention_summary,
    sync_channel_messages,
)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--channel",
        default=DEFAULT_CHANNEL,
        help=f"Channel name (default: {DEFAULT_CHANNEL!r}).",
    )
    p.add_argument(
        "--cache",
        type=Path,
        default=None,
        help="Parquet cache path (default: ./zulip_cache/<channel-slug>.parquet).",
    )
    p.add_argument("--zuliprc", type=Path, default=None, help="Path to a zuliprc file.")
    p.add_argument(
        "--repo", default=DEFAULT_REPO, help="owner/name to count PR links for."
    )
    p.add_argument(
        "--full-refresh",
        action="store_true",
        help="Redownload the whole channel instead of only newer messages.",
    )
    p.add_argument(
        "--list-channels",
        action="store_true",
        help="Print the channels this bot can read, then exit.",
    )
    p.add_argument(
        "--summary", action="store_true", help="Print per-sender PR-mention totals."
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        client = ZulipClient(load_credentials(args.zuliprc))
    except ZulipError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.list_channels:
        for name in client.channel_names():
            print(name)
        return 0

    cache_path = args.cache or default_cache_path(args.channel)
    # Counted even under --full-refresh, where the fetch is a redownload rather
    # than a top-up: "+0 new" is the honest report when nothing actually changed.
    before = pl.read_parquet(cache_path).height if cache_path.is_file() else 0

    try:
        messages = sync_channel_messages(
            args.channel,
            cache_path=cache_path,
            client=client,
            full_refresh=args.full_refresh,
            progress=lambda n: print(f"  fetched {n} messages...", file=sys.stderr),
        )
    except ZulipError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    references = extract_pr_references(messages, repo=args.repo)
    print(
        f"✓ {cache_path}: {messages.height} messages (+{messages.height - before} new)"
    )
    if messages.is_empty():
        print(
            f"  no messages found — is {args.channel!r} spelled right? "
            "(--list-channels shows what the bot can read)",
            file=sys.stderr,
        )
        return 0
    print(
        f"  {references.height} {args.repo} PR references, "
        f"{references['pr_number'].n_unique()} distinct PRs, "
        f"{messages['sent_at'].min():%Y-%m-%d} .. {messages['sent_at'].max():%Y-%m-%d}"
    )
    if args.summary:
        print()
        with pl.Config(tbl_rows=-1):
            print(mention_summary(references))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

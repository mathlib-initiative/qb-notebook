"""Read a Zulip channel over the REST API and mine GitHub PR references from it.

Built for the ``personal logs`` channel of the Mathlib Initiative Zulip, where
team members post weekly-ish work logs that cite the mathlib4 PRs they touched.

Two design decisions worth knowing about:

**Messages are fetched as rendered HTML** (``apply_markdown=true``), not raw
markdown, and PR references are read out of ``<a href="...">`` targets. The realm
defines linkifiers that expand shorthand into real mathlib4 PR links --
``#12345``, ``mathlib#12345``, ``!4#12345``, ``leanprover-community/mathlib4#12345``
-- so the rendered HTML already contains every link a human reader sees, and we
never have to reimplement Zulip's linkifier engine. It also drops the false
positives a raw-markdown regex would hit, because Zulip does not linkify inside
headings or code blocks (``## 10 Aug 2026`` is a heading, not a reference to PR
10).

**The parquet cache does not live in ``data/``.** That directory is deleted
wholesale by
:func:`qb_notebook.artifacts.download_and_extract_latest_successful_workflow_artifacts`,
so the cache goes to a separate gitignored ``zulip_cache/`` at the repo root. The
cached HTML holds people's personal work logs verbatim; keep it out of git.
"""

from __future__ import annotations

import configparser
import html
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence

import polars as pl

DEFAULT_SITE = "https://mathlib-initiative.zulipchat.com"
DEFAULT_CHANNEL = "personal logs"
DEFAULT_REPO = "leanprover-community/mathlib4"
DEFAULT_CACHE_DIR = "zulip_cache"
DEFAULT_WINDOW = 14

_REPO_ROOT = Path(__file__).resolve().parents[1]
_USER_AGENT = "qb-notebook-zulip-io"
_HTTP_TIMEOUT = 120
# Zulip caps num_before/num_after at 5000; 1000 keeps each response modest.
_BATCH = 1000
_MAX_RETRIES = 5
_DEFAULT_RETRY_AFTER = 5.0

#: Schema of the message cache. Declared explicitly so an empty channel (or a
#: first run with nothing fetched yet) still produces a well-typed frame.
MESSAGE_SCHEMA: dict[str, pl.DataType] = {
    "message_id": pl.Int64,
    "sent_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "stream_id": pl.Int64,
    "channel": pl.String,
    "topic": pl.String,
    "sender_id": pl.Int64,
    "sender_full_name": pl.String,
    "content_html": pl.String,
}

REFERENCE_SCHEMA: dict[str, pl.DataType] = {
    "message_id": pl.Int64,
    "sent_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "date": pl.Date,
    "stream_id": pl.Int64,
    "channel": pl.String,
    "topic": pl.String,
    "sender_id": pl.Int64,
    "sender_full_name": pl.String,
    "pr_number": pl.Int64,
}

#: Count columns produced by :func:`pr_mentions_per_day`.
COUNT_COLUMNS = ("prs_mentioned", "mentions", "messages", "senders")


class ZulipError(RuntimeError):
    """Raised when the Zulip API is unreachable or returns an error result."""


# --------------------------------------------------------------------------- #
# Credentials
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ZulipCredentials:
    """Bot credentials from a ``zuliprc`` file.

    ``api_key`` is excluded from ``repr`` so the secret does not leak into
    tracebacks or notebook cell output.
    """

    email: str
    api_key: str = field(repr=False)
    site: str = DEFAULT_SITE

    @property
    def base_url(self) -> str:
        return f"{self.site.rstrip('/')}/api/v1"

    @property
    def auth_header(self) -> str:
        token = b64encode(f"{self.email}:{self.api_key}".encode()).decode()
        return f"Basic {token}"


def candidate_zuliprc_paths() -> list[Path]:
    """Where :func:`load_credentials` looks, in order."""
    paths = []
    env = os.environ.get("ZULIPRC")
    if env:
        paths.append(Path(env).expanduser())
    paths.append(_REPO_ROOT / "zuliprc")
    paths.append(Path.home() / ".zuliprc")
    return paths


def load_credentials(path: str | Path | None = None) -> ZulipCredentials:
    """Read a ``zuliprc`` (ini with an ``[api]`` section).

    With no ``path``, tries ``$ZULIPRC``, then ``<repo root>/zuliprc``, then
    ``~/.zuliprc``.
    """
    candidates = [Path(path).expanduser()] if path else candidate_zuliprc_paths()
    for candidate in candidates:
        if candidate.is_file():
            parser = configparser.ConfigParser()
            parser.read(candidate)
            if not parser.has_section("api"):
                raise ZulipError(f"{candidate} has no [api] section")
            section = parser["api"]
            missing = [key for key in ("email", "key") if not section.get(key)]
            if missing:
                raise ZulipError(f"{candidate} is missing [api] {', '.join(missing)}")
            return ZulipCredentials(
                email=section["email"].strip(),
                api_key=section["key"].strip(),
                site=section.get("site", DEFAULT_SITE).strip(),
            )
    tried = ", ".join(str(candidate) for candidate in candidates)
    raise ZulipError(f"no zuliprc found (tried: {tried})")


# --------------------------------------------------------------------------- #
# API client
# --------------------------------------------------------------------------- #


def _retry_after(body: bytes, headers=None) -> float:
    """Seconds to wait after a 429, from the JSON body or the header."""
    try:
        return float(json.loads(body.decode())["retry-after"])
    except Exception:
        try:
            return float(headers.get("Retry-After"))
        except (AttributeError, TypeError, ValueError):
            return _DEFAULT_RETRY_AFTER


def _error_detail(body: bytes) -> str:
    """Zulip's own ``msg`` for a failed request, e.g. an unknown channel name."""
    try:
        message = json.loads(body.decode()).get("msg")
    except Exception:
        return ""
    return f" — {message}" if message else ""


class ZulipClient:
    """Minimal read-only Zulip REST client (stdlib only, no ``zulip`` dep)."""

    def __init__(
        self,
        credentials: ZulipCredentials | None = None,
        *,
        timeout: int = _HTTP_TIMEOUT,
    ) -> None:
        self.credentials = credentials or load_credentials()
        self.timeout = timeout

    def get(self, endpoint: str, params: dict[str, str] | None = None) -> dict:
        """GET ``/api/v1/<endpoint>``, retrying on rate limits."""
        url = f"{self.credentials.base_url}/{endpoint.lstrip('/')}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": self.credentials.auth_header,
                "User-Agent": _USER_AGENT,
            },
        )
        payload: dict | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.load(response)
                break
            except urllib.error.HTTPError as exc:
                body = exc.read()
                if exc.code == 429 and attempt < _MAX_RETRIES - 1:
                    time.sleep(_retry_after(body, exc.headers))
                    continue
                raise ZulipError(
                    f"GET {endpoint} failed: HTTP {exc.code}{_error_detail(body)}"
                ) from exc
            except urllib.error.URLError as exc:
                raise ZulipError(f"GET {endpoint} failed: {exc.reason}") from exc
        if payload is None:
            raise ZulipError(f"GET {endpoint} failed: rate limited after retries")
        if payload.get("result") != "success":
            raise ZulipError(f"GET {endpoint} returned: {payload.get('msg')!r}")
        return payload

    def channel_names(self) -> list[str]:
        """Channels this bot can see (useful for spelling checks)."""
        return sorted(
            stream["name"] for stream in self.get("streams").get("streams", [])
        )


def fetch_channel_messages(
    channel: str = DEFAULT_CHANNEL,
    *,
    client: ZulipClient | None = None,
    after_id: int | None = None,
    batch: int = _BATCH,
    progress: Callable[[int], None] | None = None,
) -> list[dict]:
    """Raw message dicts for ``channel``, oldest first.

    Pass ``after_id`` to fetch only messages newer than that id. Bodies come back
    as Zulip's rendered HTML, with linkifiers already expanded -- see the module
    docstring for why that matters.
    """
    client = client or ZulipClient()
    narrow = json.dumps([{"operator": "channel", "operand": channel}])
    anchor = "oldest" if after_id is None else str(int(after_id))
    include_anchor = after_id is None

    messages: list[dict] = []
    seen: set[int] = set()
    while True:
        payload = client.get(
            "messages",
            {
                "anchor": anchor,
                "num_before": "0",
                "num_after": str(batch),
                "narrow": narrow,
                "apply_markdown": "true",
                "include_anchor": "true" if include_anchor else "false",
            },
        )
        page = payload.get("messages", [])
        fresh = [message for message in page if message["id"] not in seen]
        messages.extend(fresh)
        seen.update(message["id"] for message in fresh)
        if progress is not None:
            progress(len(messages))
        # `not fresh` also guards against a server that keeps echoing the anchor.
        if payload.get("found_newest") or not page or not fresh:
            break
        anchor = str(page[-1]["id"])
        include_anchor = False
    return messages


# --------------------------------------------------------------------------- #
# Cache
# --------------------------------------------------------------------------- #


def messages_to_frame(messages: Iterable[dict]) -> pl.DataFrame:
    """Turn raw API message dicts into a :data:`MESSAGE_SCHEMA` frame."""
    rows = []
    for message in messages:
        recipient = message.get("display_recipient")
        rows.append(
            {
                "message_id": message["id"],
                "sent_at": datetime.fromtimestamp(
                    message["timestamp"], tz=timezone.utc
                ),
                "stream_id": message.get("stream_id"),
                "channel": recipient if isinstance(recipient, str) else None,
                "topic": message.get("subject"),
                "sender_id": message.get("sender_id"),
                "sender_full_name": message.get("sender_full_name"),
                "content_html": message.get("content"),
            }
        )
    return pl.DataFrame(rows, schema=MESSAGE_SCHEMA).sort("message_id")


def channel_slug(channel: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", channel.lower()).strip("_") or "channel"


def default_cache_path(
    channel: str = DEFAULT_CHANNEL, *, cache_dir: str | Path | None = None
) -> Path:
    """``<repo root>/zulip_cache/<channel-slug>.parquet`` by default."""
    root = Path(cache_dir) if cache_dir else _REPO_ROOT / DEFAULT_CACHE_DIR
    return root / f"{channel_slug(channel)}.parquet"


def load_cached_messages(cache_path: str | Path) -> pl.DataFrame:
    """Read the cache, or an empty typed frame when it does not exist yet."""
    path = Path(cache_path)
    if not path.is_file():
        return pl.DataFrame([], schema=MESSAGE_SCHEMA)
    return pl.read_parquet(path).sort("message_id")


def sync_channel_messages(
    channel: str = DEFAULT_CHANNEL,
    *,
    cache_path: str | Path | None = None,
    client: ZulipClient | None = None,
    full_refresh: bool = False,
    progress: Callable[[int], None] | None = None,
) -> pl.DataFrame:
    """Bring the local parquet cache of ``channel`` up to date and return it.

    Incremental: only messages newer than the highest cached id are downloaded,
    which means later *edits* to (or deletions of) already-cached messages are
    not picked up. Pass ``full_refresh=True`` to redownload from scratch.
    """
    path = Path(cache_path) if cache_path else default_cache_path(channel)
    cached = (
        pl.DataFrame([], schema=MESSAGE_SCHEMA)
        if full_refresh
        else load_cached_messages(path)
    )
    after_id = None if cached.is_empty() else int(cached["message_id"].max())

    fetched = messages_to_frame(
        fetch_channel_messages(
            channel, client=client, after_id=after_id, progress=progress
        )
    )
    combined = (
        pl.concat([cached, fetched], how="vertical")
        .unique(subset="message_id", keep="last")
        .sort("message_id")
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(path)
    return combined


# --------------------------------------------------------------------------- #
# GitHub PR references
# --------------------------------------------------------------------------- #

_HREF_RE = re.compile(r"""<a\b[^>]*?\bhref=(["'])(.*?)\1""", re.IGNORECASE | re.DOTALL)
# Anchored: hrefs are whole attribute values, and trailing path/fragment parts
# (`/files`, `#discussion_r2455167829`) must not break the match.
_PR_URL_RE = re.compile(
    r"https?://(?:www\.)?github\.com"
    r"/(?P<org>[^/?#\s]+)/(?P<repo>[^/?#\s]+)/pull/(?P<number>\d+)(?![\d])",
    re.IGNORECASE,
)


def pr_numbers_in_html(
    content_html: str | None, *, repo: str | None = DEFAULT_REPO
) -> list[int]:
    """PR numbers linked from one rendered-HTML message body.

    Deduplicated, in first-appearance order. ``repo`` filters on
    ``owner/name`` case-insensitively; pass ``None`` to accept any repository.
    """
    if not content_html:
        return []
    wanted = repo.lower() if repo else None
    numbers: list[int] = []
    seen: set[int] = set()
    for _quote, raw in _HREF_RE.findall(content_html):
        match = _PR_URL_RE.match(html.unescape(raw).strip())
        if match is None:
            continue
        if wanted and f"{match['org']}/{match['repo']}".lower() != wanted:
            continue
        number = int(match["number"])
        if number not in seen:
            seen.add(number)
            numbers.append(number)
    return numbers


def extract_pr_references(
    messages: pl.DataFrame, *, repo: str | None = DEFAULT_REPO
) -> pl.DataFrame:
    """Long frame with one row per (message, distinct PR) pair.

    A PR linked twice in the same message counts once; the same PR cited by two
    people on the same day yields two rows (one per message).
    """
    if messages.is_empty():
        return pl.DataFrame([], schema=REFERENCE_SCHEMA)
    return (
        messages.with_columns(
            pl.col("content_html")
            .map_elements(
                lambda body: pr_numbers_in_html(body, repo=repo),
                return_dtype=pl.List(pl.Int64),
            )
            .alias("pr_number")
        )
        .drop("content_html")
        .explode("pr_number")
        .filter(pl.col("pr_number").is_not_null())
        # UTC calendar date, matching the "date (UTC)" convention of the other plots.
        .with_columns(pl.col("sent_at").dt.date().alias("date"))
        .select(list(REFERENCE_SCHEMA))
        .sort(["message_id", "pr_number"])
    )


def label_top_senders(
    references: pl.DataFrame,
    *,
    limit: int = 6,
    column: str = "sender_full_name",
    other_label: str = "Other",
) -> pl.DataFrame:
    """Add a ``sender_group`` column: the ``limit`` busiest senders, rest folded.

    "Busiest" is by distinct PRs mentioned over the whole history. Categorical
    palettes top out well before the number of people in a channel, so the tail
    folds into one neutral bucket rather than growing new hues.
    """
    if references.is_empty():
        return references.with_columns(
            pl.lit(None, dtype=pl.String).alias("sender_group")
        )
    top = (
        references.group_by(column)
        .agg(pl.col("pr_number").n_unique().alias("prs"))
        .sort(["prs", column], descending=[True, False])
        .head(limit)
        .get_column(column)
        .to_list()
    )
    return references.with_columns(
        pl.when(pl.col(column).is_in(top))
        .then(pl.col(column))
        .otherwise(pl.lit(other_label))
        .alias("sender_group")
    )


# --------------------------------------------------------------------------- #
# Daily series
# --------------------------------------------------------------------------- #


def pr_mentions_per_day(
    references: pl.DataFrame,
    *,
    group_by: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pl.DataFrame:
    """Daily mention counts on a **dense** calendar (quiet days are explicit zeros).

    Columns: ``date``, any ``group_by`` keys, then

    - ``prs_mentioned`` -- distinct PRs cited that day,
    - ``mentions`` -- (message, PR) pairs, so two people citing one PR counts twice,
    - ``messages`` -- messages carrying at least one reference,
    - ``senders`` -- distinct people citing something.

    The dense calendar is not cosmetic. Personal-log posts land weekly-ish, so
    most days carry nothing, and Polars' ``rolling_mean`` is row-order-based
    rather than time-based: rolling a sparse series would average over a far
    longer calendar span than the nominal window and inflate the result. Same
    trap as ``generate_plot_site._build_merged_split_per_day``, but sharper here
    because the gaps are much bigger.
    """
    keys = list(group_by or [])
    # Group-key dtypes come from the input so the empty frame matches what the
    # populated path would return (e.g. an Int64 `sender_id` stays Int64).
    schema = {"date": pl.Date}
    schema.update({key: references.schema.get(key, pl.String) for key in keys})
    schema.update({column: pl.Int64 for column in COUNT_COLUMNS})
    if references.is_empty():
        return pl.DataFrame([], schema=schema)

    aggregated = references.group_by([*keys, "date"]).agg(
        pl.col("pr_number").n_unique().alias("prs_mentioned"),
        pl.len().alias("mentions"),
        pl.col("message_id").n_unique().alias("messages"),
        pl.col("sender_id").n_unique().alias("senders"),
    )

    low = start or references["date"].min()
    high = end or references["date"].max()
    calendar = pl.DataFrame({"date": pl.date_range(low, high, "1d", eager=True)})
    if keys:
        calendar = calendar.join(references.select(keys).unique(), how="cross")

    return (
        calendar.join(aggregated, on=[*keys, "date"], how="left")
        .with_columns(
            [pl.col(column).fill_null(0).cast(pl.Int64) for column in COUNT_COLUMNS]
        )
        .sort([*keys, "date"])
    )


def with_rolling_mean(
    daily: pl.DataFrame,
    *,
    columns: Sequence[str] = ("prs_mentioned",),
    window: int = DEFAULT_WINDOW,
    over: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Append ``<column>_<window>d_avg`` trailing means.

    Sorts by ``[*over, "date"]`` first: Polars' ``rolling_mean`` walks rows in
    frame order, so an unsorted (or interleaved-group) frame silently mixes
    dates. The first ``window - 1`` rows of each group are null, matching the
    plot-site series.
    """
    keys = list(over or [])
    sorted_daily = daily.sort([*keys, "date"])
    expressions = []
    for column in columns:
        expression = pl.col(column).cast(pl.Float64).rolling_mean(window)
        if keys:
            expression = expression.over(keys)
        expressions.append(expression.alias(f"{column}_{window}d_avg"))
    return sorted_daily.with_columns(expressions)


def mention_summary(references: pl.DataFrame) -> pl.DataFrame:
    """Per-sender totals: distinct PRs, mentions, log messages, active span."""
    if references.is_empty():
        return pl.DataFrame(
            [],
            schema={
                "sender_full_name": pl.String,
                "prs_mentioned": pl.Int64,
                "mentions": pl.Int64,
                "messages": pl.Int64,
                "first_log": pl.Date,
                "last_log": pl.Date,
            },
        )
    return (
        references.group_by("sender_full_name")
        .agg(
            pl.col("pr_number").n_unique().cast(pl.Int64).alias("prs_mentioned"),
            pl.len().cast(pl.Int64).alias("mentions"),
            pl.col("message_id").n_unique().cast(pl.Int64).alias("messages"),
            pl.col("date").min().alias("first_log"),
            pl.col("date").max().alias("last_log"),
        )
        .sort(["prs_mentioned", "sender_full_name"], descending=[True, False])
    )

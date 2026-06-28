"""Browser/WASM data loading.

Companion to :mod:`qb_notebook.data_io`. The full notebooks read ~155 MB of
raw parquet from ``data/`` via :func:`load_pr_interval_data` — impractical to
ship to a browser. For the WASM builds we pre-slim each notebook's tables
(column-prune + zstd) into a per-notebook ``public/`` folder (see
``scripts/export_wasm_data.py``) and fetch them over HTTP at runtime.

The slimmed frames are written *post-parse* — datetimes are already
``Datetime("us", "UTC")`` and FK columns already ``Int64`` — so this loader
just reads parquet and returns the same dict shape as
:func:`~qb_notebook.data_io.load_pr_interval_data`. No re-parsing or
re-casting; the schema is whatever the exporter selected.

Two environments:

- **Pyodide / WASM** (``sys.platform == "emscripten"``): ``base`` is a URL
  (typically ``str(mo.notebook_location() / "public")``) and each parquet is
  fetched over HTTP. This needs ``pyodide_http.patch_all()`` to have run so
  the stdlib ``urllib`` works against the browser ``fetch`` API.
- **Plain CPython** (tests / local verification): ``base`` is a local
  directory; files are read straight off disk.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Callable

import polars as pl

MANIFEST_NAME = "manifest.json"
TEAMS_SNAPSHOT_NAME = "teams.json"

# Byte-getter: maps a file name (relative to ``base``) to its raw bytes.
ByteGetter = Callable[[str], bytes]


def _http_get(url: str) -> bytes:
    # urllib (not requests) so the only browser-side requirement is
    # pyodide_http.patch_all(); the site assets are same-origin.
    from urllib.request import urlopen

    with urlopen(url) as resp:  # noqa: S310 - same-origin site assets
        return resp.read()


def _make_getter(base: str | Path) -> ByteGetter:
    base_str = str(base)
    if base_str.startswith(("http://", "https://")):
        prefix = base_str.rstrip("/")
        return lambda name: _http_get(f"{prefix}/{name}")
    root = Path(base)
    return lambda name: (root / name).read_bytes()


def apply_wasm_compat_shims() -> None:
    """Patch over library API gaps between the repo's pinned versions and the
    (often older) builds Pyodide ships, so the notebooks' plotting code runs
    unchanged in the browser.

    Called from each notebook's WASM bootstrap cell. No-op on a new-enough
    library, so it is safe to call regardless of the installed versions.
    """
    _patch_boxplot_tick_labels()


def _patch_boxplot_tick_labels() -> None:
    # `Axes.boxplot(tick_labels=...)` was added in matplotlib 3.9 (renamed
    # from the older `labels=`). Pyodide may ship < 3.9, where the notebooks'
    # `tick_labels=` raises TypeError. Translate it back to `labels` there.
    import re

    import matplotlib

    m = re.match(r"(\d+)\.(\d+)", matplotlib.__version__)
    if not m or (int(m.group(1)), int(m.group(2))) >= (3, 9):
        return

    from matplotlib.axes import Axes

    _orig_boxplot = Axes.boxplot

    def _boxplot(self, *args, **kwargs):
        if "tick_labels" in kwargs:
            kwargs["labels"] = kwargs.pop("tick_labels")
        return _orig_boxplot(self, *args, **kwargs)

    Axes.boxplot = _boxplot


def load_slimmed_data(
    base: str | Path,
    *,
    get_bytes: ByteGetter | None = None,
) -> dict[str, pl.DataFrame]:
    """Load a slimmed per-notebook dataset.

    Returns the same dict shape as
    :func:`~qb_notebook.data_io.load_pr_interval_data`, restricted to the
    tables the exporter wrote (listed in ``manifest.json``).

    ``base`` is either a URL base (str beginning ``http://`` / ``https://``)
    that holds ``manifest.json`` and the parquet files, or a local directory
    (``Path`` / str). Pass ``get_bytes`` to override the byte-getter (handy
    for tests or custom transports).
    """
    getter = get_bytes if get_bytes is not None else _make_getter(base)
    manifest = json.loads(getter(MANIFEST_NAME).decode("utf-8"))
    tables: dict[str, str] = manifest["tables"]
    return {
        key: pl.read_parquet(io.BytesIO(getter(filename)))
        for key, filename in tables.items()
    }


def load_teams_snapshot(
    base: str | Path,
    *,
    get_bytes: ByteGetter | None = None,
):
    """Load a ``teams.json`` team-membership snapshot shipped under ``base``.

    Returns a :class:`qb_notebook.teams.Teams`. ``base`` is the same
    ``public/`` location as :func:`load_slimmed_data` (a URL in WASM, a local
    directory otherwise). Team-overlay notebooks (``area_health``,
    ``review_state_machine``, ``reviewer_load``) read this in the browser in
    place of the ``leanprover-community.github.io`` checkout they use locally;
    the snapshot is written into ``public/`` by ``scripts/build_wasm_site.py``.
    """
    from qb_notebook.teams import load_snapshot

    getter = get_bytes if get_bytes is not None else _make_getter(base)
    return load_snapshot(getter(TEAMS_SNAPSHOT_NAME))

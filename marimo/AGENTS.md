# marimo/AGENTS.md

Conventions and gotchas specific to the marimo notebooks in this folder.
The root `AGENTS.md` covers project-wide rules; this file adds the
marimo-only details that have bitten us in past sessions.

## The 512-byte header rule (most important)

Marimo's directory scanner does a **fast header sniff** to decide
whether a `.py` file is a notebook: it reads the first **512 bytes** and
looks for both `import marimo` and `marimo.App` substrings. If either
marker isn't in that window, the file is treated as plain Python and
**won't appear in the Workspace pane** even though it parses cleanly,
runs with `marimo edit <path>` if given explicitly, and passes
`marimo check`.

See `marimo/_server/files/directory_scanner.py::is_marimo_app` for the
exact check.

To stay safely under the cutoff:

- Keep the module docstring **short** — one line if possible.
- Put `import marimo`, `__generated_with = ...`, and
  `app = marimo.App(...)` **immediately** after the docstring.
- Move any long prose (run instructions, theme description, notes) into
  comments **after** the `app = ...` line, not before it.
- A quick sanity check:

  ```python
  head = pathlib.Path(p).read_bytes()[:512]
  assert b"import marimo" in head and b"marimo.App" in head
  ```

The required preamble shape:

```python
"""Theme N — short title."""

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")

# Optional longer prose, run instructions, design notes go HERE,
# below the app definition.
```

## Workspace listing is cached at server start

`marimo edit marimo/` enumerates the directory at startup. New files
added later are **not picked up** by:

- the Workspace "refresh" icon (re-renders the cached listing only),
- a browser hard reload,
- the `--watch` flag (which watches the *currently open notebook* for
  on-disk edits, not the parent directory for new files).

To see a new notebook, kill the marimo process (Ctrl+C) and restart it.

## Cell wiring

Marimo cells are functions decorated with `@app.cell`. Values flow
between cells by **named return → named argument**, e.g.:

```python
@app.cell
def _():
    import polars as pl
    return (pl,)

@app.cell
def _(pl):           # consumes `pl` from the cell above
    df = pl.DataFrame(...)
    return (df,)
```

Cell-private temporaries should be `_`-prefixed (e.g. `_fig`, `_ax`,
`_vals`) so they don't collide with downstream cells. Unprefixed names
are part of the cell's contract and **must be globally unique** across
the notebook.

Anti-pattern: `__import__("polars").len()` inside a cell — if you
forgot to thread `pl` into the cell signature, fix the signature
instead. Marimo's static analyzer will complain (or silently break the
DAG) if cells import the same module independently.

## Output rendering

The last expression of a cell becomes the cell's rendered output (like
Jupyter). Don't bury a figure or table in a `print` — leave it as the
final expression. If you want both narration and an artifact in one
cell, return a `mo.vstack([...])` or split into two cells.

## Repo / data path bootstrap

Every notebook needs a `sys.path` bootstrap so `qb_notebook` is
importable regardless of where marimo was launched from, and should
resolve `data/` against the repo root (not `Path.cwd()`):

```python
@app.cell
def _():
    import sys
    from pathlib import Path

    _repo_root = Path(__file__).resolve().parents[1]
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))
    ...
```

## Authoring loop

- Author / explore (auto-reloads on save): `uv run marimo edit marimo/<name>.py`
- Serve read-only: `uv run marimo run marimo/<name>.py`
- Sanity-check the DAG and style: `uv run marimo check marimo/<name>.py`
- Format / lint: standard `uv run ruff format` and `uv run ruff check`.

`marimo check` is fast and catches most cell-graph mistakes (cycles,
duplicate names, unused returns). Run it before committing.

## Publishing notebooks as in-browser WASM (Pyodide)

Notebooks can be published as an interactive static site (GitHub Pages)
that runs entirely in the browser via WebAssembly. The full process —
how the local `qb_notebook` package, the ~155 MB raw data, and the
heavy deps are made browser-ready, the per-notebook conversion recipe,
the build/preview commands, and the known version-shim gotchas — lives
in [`docs/wasm-export.md`](../docs/wasm-export.md). `queue_window_state.py`
is the reference conversion; copy its `is_wasm` bootstrap cell.

## Putting reusable logic somewhere else

IO and reusable transforms belong in `qb_notebook/*` modules, not in
the notebook body. The notebook is for layout, UI controls, and
plotting. If you find yourself writing a helper that more than one
notebook would want, lift it into a module and import it.

Cross-cutting helpers already in place:

- `qb_notebook.data_io.load_pr_interval_data` — canonical parquet
  loader; extend this rather than duplicating the parse-and-cast logic.
- `qb_notebook.intervals` — generic interval-overlap utilities.
- `qb_notebook.review_states` — label-interval reconstruction,
  trigger attribution, first-touch / inline-comment / pipeline-stage
  helpers used across the review-analysis notebooks.
- `qb_notebook.pr_shape` — per-PR shape decorators (size / author
  cohort / draft / PR type).
- `qb_notebook.temporal` — UTC time-of-day / weekday / seasonality
  decorators and per-actor activity windows.
- `qb_notebook.assignments` — assignment / review-request event
  classification and policy-outcome helpers.
- `qb_notebook.teams` — YAML loader for reviewer / maintainer team
  membership (needs a sibling `leanprover-community.github.io`
  checkout).
- `qb_notebook.zulip_io` — Zulip channel reader + mathlib4 PR-link
  extraction, used by `personal_logs.py`. That notebook is the one here
  that does **not** read `data/`: its source is the gitignored
  `zulip_cache/personal_logs.parquet` (see the root AGENTS.md section
  "Zulip Personal Logs"). It degrades gracefully via `mo.stop` when the
  cache is absent, so it renders without credentials — keep that guard
  if you extend it.

See [`docs/review-analysis/notebooks.md`](../docs/review-analysis/notebooks.md)
for which notebook uses what, and the plan index's
[helper inventory](../docs/review-analysis-plan.md#cross-cutting-infrastructure)
for signatures.

## Team-membership loader

`qb_notebook.teams.load(repo_path)` reads `data/people.yaml` and
`data/teams.yaml` from a sibling `leanprover-community.github.io`
checkout and returns a `Teams` dataclass with lowercased GitHub-login
sets (`reviewers`, `maintainers`, `admins`, ...). Notebooks that need
team overlays should:

- Look up `../leanprover-community.github.io` relative to the repo
  root and **fall through gracefully** (warn, render with empty sets)
  if the checkout isn't present — don't hard-fail.
- Pass `warn_on_unmatched=False` if the noise is undesirable in the
  notebook output.

There is also a CLI for dumping a JSON snapshot:

```
uv run python -m qb_notebook.teams \
    --repo ../leanprover-community.github.io \
    --output data/teams.json
```

## Known data quirks (notebook-relevant)

- The `maintainer-merge` label only entered widespread use around
  mid-2024. Anything keyed off it has a short history; prefer
  `ready-to-merge` LABELED events for longer-baseline metrics.
- `actor_login` casing is preserved as the user signed up with on
  GitHub. Lowercase before set-intersecting with team-membership logins
  (which are also lowercased by the teams loader).
- Bors is the actual merger for most PRs; use
  `qb_notebook.filters.expr_merged_to_master` /
  `expr_merged_at_effective` rather than `prs.merged_at` directly.

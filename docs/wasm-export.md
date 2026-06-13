# Publishing the marimo notebooks as WASM (GitHub Pages)

Notes on running the `marimo/*.py` notebooks **fully in the browser** via
WebAssembly (Pyodide), so they can be published as an interactive static
site (GitHub Pages). Viewers run everything locally in their browser — no
server, no data sent anywhere.

**Status (2026-06):** the pattern is proven end-to-end on
`marimo/queue_window_state.py` — it boots, installs packages, fetches the
slimmed data, and renders with live-recomputing UI controls in a real
browser. The remaining work is templating it across the other 8 notebooks
and adding a GitHub Pages deploy workflow (see [Remaining work](#remaining-work)).

## Why this is non-trivial

The notebooks aren't browser-ready as written. Three obstacles, each solved
once and reused:

1. **Local `qb_notebook` package.** The notebooks `import qb_notebook` via a
   `sys.path` bootstrap to the repo root. In the browser there is no repo and
   no path. → We build `qb_notebook` as a **wheel** and `micropip`-install it
   in-browser.
2. **~155 MB of raw parquet** loaded by `load_pr_interval_data("data")`. Too
   big to ship; the browser also has no `data/` dir. → We **pre-slim** each
   notebook's tables (drop unused columns, keep all rows, recompress zstd) to
   a few MB and **fetch them over HTTP** at runtime.
3. **Heavy dependencies.** polars, pandas, numpy, pyarrow, matplotlib, scipy
   are all Pyodide built-ins; seaborn/plotly are pure-Python via `micropip`.
   `kaleido` (static PNG export) has **no** Pyodide build — but it's only used
   by offline export cells, never for in-browser rendering.

A consequence of (3): **Pyodide ships older library versions than the repo
pins.** Notebook code written against new APIs (e.g. matplotlib 3.9's
`boxplot(tick_labels=...)`) breaks in-browser. We absorb these gaps in a
single centralized shim (see `apply_wasm_compat_shims`).

## How it fits together

```
build_wasm_site.py
  ├── uv build --wheel                    → qb_notebook-<ver>-py3-none-any.whl
  ├── marimo export html-wasm <nb>        → _site_wasm/<nb>/index.html (+ assets)
  ├── export_wasm_data.export_notebook    → _site_wasm/<nb>/public/*.parquet + manifest.json
  └── copy wheel                          → _site_wasm/<nb>/public/<wheel>
                                            + _site_wasm/index.html (landing)

in the browser, the notebook's bootstrap cell:
  micropip.install(pyodide-http, [runtime deps], <wheel> deps=False)
  pyodide_http.patch_all()                # so urllib works against fetch()
  apply_wasm_compat_shims()               # matplotlib etc. version gaps
  → wasm_io.load_slimmed_data(notebook_location()/public)  # fetch parquet
```

### Key files

| File | Role |
| --- | --- |
| `qb_notebook/wasm_io.py` | `load_slimmed_data(base)` — fetch slimmed parquet (HTTP in WASM, disk locally), return the `load_pr_interval_data` dict shape. `apply_wasm_compat_shims()` — runtime patches for Pyodide's older libs. |
| `scripts/export_wasm_data.py` | `SPECS` (per-notebook `{table: [columns]}`) + `export_notebook(...)` — writes slimmed parquet + `manifest.json`. |
| `scripts/build_wasm_site.py` | Orchestrates: builds wheel, runs `marimo export html-wasm`, stages `public/`, writes landing page. `WHEEL_NAME` must match the notebook bootstrap. |
| `marimo/queue_window_state.py` | Reference conversion. Copy its bootstrap cell + data-cell branch. |

## Converting a notebook (recipe)

Everything WASM-specific is guarded behind `is_wasm`, so **local
`marimo edit` behavior is unchanged**.

1. **Find the tables + columns the notebook actually reads.** It only needs
   the keys it pulls out of `load_pr_interval_data` (e.g. `queue_window_state`
   uses just `prs`, `events`, `queue_windows` — not the 58 MB `check_runs`).
   For each, list the exact columns the cells *and the helpers they call*
   reference. Read the helper bodies in `qb_notebook/review_states.py` etc. —
   grep is not enough (e.g. `queue_window_intervals` selects `window_count`
   and `closed_by_event_type`, which a casual grep misses).

2. **Add a spec** to `SPECS` in `scripts/export_wasm_data.py`. Slimming is
   **column-only (all rows kept)**, so a notebook's figures from slimmed data
   are *identical* to full-data figures — the dropped columns are simply never
   referenced. Don't row-filter (it breaks that guarantee).

3. **Verify the spec** before touching the notebook (see
   [Verifying](#verifying-without-a-browser)). A missing column shows up as a
   `polars ColumnNotFoundError` naming the column — add it and re-run.

4. **Add the WASM bootstrap cell** — copy it verbatim from
   `marimo/queue_window_state.py` (the `async def _(mo)` cell that sets
   `is_wasm`). It installs `pyodide-http`, the runtime deps **including
   pyarrow**, the wheel (`deps=False`), and calls `apply_wasm_compat_shims()`.
   Keep its locals `_`-prefixed so only `is_wasm` leaks across cells.

5. **Thread `is_wasm` into the imports cell**, guard the `__file__` use
   (undefined in WASM), and add `from qb_notebook.wasm_io import load_slimmed_data`:
   ```python
   @app.cell
   def _(is_wasm):
       import sys
       from pathlib import Path
       if not is_wasm:
           _repo_root = Path(__file__).resolve().parents[1]
           if str(_repo_root) not in sys.path:
               sys.path.insert(0, str(_repo_root))
       ...
       from qb_notebook.wasm_io import load_slimmed_data
       return (..., load_slimmed_data, ...)
   ```

6. **Branch the data-load cell** on `is_wasm`:
   ```python
   @app.cell
   def _(Path, is_wasm, load_pr_interval_data, load_slimmed_data, mo, ...):
       if is_wasm:
           data = load_slimmed_data(str(mo.notebook_location() / "public"))
       else:
           data = load_pr_interval_data(Path(__file__).resolve().parents[1] / "data")
       ...
   ```

7. **Strip / guard offline export cells.** Anything that writes files or uses
   `kaleido` (e.g. `anatomy_of_a_merge.py`'s "Export" section) must be removed
   or guarded behind `not is_wasm` — there's no filesystem and no kaleido in
   the browser.

8. **Run `uv run marimo check <nb>` + `uv run ruff check/format`.** marimo
   gotchas that bit us: cross-cell names must be unique (don't `import sys` in
   two cells unless one is `_`-prefixed), and a cell can't *end* on an `if`
   whose branches are bare expressions (assign side-effecting calls to `_`).

> **PEP 723 note:** we deliberately do **not** add a `# /// script`
> dependency header. marimo auto-installs imported packages in WASM, and the
> wheel/pyodide-http are handled at runtime via `micropip` + `notebook_location()`
> (so the wheel URL works on both localhost and GitHub Pages without
> hardcoding). This also keeps the file header tiny — see the **512-byte
> header rule** in `marimo/AGENTS.md`, which a long PEP 723 block would break.

## Building & previewing locally

```bash
# one notebook (or omit --notebook to build all configured):
uv run python -m scripts.build_wasm_site --notebook queue_window_state --site-dir _site_wasm

python -m http.server --directory _site_wasm 8000
# open http://localhost:8000/  →  hard-reload (Cmd-Shift-R) after each rebuild
```

First load takes ~30–60 s (Pyodide runtime + package installs); watch the
browser devtools console. `_site_wasm/` is gitignored.

## Verifying without a browser

The full browser run **cannot be exercised by an automated/agent
environment** here — see [Environment limits](#environment-limits). Do as
much as possible offline, then a human hard-reloads the served page:

- **Slim == full equivalence.** Reproduce the notebook's compute on both the
  full data (`load_pr_interval_data("data")`) and the slimmed data
  (`wasm_io.load_slimmed_data("<out>/public")`) and compare summary numbers.
  Because slimming is column-only, they must match exactly; a mismatch or a
  `ColumnNotFoundError` means the spec dropped a needed column.
- **Wheel self-sufficiency.** Install *only* the built wheel + runtime deps
  into a throwaway venv (`uv pip install --python <venv> <wheel> polars pandas
  numpy pyarrow matplotlib scipy pyyaml`), then from a cwd outside the repo
  import `qb_notebook.wasm_io`, load the slimmed data, and run a helper. This
  proves the wheel ships every module the notebook needs (it caught nothing
  missing, but would catch an un-packaged module).
- `uv run marimo check <nb>` and `uv run ruff check/format`.
- HTTP smoke test: `python -m http.server` the site and `curl` the
  `index.html`, `public/manifest.json`, each `.parquet` (expect the `PAR1`
  magic bytes), and the wheel — all should be `200`.

## Gotchas / known issues

- **pyarrow is required.** marimo patches `pl.read_parquet` to route through
  pyarrow in WASM (`marimo/_runtime/_wasm/_polars.py`). It must be in the
  bootstrap install list even though polars can normally read parquet itself.
- **Library version gaps → `apply_wasm_compat_shims()`.** Pyodide ships older
  libs than the repo pins. Known shim: matplotlib `boxplot(tick_labels=...)`
  → `labels=` on matplotlib < 3.9. **Expect more to surface** as other
  notebooks are converted (other matplotlib/polars/scipy APIs); add each as a
  small, version-guarded patch in `apply_wasm_compat_shims` so every notebook
  benefits. The shim is a no-op on new-enough libraries (and locally).
- **Wheel deps=False.** The wheel's METADATA lists the full dev dep set
  (jupyterlab, kaleido, …), which would fail under micropip. Install it with
  `deps=False` and install the real runtime deps explicitly.
- **Wheel version is pinned in two places:** the notebook bootstrap cell
  (`qb_notebook-0.1.0-...whl`) and `build_wasm_site.WHEEL_NAME`. Bump both
  together if `pyproject.toml`'s version changes (the build asserts they match).
- **`qb_notebook.__init__` is eager** — importing the package pulls
  `.plotting` (matplotlib + scipy) and `.artifacts`, so the runtime dep set is
  polars/pandas/numpy/matplotlib/scipy/pyyaml even for a notebook that only
  touches `data_io`.
- **Data is duplicated per notebook** (each `<nb>/public/` ships its own
  parquet). Fine for now; if the total gets large, factor out a shared data
  dir and point `load_slimmed_data` at a relative `../_data` URL.
- **Team-overlay notebooks** (`area_health`, `reviewer_load`,
  `review_state_machine`) need `../leanprover-community.github.io` locally. In
  WASM that checkout is absent — ship a `teams.json` snapshot
  (`python -m qb_notebook.teams --repo ... --output ...`) into `public/` and
  load that, or let them fall through to empty team sets.

### Environment limits

This matters for an automated/agent session: the dev sandbox here **cannot**
create POSIX semaphores (`sem_open` → `EPERM`, no `/dev/shm`) and **cannot**
launch Chromium (it SIGSEGVs). So:

- `marimo export html` and `marimo run` (which spawn a kernel subprocess via
  `multiprocessing`) **fail** — run those in a normal terminal.
- Headless-browser testing **fails** — a human must do the browser check.
- `marimo export html-wasm`, `marimo check`, `uv build`, and the slimming /
  HTTP / isolated-venv checks above all **work** (no kernel, no browser).

## Remaining work

- Convert the other 8 notebooks using the recipe above; extend `SPECS`.
- Add shims to `apply_wasm_compat_shims` for each version gap that surfaces.
- Add a GitHub Pages workflow: on push, `uv sync`, fetch the data artifact
  (`download_artifact.py`), run `build_wasm_site.py --site-dir _site`, then
  `actions/upload-pages-artifact` + `actions/deploy-pages`. Note CI has the
  same no-kernel constraint, but `export html-wasm` is fine there.
- Consider a small reusable "slim == full" verification harness rather than a
  per-notebook throwaway script.

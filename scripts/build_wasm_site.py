"""Build the static WASM notebook site (for local preview and GitHub Pages).

For each notebook with a slimming spec in ``scripts.export_wasm_data.SPECS``:

  1. ``marimo export html-wasm <nb> -o <site>/<nb> --mode run``  (static bundle)
  2. export slimmed parquet + manifest -> ``<site>/<nb>/public/``
  3. copy the ``qb_notebook`` wheel        -> ``<site>/<nb>/public/``

and writes a landing ``<site>/index.html`` linking the notebooks.

The notebooks fetch ``public/<table>.parquet`` and micropip-install the wheel
from ``public/`` at runtime via ``qb_notebook.wasm_io`` (see each notebook's
WASM bootstrap cell). ``marimo export html-wasm`` only bundles the notebook
source + frontend assets; it does not run a kernel, so it works anywhere
(unlike ``marimo export html`` / ``marimo run``).

Run locally (needs the raw ``data/`` dir present)::

    uv run python -m scripts.build_wasm_site --site-dir _site_wasm
    python -m http.server --directory _site_wasm 8000
    # open http://localhost:8000/

Build a single notebook::

    uv run python -m scripts.build_wasm_site --notebook queue_window_state
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from scripts.export_wasm_data import SPECS, export_notebook
from qb_notebook.data_io import load_pr_interval_data

REPO_ROOT = Path(__file__).resolve().parents[1]
MARIMO_DIR = REPO_ROOT / "marimo"

# Must match the wheel name referenced in each notebook's WASM bootstrap cell.
WHEEL_NAME = "qb_notebook-0.1.0-py3-none-any.whl"


def build_wheel(dest_dir: Path) -> Path:
    """Build the qb_notebook wheel into ``dest_dir`` and return its path."""
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            ["uv", "build", "--wheel", "-o", tmp],
            cwd=REPO_ROOT,
            check=True,
        )
        built = list(Path(tmp).glob("*.whl"))
        if not built:
            raise RuntimeError("uv build produced no wheel")
        wheel = built[0]
        if wheel.name != WHEEL_NAME:
            raise RuntimeError(
                f"built wheel {wheel.name!r} != expected {WHEEL_NAME!r}; "
                "bump WHEEL_NAME here and in the notebook bootstrap cells"
            )
        dest_dir.mkdir(parents=True, exist_ok=True)
        target = dest_dir / wheel.name
        shutil.copy2(wheel, target)
        return target


def export_wasm_html(notebook: str, out_dir: Path) -> None:
    """Run ``marimo export html-wasm`` for one notebook into ``out_dir``."""
    nb_path = MARIMO_DIR / f"{notebook}.py"
    if not nb_path.exists():
        raise FileNotFoundError(nb_path)
    subprocess.run(
        [
            "uv",
            "run",
            "marimo",
            "export",
            "html-wasm",
            str(nb_path),
            "-o",
            str(out_dir),
            "--mode",
            "run",
        ],
        cwd=REPO_ROOT,
        check=True,
    )


def build_notebook(
    notebook: str,
    data: dict,
    wheel: Path,
    site_dir: Path,
) -> None:
    out_dir = site_dir / notebook
    export_wasm_html(notebook, out_dir)
    public = out_dir / "public"
    export_notebook(notebook, data, public)
    shutil.copy2(wheel, public / wheel.name)


def write_landing(site_dir: Path, notebooks: list[str]) -> None:
    items = "\n".join(
        f'      <li><a href="./{nb}/">{nb.replace("_", " ")}</a></li>'
        for nb in notebooks
    )
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>mathlib4 review-analysis notebooks</title>
    <style>
      body {{ font-family: system-ui, sans-serif; max-width: 48rem; margin: 3rem auto; padding: 0 1rem; line-height: 1.5; }}
      h1 {{ font-size: 1.5rem; }}
      li {{ margin: 0.35rem 0; }}
      .note {{ color: #555; font-size: 0.9rem; }}
    </style>
  </head>
  <body>
    <h1>mathlib4 review-analysis notebooks</h1>
    <p class="note">Interactive marimo notebooks running entirely in your
      browser via WebAssembly (Pyodide). First load downloads the Python
      runtime and a few MB of pre-slimmed data, then runs locally — nothing
      is sent to a server.</p>
    <ul>
{items}
    </ul>
  </body>
</html>
"""
    (site_dir / "index.html").write_text(html)
    (site_dir / ".nojekyll").touch()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--notebook", help="build only this notebook")
    ap.add_argument("--data-dir", default="data", help="raw parquet directory")
    ap.add_argument("--site-dir", default="_site_wasm", help="output site dir")
    args = ap.parse_args()

    notebooks = [args.notebook] if args.notebook else sorted(SPECS)
    unknown = [nb for nb in notebooks if nb not in SPECS]
    if unknown:
        ap.error(f"no WASM spec for: {unknown} (known: {sorted(SPECS)})")

    site_dir = Path(args.site_dir)
    print(f"Building WASM site for {notebooks} -> {site_dir}")

    print("Building qb_notebook wheel ...")
    wheel = build_wheel(site_dir / "_wheel_stage")

    print(f"Loading raw data from {args.data_dir} ...")
    data = load_pr_interval_data(args.data_dir)

    for nb in notebooks:
        print(f"\n=== {nb} ===")
        build_notebook(nb, data, wheel, site_dir)

    shutil.rmtree(site_dir / "_wheel_stage", ignore_errors=True)
    write_landing(site_dir, notebooks)

    print(f"\nDone. Serve with:\n  python -m http.server --directory {site_dir} 8000")
    print("Then open http://localhost:8000/")


if __name__ == "__main__":
    main()

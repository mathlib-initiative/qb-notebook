# Queueboard notebook

Utilities and dependencies for exploring the sanitized parquet dump under `local-sanitize/data`.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/)

## Quickstart
- Sync the environment: `uv sync`
- Create a venv: `uv venv`
- Open `.ipynb` notebooks VS Code and select the kernel corresponding to the venv just created

## Useful snippets
```python
from download_artifact import download_and_extract_latest_successful_workflow_artifacts
from pathlib import Path
import pandas as pd
import polars as pl

info = download_and_extract_latest_successful_workflow_artifacts(
    repo="leanprover-community/queueboard-core",
    workflow="upload_backup.yaml",
    out_dir="./data",
    artifact_name="analytics-datasets",
    branch="master",
    search_limit=100,  # change this if you expect there to be > 100 failed runs before the first successful one
)

data_dir = Path("data")

# Pandas + PyArrow
df = pd.read_parquet(data_dir / "core_repository.parquet")

# Polars (fast, lazy)
lazy = pl.scan_parquet(data_dir.glob("*.parquet"))
agg = lazy.group_by("owner_login").agg(pl.len()).collect()
print(agg)
```

## Included tools
- pandas/pyarrow and polars for parquet IO and data wrangling
- matplotlib, altair, seaborn, plotly for plotting
- scipy and statsmodels for statistical tests/modeling
- jupyterlab and ipykernel for notebooks

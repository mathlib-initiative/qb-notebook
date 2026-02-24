"""Compatibility shim for older notebook imports."""

from qb_notebook.artifacts import (
    GHArtifactError,
    download_and_extract_latest_successful_workflow_artifacts,
)

__all__ = [
    "GHArtifactError",
    "download_and_extract_latest_successful_workflow_artifacts",
]


if __name__ == "__main__":
    download_and_extract_latest_successful_workflow_artifacts(
        repo="leanprover-community/queueboard-core",
        workflow="upload_backup.yaml",
        out_dir="./data",
        artifact_name="analytics-datasets",
        branch="master",
        search_limit=100,
    )

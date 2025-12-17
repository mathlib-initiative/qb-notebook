import os
import json
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Union, List


class GHArtifactError(RuntimeError):
    pass


def _run(cmd: List[str], cwd: Optional[Union[str, Path]] = None) -> str:
    try:
        my_env = os.environ.copy()
        my_env["NO_COLOR"] = "true"
        my_env["CLICOLOR_FORCE"] = "0"
        out = subprocess.check_output(
            cmd,
            env=my_env,
            cwd=str(cwd) if cwd is not None else None,
            stderr=subprocess.PIPE,
            encoding="utf-8-sig",
            text=True,
        )
        return out
    except subprocess.CalledProcessError as e:
        raise GHArtifactError(
            f"Command failed (exit {e.returncode}): {' '.join(cmd)}\n\nOutput:\n{e.output}"
        ) from e


def download_and_extract_latest_successful_workflow_artifacts(
    repo: str,
    workflow: str,
    out_dir: Union[str, Path] = "./artifacts",
    artifact_name: Optional[str] = None,
    branch: Optional[str] = None,
    event: Optional[str] = None,
    search_limit: int = 50,
) -> dict:
    """
    Download and extract artifact(s) from the latest *successful* GitHub Actions run of a workflow.
    """
    out_dir = Path(out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Get recent runs, pick the first successful one
    cmd = [
        "gh",
        "run",
        "list",
        "--repo",
        repo,
        "--workflow",
        workflow,
        "--limit",
        str(search_limit),
        "--json",
        "databaseId,headBranch,event,status,conclusion,createdAt,displayTitle,url",
        "--jq",
        ".",
    ]
    if branch:
        cmd += ["--branch", branch]
    if event:
        cmd += ["--event", event]

    runs = json.loads(_run(cmd))
    if not runs:
        raise GHArtifactError(
            f"No runs found for workflow={workflow!r} in repo={repo!r}"
            + (f" on branch={branch!r}" if branch else "")
            + (f" with event={event!r}" if event else "")
        )

    run = next((r for r in runs if r.get("conclusion") == "success"), None)
    if not run:
        raise GHArtifactError(
            f"No successful runs found in the latest {search_limit} runs for workflow={workflow!r} "
            f"in repo={repo!r}"
            + (f" on branch={branch!r}" if branch else "")
            + (f" with event={event!r}" if event else "")
            + ". Try increasing search_limit."
        )

    run_id = str(run["databaseId"])

    # 2) Download and extract files from artifact for that run
    download_dir = out_dir
    if download_dir.exists():
        shutil.rmtree(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    dl_cmd = [
        "gh",
        "run",
        "download",
        run_id,
        "--repo",
        repo,
        "--dir",
        str(download_dir),
    ]
    if artifact_name:
        dl_cmd += ["--name", artifact_name]

    _run(dl_cmd)

    extracted_files = []
    # gh run download typically extracts each artifact into its own subdir under --dir.
    # Capture files in those artifact folders
    extracted_files = sorted(
        str(p) for p in download_dir.iterdir() if not p.name.startswith(".")
    )

    return {
        "repo": repo,
        "workflow": workflow,
        "run_id": run_id,
        "run_url": run.get("url"),
        "run_title": run.get("displayTitle"),
        "run_branch": run.get("headBranch"),
        "run_event": run.get("event"),
        "run_status": run.get("status"),
        "run_conclusion": run.get("conclusion"),
        "created_at": run.get("createdAt"),
        "extracted_files": extracted_files,
    }


if __name__ == "__main__":
    download_and_extract_latest_successful_workflow_artifacts(
        repo="leanprover-community/queueboard-core",
        workflow="upload_backup.yaml",
        out_dir="./data",
        artifact_name="analytics-datasets",
        branch="master",
        search_limit=100,  # change this if you expect there to be > 100 failed runs before the first successful one
    )

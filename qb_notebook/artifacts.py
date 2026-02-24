from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path


class GHArtifactError(RuntimeError):
    """Raised when GitHub artifact fetch/download fails."""


def _run(cmd: list[str], cwd: str | Path | None = None) -> str:
    try:
        my_env = os.environ.copy()
        my_env["NO_COLOR"] = "true"
        my_env["CLICOLOR_FORCE"] = "0"
        return subprocess.check_output(
            cmd,
            env=my_env,
            cwd=str(cwd) if cwd is not None else None,
            stderr=subprocess.PIPE,
            encoding="utf-8-sig",
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise GHArtifactError(
            f"Command failed (exit {e.returncode}): {' '.join(cmd)}\n\nOutput:\n{e.output}"
        ) from e


def download_and_extract_latest_successful_workflow_artifacts(
    repo: str,
    workflow: str,
    out_dir: str | Path = "./artifacts",
    artifact_name: str | None = None,
    branch: str | None = None,
    event: str | None = None,
    search_limit: int = 50,
) -> dict:
    """
    Download and extract artifact(s) from the latest successful run of a workflow.
    """
    out_dir = Path(out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

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

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dl_cmd = [
        "gh",
        "run",
        "download",
        run_id,
        "--repo",
        repo,
        "--dir",
        str(out_dir),
    ]
    if artifact_name:
        dl_cmd += ["--name", artifact_name]

    _run(dl_cmd)

    extracted_files = sorted(
        str(p) for p in out_dir.iterdir() if not p.name.startswith(".")
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and extract artifact(s) from the latest successful workflow run."
    )
    parser.add_argument("--repo", required=True, help="Repository, e.g. owner/name.")
    parser.add_argument("--workflow", required=True, help="Workflow filename or name.")
    parser.add_argument(
        "--out-dir",
        default="./artifacts",
        help="Directory where artifacts are extracted.",
    )
    parser.add_argument(
        "--artifact-name",
        default=None,
        help="Optional artifact name to download from the run.",
    )
    parser.add_argument(
        "--branch",
        default=None,
        help="Optional branch filter for workflow runs.",
    )
    parser.add_argument(
        "--event",
        default=None,
        help="Optional event filter for workflow runs.",
    )
    parser.add_argument(
        "--search-limit",
        type=int,
        default=50,
        help="Number of recent runs to inspect for a successful run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    info = download_and_extract_latest_successful_workflow_artifacts(
        repo=args.repo,
        workflow=args.workflow,
        out_dir=args.out_dir,
        artifact_name=args.artifact_name,
        branch=args.branch,
        event=args.event,
        search_limit=args.search_limit,
    )
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()

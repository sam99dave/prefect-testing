"""
Deployment script for both flows using ``flow.from_source().deploy()``.

Managed work pools provision their own compute with official Prefect images.
Flow source code must live in a remote location (git, S3, …) so the pool
can clone/download it at runtime.  This script points at the GitHub repo.

Prerequisites:
- Authenticated to Prefect Cloud: ``prefect cloud login``
- Work pool ``my-managed-pool`` exists (type: Prefect Managed)
- Code committed and pushed to the GitHub repo below

Usage:
  python deploy.py
"""

from __future__ import annotations

from uuid import UUID

from prefect import flow

GITHUB_REPO = "https://github.com/sam99dave/prefect-testing.git"
DEFAULT_WORK_POOL_NAME = "my-managed-pool"


def deploy_all_flows(
    *,
    repo_url: str,
    work_pool_name: str,
) -> list[UUID]:
    """
    Register both flow deployments using ``flow.from_source().deploy()``.

    Each flow is loaded from the remote git repo so the managed work pool can
    clone the source at runtime.  No custom Docker image is needed — the pool
    picks an official ``prefecthq`` image automatically.

    Args:
        repo_url: HTTPS URL of the git repository containing the flow files.
        work_pool_name: Prefect work pool that will execute scheduled runs.

    Returns:
        Deployment UUIDs (hours_to_seconds, date_checker).
    """
    deployment_ids: list[UUID] = []

    deployment_ids.append(
        flow.from_source(
            source=repo_url,
            entrypoint="even_odd_flow.py:hours_to_seconds",
        ).deploy(
            name="hours-to-seconds-deployment",
            work_pool_name=work_pool_name,
            tags=["hts"],
            build=False,
            push=False,
            print_next_steps=False,
        )
    )

    deployment_ids.append(
        flow.from_source(
            source=repo_url,
            entrypoint="hello_flow.py:date_checker",
        ).deploy(
            name="date-checker-deployment",
            work_pool_name=work_pool_name,
            build=False,
            push=False,
            print_next_steps=True,
        )
    )

    return deployment_ids


if __name__ == "__main__":
    print("Deploying flows to Prefect Cloud managed work pool...")
    print(f"Work pool : {DEFAULT_WORK_POOL_NAME}")
    print(f"Source    : {GITHUB_REPO}")

    ids = deploy_all_flows(
        repo_url=GITHUB_REPO,
        work_pool_name=DEFAULT_WORK_POOL_NAME,
    )

    print(f"\nDeployment IDs: {[str(i) for i in ids]}")
    print("View deployments at: https://app.prefect.cloud")

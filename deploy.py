"""
Deployment script for both flows using each flow's ``deploy()`` method.
Registers deployments with Prefect in a single script run.

Prerequisites:
- Authenticated to Prefect Cloud: prefect cloud login
- Work pool 'my-managed-pool' exists in Prefect Cloud UI (managed pool)

Managed work pools handle image selection and source code upload automatically.
Do NOT pass a custom ``image`` — Prefect picks an official image and uploads
your flow source to cloud storage so the container can download it at runtime.

Usage:
  python deploy.py
"""

from __future__ import annotations

from uuid import UUID

from even_odd_flow import hours_to_seconds
from hello_flow import date_checker

DEFAULT_WORK_POOL_NAME = "my-managed-pool"


def deploy_all_flows(*, work_pool_name: str) -> list[UUID]:
    """
    Register both flow deployments with Prefect using ``Flow.deploy``.

    Source code is uploaded to Prefect Cloud automatically. The managed pool
    selects an official image and downloads the source at runtime.

    Args:
        work_pool_name: Prefect work pool that will execute scheduled runs.

    Returns:
        Deployment UUIDs (hours_to_seconds, date_checker).
    """
    deployment_ids: list[UUID] = []

    deployment_ids.append(
        hours_to_seconds.deploy(
            name="hours-to-seconds-deployment",
            work_pool_name=work_pool_name,
            print_next_steps=False,
        )
    )

    deployment_ids.append(
        date_checker.deploy(
            name="date-checker-deployment",
            work_pool_name=work_pool_name,
            print_next_steps=True,
        )
    )

    return deployment_ids


if __name__ == "__main__":
    print("Deploying flows to Prefect Cloud managed work pool...")
    print(f"Work pool: {DEFAULT_WORK_POOL_NAME}")

    ids = deploy_all_flows(work_pool_name=DEFAULT_WORK_POOL_NAME)

    print(f"\nDeployment IDs: {[str(i) for i in ids]}")
    print("View deployments at: https://app.prefect.cloud")

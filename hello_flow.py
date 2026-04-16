"""
Date checker flow - validates a list of dates (past or future) in parallel.
Input: list of date strings DD/MM/YYYY
Output: list of results, each with date_check + hours-to-seconds conversion.
"""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    ArtifactFilter,
    ArtifactFilterFlowRunId,
    ArtifactFilterKey,
)
from prefect.deployments import run_deployment


def _parse_artifact_data(raw: Any) -> Optional[Dict[str, Any]]:
    """Strip the markdown code fence and parse the inner JSON."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        raw = raw.strip().removeprefix("```json").removesuffix("```").strip()
        return json.loads(raw)
    return None


async def _read_conversion_artifact_async(
    flow_run_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Read the ``conversion-result`` artifact from a completed flow run
    using the async Prefect SDK client.
    """
    async with get_client() as client:
        artifacts = await client.read_artifacts(
            artifact_filter=ArtifactFilter(
                flow_run_id=ArtifactFilterFlowRunId(any_=[UUID(flow_run_id)]),
                key=ArtifactFilterKey(any_=["conversion-result"]),
            ),
            limit=1,
        )

    if not artifacts:
        return None

    return _parse_artifact_data(artifacts[0].data)


def _read_conversion_artifact(flow_run_id: str) -> Optional[Dict[str, Any]]:
    """
    Sync wrapper — runs the async SDK call in a fresh event loop.

    Prefect's older managed-pool runtime doesn't use anyio worker threads,
    so ``asyncio.run`` is the safest portable bridge.
    """
    return asyncio.run(_read_conversion_artifact_async(flow_run_id))


@task
def check_single_date(date_str: str) -> Dict[str, Any]:
    """
    Parse one date string and compute its hour difference from now.

    Returns a dict with the original string and the check result.
    """
    try:
        input_date = datetime.strptime(date_str, "%d/%m/%Y")
    except ValueError as exc:
        return {
            "date_str": date_str,
            "date_result": {
                "result": None,
                "status": f"Failed: Invalid date format - {exc}",
            },
        }

    now = datetime.now()
    if input_date.date() > now.date():
        date_result = {"result": None, "status": "Failed"}
    else:
        hours_diff = (now - input_date).total_seconds() / 3600
        date_result = {"result": round(hours_diff, 2), "status": "Completed"}

    return {"date_str": date_str, "date_result": date_result}


@task
def trigger_and_wait_for_conversion(
    date_result: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Trigger the hours-to-seconds deployment, block until it completes,
    then read the result back via the Prefect artifact SDK.

    Designed to be ``.submit()``-ed so many instances run in parallel.
    """
    print(f"Triggering conversion for: {date_result}")

    flow_run = run_deployment(
        name="hours-to-seconds-flow/hours-to-seconds-deployment",
        parameters={"result_from_date_checker": date_result},
        tags=["flowc"],
        as_subflow=False,
    )

    print(f"Flow run {flow_run.name} finished — state: {flow_run.state.name}")

    conversion_result = _read_conversion_artifact(str(flow_run.id))
    print(f"Conversion result: {conversion_result}")
    return conversion_result


@flow(name="date-checker-flow", log_prints=True)
def date_checker(date_strings: List[str]) -> List[Dict[str, Any]]:
    """
    Check multiple dates and convert hours-to-seconds in parallel.

    Input: list of date strings in DD/MM/YYYY format.
    Returns: list of ``{date_str, date_check, conversion}`` dicts, one per
    input date, in the same order.

    All hours-to-seconds deployments run concurrently on the managed pool
    (each in its own container) via ``.submit()``.
    """
    print(f"Received {len(date_strings)} date(s): {date_strings}")

    # Phase 1 — check every date (lightweight, runs locally in the flow)
    checked = [check_single_date(ds) for ds in date_strings]
    print(f"Date check results: {[c['date_result']['status'] for c in checked]}")

    # Phase 2 — fan-out: submit all conversions concurrently
    conversion_futures = [
        trigger_and_wait_for_conversion.submit(item["date_result"])
        for item in checked
    ]

    # Phase 3 — fan-in: collect results in original order
    results: List[Dict[str, Any]] = []
    for item, future in zip(checked, conversion_futures):
        results.append(
            {
                "date_str": item["date_str"],
                "date_check": item["date_result"],
                "conversion": future.result(),
            }
        )

    print(f"Final collated output ({len(results)} entries):")
    for r in results:
        print(f"  {r['date_str']}: {r['conversion']}")

    return results

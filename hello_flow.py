"""
Date checker flow - validates if date is past or future.
Input: date string DD/MM/YYYY
Output: Combined result from date checker + hours-to-seconds conversion.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

import httpx

from prefect import flow, task
from prefect.deployments import run_deployment
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL


@task
def parse_date(date_str: str) -> datetime:
    """Parse date string in DD/MM/YYYY format."""
    return datetime.strptime(date_str, "%d/%m/%Y")


@task
def calculate_hour_difference(input_date: datetime) -> Dict[str, Any]:
    """
    Calculate hour difference between now and input date.

    Returns Failed if future date.
    """
    now = datetime.now()

    if input_date.date() > now.date():
        return {"result": None, "status": "Failed"}

    diff = now - input_date
    hours_diff = diff.total_seconds() / 3600

    return {"result": round(hours_diff, 2), "status": "Completed"}


def _read_conversion_artifact(flow_run_id: str) -> Optional[Dict[str, Any]]:
    """
    Read the ``conversion-result`` artifact via the Prefect REST API.

    Uses httpx directly so it works on any ``prefect-client`` version
    (the sync SDK client doesn't expose ``read_artifacts`` everywhere).
    The artifact body is a markdown-fenced JSON block — strip the fence
    and parse.
    """
    api_url = PREFECT_API_URL.value()
    api_key = PREFECT_API_KEY.value()

    resp = httpx.post(
        f"{api_url}/artifacts/filter",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "artifacts": {
                "flow_run_id": {"any_": [flow_run_id]},
                "key": {"any_": ["conversion-result"]},
            },
            "limit": 1,
            "sort": "CREATED_DESC",
        },
    )
    resp.raise_for_status()
    items = resp.json()

    if not items:
        return None

    raw: Any = items[0].get("data")
    if isinstance(raw, str):
        raw = raw.strip().removeprefix("```json").removesuffix("```").strip()
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return None


@task
def trigger_and_wait_for_conversion(date_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Trigger the hours-to-seconds deployment, block until it completes,
    then read the result back via the Prefect artifact REST API.
    """
    print(f"Triggering hours-to-seconds deployment with input: {date_result}")

    flow_run = run_deployment(
        name="hours-to-seconds-flow/hours-to-seconds-deployment",
        parameters={"result_from_date_checker": date_result},
        as_subflow=False,
    )

    print(f"Flow run {flow_run.name} finished — state: {flow_run.state.name}")

    conversion_result = _read_conversion_artifact(str(flow_run.id))
    print(f"Conversion result: {conversion_result}")
    return conversion_result


@flow(name="date-checker-flow", log_prints=True)
def date_checker(date_str: str) -> Dict[str, Any]:
    """
    Check if date is past or future, trigger conversion, and return combined output.

    Input format: DD/MM/YYYY
    Returns the hours-to-seconds conversion result when the date is valid and
    in the past, otherwise returns the error / failure from the date check.
    """
    print(f"Input date: {date_str}")

    try:
        input_date = parse_date(date_str)
        date_result = calculate_hour_difference(input_date)
        print(f"Date checker result: {date_result}")

        conversion_result = trigger_and_wait_for_conversion(date_result)

        final_output: Dict[str, Any] = {
            "date_check": date_result,
            "conversion": conversion_result,
        }
        print(f"Final output: {final_output}")
        return final_output

    except ValueError as e:
        error_result: Dict[str, Any] = {
            "result": None,
            "status": f"Failed: Invalid date format - {e}",
        }
        print(f"Error: {error_result}")
        return error_result

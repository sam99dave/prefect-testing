"""
Hours to seconds converter flow.
Takes the result from date_checker flow and converts hours to seconds.
"""

import json
import time
from typing import Any, Dict

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

ARTIFACT_KEY = "conversion-result"


@task(tags=["time-conversion"])
def convert_hours_to_seconds(hours: float) -> float:
    """Convert hours to seconds."""
    print(f"Sleeping 10s to simulate work (hours={hours})...")
    time.sleep(10)
    return hours * 3600


@task
def process_result(result_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process the result from date_checker flow.

    If status is Completed, convert hours to seconds.
    """
    if result_dict.get("status") == "Completed":
        hours = result_dict.get("result")
        if hours is not None:
            seconds = convert_hours_to_seconds(hours)
            return {
                "result": round(seconds, 2),
                "original_hours": hours,
                "status": "Converted",
            }

    return {
        "result": None,
        "original_status": result_dict.get("status"),
        "status": "Skipped",
    }


@flow(name="hours-to-seconds-flow", log_prints=True)
def hours_to_seconds(result_from_date_checker: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert hours to seconds if date checker completed successfully.

    Input: {"result": hour_difference or None, "status": "Completed" or "Failed"}
    Returns: {"result": seconds or None, "status": "Converted" or "Skipped"}

    The result is also published as a Prefect artifact so callers on
    different infrastructure (e.g. managed pool containers) can read it
    back via the API without shared filesystem storage.
    """
    print(f"Received input: {result_from_date_checker}")

    result = process_result(result_from_date_checker)
    print(f"Output: {result}")

    create_markdown_artifact(
        markdown=f"```json\n{json.dumps(result, indent=2)}\n```",
        key=ARTIFACT_KEY,
        description="Conversion result from hours-to-seconds flow",
    )

    return result

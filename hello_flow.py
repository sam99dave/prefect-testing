"""
Date checker flow - validates if date is past or future.
Input: date string DD/MM/YYYY
Output: Combined result from date checker + hours-to-seconds conversion.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from prefect import flow, task
from prefect.deployments import run_deployment


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


@task
def trigger_and_wait_for_conversion(date_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Trigger the hours-to-seconds deployment via ``run_deployment``, block
    until the remote flow run reaches a terminal state, then return its result.
    """
    print(f"Triggering hours-to-seconds deployment with input: {date_result}")

    flow_run = run_deployment(
        name="hours-to-seconds-flow/hours-to-seconds-deployment",
        parameters={"result_from_date_checker": date_result},
        as_subflow=False,
    )

    print(f"Flow run {flow_run.name} finished — state: {flow_run.state.name}")

    conversion_result: Optional[Dict[str, Any]] = flow_run.state.result(
        raise_on_failure=False,
    )
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

        final_output = {
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

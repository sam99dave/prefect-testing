"""
Date checker flow - validates if date is past or future.
Input: date string DD/MM/YYYY
Output: {"result": hour_difference or None, "status": "Completed" or "Failed"}
"""

from datetime import datetime
from prefect import flow, task
from prefect.deployments import run_deployment


@task
def parse_date(date_str: str) -> datetime:
    """Parse date string in DD/MM/YYYY format."""
    return datetime.strptime(date_str, "%d/%m/%Y")


@task
def calculate_hour_difference(input_date: datetime) -> dict:
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
def trigger_hours_to_seconds_conversion(date_result: dict):
    """
    Task that triggers the hours-to-seconds flow deployment using Prefect SDK.
    """
    print(f"Task: Triggering hours-to-seconds deployment with input: {date_result}")

    flow_run = run_deployment(
        name="hours-to-seconds-flow/hours-to-seconds-deployment",
        parameters={"result_from_date_checker": date_result},
        timeout=0,
        as_subflow=False,
    )

    return {
        "triggered_flow_run_id": str(flow_run.id),
        "triggered_flow_run_state": str(flow_run.state.name)
        if flow_run.state
        else None,
    }


@flow(name="date-checker-flow", log_prints=True)
def date_checker(date_str: str):
    """
    Check if date is past or future.
    Input format: DD/MM/YYYY
    Also triggers hours-to-seconds flow via task.
    """
    print(f"Input date: {date_str}")

    try:
        input_date = parse_date(date_str)
        result = calculate_hour_difference(input_date)
        print(f"Date checker result: {result}")

        # Trigger the second flow via task
        trigger_info = trigger_hours_to_seconds_conversion(result)
        print(f"Triggered conversion flow: {trigger_info}")

        return result

    except ValueError as e:
        error_result = {
            "result": None,
            "status": f"Failed: Invalid date format - {str(e)}",
        }
        print(f"Error: {error_result}")
        return error_result

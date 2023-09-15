from datetime import datetime
import json
import logging
import httpx


def get_anthology(anthology_base_url, studentEnrollmentPeriodId, anthology_api_key):
    url = f"{anthology_base_url}/api/commands/Academics/StudentEnrollmentPeriod/get"
    body = {"payload": {"Id": studentEnrollmentPeriodId}}
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(url=url, data=json.dumps(body), headers=headers, timeout=30.0)
        logging.info(f"Status code of GET: {json.dumps(response.status_code)}")

    response.raise_for_status()
    result = response.json()

    return result


def get_earliest_participation_date(result: dict, earliest: str) -> str:
    FDP = None
    for extended_property in result["payload"]["data"]["extendedProperties"]:
        if extended_property["name"] == "First Date of Student Participation":
            FDP = extended_property["value"]

    logging.info(f"FDP: {FDP}")

    return min(FDP, earliest) if FDP else earliest


def update_request_body(result, earliest, latest, must_update_student_status):
    result, update_FDP, update_LDP = update_FDP_LDP(result, earliest, latest)

    # end if neither FDP/LDP needs to be updated
    if not update_FDP and not update_LDP:
        return result, None, None

    result = modify_anthology_payload(result, must_update_student_status, earliest)
    logging.debug(f"result of update_request_body(): {json.dumps(result, default=str)}")

    return result, update_FDP, update_LDP


def update_FDP_LDP(result, earliest, latest):
    # variables that state whether if FDP and/or LDP need to be updated
    update_FDP = None
    update_LDP = None

    logging.info(f"earliest, latest: {earliest, latest}")

    for row in result["payload"]["data"]["extendedProperties"]:
        if row["name"] == "First Date of Student Participation":
            logging.info(f"FDP: {row['value']}")
            if not row["value"]:
                row["value"] = earliest
                update_FDP = earliest

        if row["name"] == "Last Date of Student Participation":
            logging.info(f"LDP: {row['value']}")
            if not row["value"]:
                row["value"] = latest
                update_LDP = latest
            elif parse_datetime(row["value"]) < parse_datetime(latest):
                row["value"] = latest
                update_LDP = latest

    logging.info(f"update_FDP, update_LDP: {update_FDP, update_LDP}")

    return result, update_FDP, update_LDP


def parse_datetime(datetime_str: str) -> datetime:
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y/%m/%d %I:%M:%S %p",
        "%m/%d/%y %I:%M:%S %p",
        "%d %B %Y %I:%M:%S %p",
        "%d %b %Y %I:%M:%S %p",
        "%B %d, %Y %I:%M:%S %p",
        "%b %d, %Y %I:%M:%S %p",
        "%Y/%m/%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            pass

    raise ValueError(f"Invalid datetime string: {datetime_str}")


def modify_anthology_payload(result, must_update_student_status, earliest):
    # rename "data" with "entity"
    result["payload"]["entity"] = result["payload"].pop("data")

    # remove fields that are not present in the body of the API POST call to update Anthology
    if "count" in result["payload"]:
        del result["payload"]["count"]

    list_of_removals = [
        "notifications",
        "hasError",
        "hasFault",
        "hasWarning",
        "hasValidationError",
        "hasValidationWarning",
        "hasValidationInformation",
        "hasSecurityError",
    ]

    for item in list_of_removals:
        if item in result:
            del result[item]

    # note: here's where the enrollment status codes come into play
    logging.info(f'result["payload"]["entity"]["schoolStatusId"]: {result["payload"]["entity"]["schoolStatusId"]}')

    if must_update_student_status:
        result["payload"]["entity"]["schoolStatusId"] = 13
        # only modify program version start date if it is null, otherwise keep as is
        start_date = get_actual_start_date(result, earliest)
        result["payload"]["entity"]["actualStartDate"] = (
            start_date
            if not result["payload"]["entity"]["actualStartDate"]
            else result["payload"]["entity"]["actualStartDate"]
        )
        result["payload"]["entity"]["schoolStatusChangeDate"] = (
            start_date
            if not result["payload"]["entity"]["schoolStatusChangeDate"]
            or (start_date > result["payload"]["entity"]["schoolStatusChangeDate"])
            else result["payload"]["entity"]["schoolStatusChangeDate"]
        )

    return result


def get_actual_start_date(result: dict, earliest: str) -> str:
    for property in result["payload"]["entity"]["extendedProperties"]:
        if property["name"] == "First Date of Student Participation":
            FDP = property["value"]

    logging.info(f"Within get_actual_start_date(), FDP was {FDP}, and earliest was {earliest}.")

    # returning formatted_earliest if FDP is null
    if not FDP:
        return parse_datetime(earliest).strftime("%Y/%m/%d 00:00:00")

    formatted_FDP = parse_datetime(FDP).strftime("%Y/%m/%d 00:00:00")
    formatted_earliest = parse_datetime(earliest).strftime("%Y/%m/%d 00:00:00")

    return min(formatted_FDP, formatted_earliest)


def post_anthology(anthology_base_url, body, anthology_api_key):
    url = f"{anthology_base_url}/api/commands/Academics/StudentEnrollmentPeriod/UpdateStudentEnrollmentPeriod"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(url=url, headers=headers, data=json.dumps(body), timeout=30.0)
        logging.info(
            f"For the post_anthology() API, the status code was {response.status_code}. response.text: {json.dumps(response.text, default=str)}"
        )
    response.raise_for_status()

    return None


def generate_function_response(
    studentEnrollmentPeriodId: int,
    update_FDP: str,
    update_LDP: str,
    earliest_participation_date: str,
    EAD: str,
    must_update_student_status: bool,
    error_flags: list[str],
    course_status_change_logs: list[dict],
    courses_with_missing_attendance_data: list[int],
) -> dict:
    new_FDP = f"Changed to {update_FDP}" if update_FDP else "No changes made to this field"
    new_LDP = f"Changed to {update_LDP}" if update_LDP else "No changes made to this field"

    return {
        "studentEnrollmentPeriodId": studentEnrollmentPeriodId,
        "FDP": new_FDP,
        "LDP": new_LDP,
        "EAD": EAD,
        "earliest_participation_date": earliest_participation_date,
        "Updated student statuses": must_update_student_status,
        "error_flag_participation_but_no_registered_courses": True
        if "error_flag_participation_but_no_registered_courses" in error_flags
        else False,
        "error_flag_EAD_gt_earliest_participation": True
        if "error_flag_EAD_gt_earliest_participation" in error_flags
        else False,
        "course_status_change_logs": course_status_change_logs,
        "courses_with_missing_attendance_data": courses_with_missing_attendance_data,
    }

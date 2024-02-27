import json
import logging
import httpx
import datetime
from pytz import timezone


def create_student_status_history_record(
    anthology_api_key,
    anthology_base_url,
    studentId,
    studentEnrollmentPeriodId,
    earliest_participation_date,
):
    school_status_history_id = get_school_status_history_id(
        anthology_api_key, anthology_base_url, studentId, studentEnrollmentPeriodId
    )
    current_payload = get_school_status_history(
        school_status_history_id, anthology_api_key, anthology_base_url
    )

    # skip if there's already an "Enrolled" record
    if current_payload["payload"]["data"]["newSchoolStatusId"] == 13:
        return None

    modified_payload = modify_student_status_history_payload(
        current_payload, earliest_participation_date
    )
    status_code = save_student_status_history_record(
        modified_payload, anthology_api_key, anthology_base_url
    )

    return None


def get_school_status_history_id(
    anthology_api_key, anthology_base_url, studentId, studentEnrollmentPeriodId
):
    url = f"{anthology_base_url}/ds/campusnexus/StudentSchoolStatusHistory/CampusNexus.GetStudentEnrollmentStatusChangesList(studentId={studentId},studentEnrollmentPeriodId={studentEnrollmentPeriodId})?$orderby=CreatedDateTime desc"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=30.0)
        logging.info(f"response.text: {response.text}")

    response.raise_for_status()
    results = response.json()

    school_status_history_id = results["value"][0]["Id"]
    logging.info(f"school_status_history_id: {school_status_history_id}")

    return school_status_history_id


def get_school_status_history(
    school_status_history_id, anthology_api_key, anthology_base_url
):
    url = f"{anthology_base_url}/api/commands/Common/StudentSchoolStatusHistory/get"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}
    body = {"payload": {"id": school_status_history_id}}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(
            url=url, headers=headers, data=json.dumps(body), timeout=30.0
        )
        logging.info(f"response.text: {response.text}")

    response.raise_for_status()
    results = response.json()

    return results


def modify_student_status_history_payload(current_payload, earliest_participation_date):
    tz = timezone("US/Eastern")
    current_eastern_time = datetime.datetime.now(tz).isoformat()

    data = current_payload["payload"]["data"]
    modified_payload = {
        "payload": {
            **data,
            "id": 0,
            "previousSchoolStatusId": data["newSchoolStatusId"],
            "previousSystemSchoolStatusId": data["newSystemSchoolStatusId"],
            "newSchoolStatusId": 13,
            "newSystemSchoolStatusId": 13,
            "lastModifiedDateTime": data["createdDateTime"],
            "createdDateTime": current_eastern_time,
            "effectiveDate": earliest_participation_date,
            "statusChangeType": "S",
            "internalNote": "Active Enrollment",
            "note": "Activated by Participation",
            "newEnrollmentStatusId": None,
            "previousEnrollmentStatusId": None,
            "statusBeginDate": None,
            "enrollmentStatusTermId": None,
            "schoolStatusChangeReasonId": 0,
            "statusBeginDate": None,
            "enrollmentStatusNewUnitValue": None,
            "enrollmentStatusPreviousUnitValue": None,
        }
    }

    return modified_payload


def save_student_status_history_record(
    modified_payload, anthology_api_key, anthology_base_url
):
    url = f"{anthology_base_url}/api/commands/Common/StudentSchoolStatusHistory/saveNew"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(
            url=url, headers=headers, data=json.dumps(modified_payload), timeout=30.0
        )
        logging.info(f"response.text: {response.text}")

    response.raise_for_status()

    return response.status_code

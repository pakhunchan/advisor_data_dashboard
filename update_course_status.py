import httpx
import json
import logging


def update_course_statuses(
    anthology_api_key: str,
    anthology_base_url: str,
    anthology_course_ids: list,
    studentEnrollmentPeriodId: int,
    studentId: int,
) -> list[dict]:
    student_courses = get_student_courses(
        anthology_api_key, anthology_base_url, studentEnrollmentPeriodId, studentId
    )

    student_course_ids = [
        course["Id"]
        for course in student_courses
        if (course["ClassSectionId"] in anthology_course_ids)
        and course["Status"] in {"S"}
    ]

    if not student_course_ids:
        return []

    student_course_payloads_unmodified = [
        get_student_course_payload(
            student_course_id, anthology_api_key, anthology_base_url
        )
        for student_course_id in student_course_ids
    ]

    student_course_payloads_modified = [
        modify_student_course_payload(student_course_payload)
        for student_course_payload in student_course_payloads_unmodified
    ]

    course_status_change_logs = [
        update_student_course(
            student_course_payload, anthology_api_key, anthology_base_url
        )
        for student_course_payload in student_course_payloads_modified
        if student_course_payload
    ]

    logging.info(
        f"student_course_status_change_logs: {json.dumps(course_status_change_logs, default=str)}"
    )

    return course_status_change_logs


def get_student_courses(
    anthology_api_key: str,
    anthology_base_url: str,
    studentEnrollmentPeriodId: str,
    studentId: str,
) -> list[dict]:
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        url = f"{anthology_base_url}/ds/campusnexus/StudentCourses?$filter=StudentEnrollmentPeriodId eq {studentEnrollmentPeriodId} and StudentId eq {studentId}"
        headers = {"ApiKey": anthology_api_key}

        response = client.get(url=url, headers=headers, timeout=30.0)

    results = response.json()
    student_courses = results.get("value", [])
    logging.info(f"student_courses: {json.dumps(student_courses, default=str)}")

    return student_courses


def get_student_course_payload(
    student_course_id: list, anthology_api_key: str, anthology_base_url: str
) -> dict:
    url = f"{anthology_base_url}/api/commands/Academics/StudentCourse/get"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}
    body = {"payload": {"Id": student_course_id}}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(url=url, headers=headers, data=json.dumps(body))
        results = response.json()

    return results


def modify_student_course_payload(student_course_payload: dict) -> dict:
    logging.info(
        f"student_course_payload: {json.dumps(student_course_payload, default=str)}"
    )

    modified_payload = {
        "payload": {
            "campusId": student_course_payload["payload"]["data"]["campusId"],
            "entity": student_course_payload["payload"]["data"],
        }
    }

    # only need to update course if status is currently "S - scheduled".
    if modified_payload["payload"]["entity"]["status"] == "S":
        # adding this temporary field "priorCourseStatus". will use + remove it in the next step
        modified_payload["payload"]["priorCourseStatus"] = modified_payload["payload"][
            "entity"
        ]["status"]
        modified_payload["payload"]["entity"]["status"] = "C"
        # Need to add previousStatus = "S"
        return modified_payload

    else:
        return {}


def update_student_course(
    student_course_payload: dict, anthology_api_key: str, anthology_base_url: str
) -> int:
    url = (
        f"{anthology_base_url}/api/commands/Academics/StudentCourse/updateStudentCourse"
    )
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}
    body = student_course_payload

    priorCourseStatus = body["payload"].pop("priorCourseStatus")

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(url=url, headers=headers, data=json.dumps(body))
        logging.info(
            f"Status code of /api/commands/Academics/StudentCourse/updateStudentCourse was {response.status_code}. response.text: {response.text}"
        )
    response.raise_for_status()

    course_status_change_log = {
        "studentId": body["payload"]["entity"]["studentId"],
        "studentEnrollmentPeriodId": body["payload"]["entity"][
            "studentEnrollmentPeriodId"
        ],
        "studentCourseId": body["payload"]["entity"]["id"],
        "priorCourseStatus": priorCourseStatus,
        "newCourseStatus": body["payload"]["entity"]["status"],
        # "status_code": response.status_code
    }
    logging.info(
        f"course_status_change_log: {json.dumps(course_status_change_log, default=str)}"
    )

    return course_status_change_log

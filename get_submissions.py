import httpx
import logging
from time import sleep


def get_submissions(canvas_base_url: str, canvas_bearer_token: str, course_id: int, submitted_since: str) -> list[dict]:
    url = f"{canvas_base_url}/api/v1/courses/{course_id}/students/submissions"
    params = {
        "student_ids": "all",
        "grouped": "true",
        "per_page": 10,
        "page": 1,
        "submitted_since": submitted_since,
    }
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

    results = []

    max_retries = 3
    retries = 0
    while True:
        with httpx.Client() as client:
            try:
                response = client.get(url=url, params=params, headers=headers, timeout=120.0)
                logging.info(f"Status code {response.status_code}. {response.text}")

                response.raise_for_status()
                results.extend(response.json())

                # if there is a next page of results, headers["Link"] will include the phrase: rel="next"
                if "next" not in response.headers.get("Link"):
                    break
                params["page"] += 1
                retries = 0
            except:
                retries += 1
                sleep(2**retries)
                if retries >= max_retries:
                    raise

    return results


def make_student_id_dict(student_payload: list) -> dict:
    student_id_dict = {}

    for student in student_payload:
        studentNumber = str(student["studentNumber"])
        studentEnrollmentPeriodId = str(student["studentEnrollmentPeriodId"])

        if studentNumber not in student_id_dict:
            student_id_dict[studentNumber] = studentEnrollmentPeriodId
        elif isinstance(student_id_dict[studentNumber], str):
            student_id_dict[studentNumber] = [student_id_dict[studentNumber], studentEnrollmentPeriodId]
        else:
            student_id_dict[studentNumber] = [*student_id_dict[studentNumber], studentEnrollmentPeriodId]

    return student_id_dict


def should_not_skip(submission, student, student_id_dict):
    return (
        # "rollcall" not in (submission.get("url") or "")  # keep only non-attendance data
        # and "sis_user_id" in student  # keep only if submission has a student number attached
        # and student_id_dict.get(student["sis_user_id"])  # keep only if student number is in our active student list
        "sis_user_id" in student  # keep only if submission has a student number attached
        and student_id_dict.get(student["sis_user_id"])  # keep only if student number is in our active student list
    )


# function that takes in an assignment submission and updates canvas_student_participation as necessary
def add_submission(canvas_student_participation: dict, studentNumber: str, submitted_at: str, course_id: int) -> None:
    if not submitted_at:
        return
    if canvas_student_participation.get(studentNumber):
        if submitted_at < canvas_student_participation.get(studentNumber, {}).get("earliest"):
            canvas_student_participation[studentNumber]["earliest"] = submitted_at
        if submitted_at > canvas_student_participation.get(studentNumber, {}).get("latest"):
            canvas_student_participation[studentNumber]["latest"] = submitted_at
    else:
        canvas_student_participation[studentNumber] = {
            "earliest": submitted_at,
            "latest": submitted_at,
            "canvas_course_id": course_id,
        }

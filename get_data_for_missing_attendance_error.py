import httpx
import json


def get_student_name(
    anthology_api_key: str, anthology_base_url: str, student_id: str
) -> str:
    url = f"{anthology_base_url}/api/commands/Common/Student/get"
    body = {"payload": {"Id": student_id}}
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(
            url=url, headers=headers, data=json.dumps(body), timeout=30.0
        )
        results = response.json()["payload"]["data"]
        first_name = results.get("firstName", "")
        last_name = results.get("lastName", "")

    return f"{first_name} {last_name}"


def get_course_info(
    canvas_bearer_token: str,
    canvas_base_url: str,
    canvas_term_id: int,
    anthology_course_ids: str,
):
    sis_course_ids = {f"AdClassSched_{course_id}" for course_id in anthology_course_ids}

    url = f"{canvas_base_url}/api/v1/accounts/11/courses"
    page = 1
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

    list_of_courses = []

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        while True:
            params = {
                "per_page": 100,
                "page": page,
                "enrollment_term_id": canvas_term_id,
            }
            response = client.get(url=url, params=params, headers=headers)
            results = response.json()
            list_of_courses.extend(results)

            if "next" not in response.headers.get("Link"):
                break
            page += 1

    courses_with_missing_attendance_data = [
        f"ClassSectionId #{course['sis_course_id'].lstrip('AdClassSched_')} - {course['course_code']}"
        for course in list_of_courses
        if course["sis_course_id"] in sis_course_ids
    ]

    return courses_with_missing_attendance_data

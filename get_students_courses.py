import httpx
import time
import json
import logging


def get_all_students_courses(anthology_api_key: str, anthology_base_url: str, term_id: int) -> list[dict]:
    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/StudentCourses"
    headers = {"ApiKey": anthology_api_key}
    params = {
        # pretty sure Status 'F' = ClassSectionId 0, but adding both conditions just in case
        "$filter": f"TermId eq {term_id} and Status ne 'F' and ClassSectionId ne 0",
        "$select": "Id, StudentId, StudentEnrollmentPeriodId, ClassSectionId",
    }

    with httpx.Client() as client:
        for attempt in range(max_retries + 1):
            try:
                response = client.get(url=url, headers=headers, params=params, timeout=120.0)
                results = response.json()
                logging.info(f"results: {json.dumps(results, default=str)}")
                student_courses = results["value"]
                break
            except Exception as err:
                logging.exception(err)
                if attempt >= max_retries:
                    response.raise_for_status()
                time.sleep(base_delay * 2**attempt)

    return student_courses

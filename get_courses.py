import logging
import httpx
import json
from time import sleep


def get_canvas_courses(
    canvas_bearer_token: str, canvas_base_url: str, term_id: int
) -> list:
    page = 1
    url = f"{canvas_base_url}/api/v1/accounts/11/courses/"
    params = {"per_page": 100, "page": page, "enrollment_term_id": term_id}
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

    results = []

    while True:
        transport = httpx.HTTPTransport(retries=3)
        with httpx.Client(transport=transport) as client:
            response = client.get(
                url=url, params=params, headers=headers, timeout=120.0
            )

        response.raise_for_status()
        results.extend(response.json())

        logging.info(json.dumps(dict(response.headers.multi_items())))
        logging.info(json.dumps(response.headers.get("Link")))

        # if there is a next page of results, headers["Link"] will include the phrase: rel="next"
        if "next" not in response.headers.get("Link"):
            break
        params["page"] += 1

    return results


def get_zero_credit_anthology_courses(
    anthology_api_key: str, anthology_base_url: str
) -> list:
    import httpx

    url = f"{anthology_base_url}/ds/campusnexus/ClassSections"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=30.0)
    response.raise_for_status()

    results = response.json()
    list_of_courses = results["value"]

    zero_credit_anthology_course_ids = [
        f"AdClassSched_{course['Id']}"
        for course in list_of_courses
        if not course["EnrollmentStatusCreditHours"]
    ]
    logging.info(
        f"zero_credit_anthology_course_ids: {json.dumps(zero_credit_anthology_course_ids, default=str)}"
    )

    return zero_credit_anthology_course_ids


def get_exclude_canvas_course_ids(
    canvas_bearer_token: str, canvas_base_url: str, exclude_anthology_course_codes: set
) -> set:
    url = f"{canvas_base_url}/api/v1/accounts/11/courses"
    params = {"per_page": 100, "page": 1, "enrollment_term_id": 8}
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

    list_of_canvas_courses = []
    max_retries = 3
    retries = 0

    while True:
        with httpx.Client() as client:
            try:
                response = client.get(url=url, params=params, headers=headers)
                response.raise_for_status()
                list_of_canvas_courses.extend(response.json())

                if "next" not in response.headers.get("Link"):
                    break
                params["page"] += 1
                retries = 0
            except Exception as err:
                logging.exception(err)
                retries += 1
                if retries >= max_retries:
                    raise
                sleep(2**retries)

    exclude_canvas_course_ids = {
        course["id"]
        for course in list_of_canvas_courses
        if course["course_code"] in exclude_anthology_course_codes
    }

    return exclude_canvas_course_ids

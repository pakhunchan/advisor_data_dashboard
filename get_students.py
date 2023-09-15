import httpx
import json
import logging


def get_school_status_ids(anthology_base_url: str, anthology_api_key: str, school_status_codes: set) -> list:
    url = f"{anthology_base_url}/ds/campusnexus/SchoolStatuses"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=2)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=30.0)
    response.raise_for_status()

    results = response.json()
    school_statuses = results["value"]
    school_status_ids = [status["Id"] for status in school_statuses if status["Code"] in school_status_codes]

    logging.info(f"school_status_ids: {json.dumps(school_status_ids, default=str)}")

    return school_status_ids


def get_students(filtered_school_status_ids: list, anthology_base_url: str, anthology_api_key: str) -> list:
    students = []
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=4)
    with httpx.Client(transport=transport) as client:
        for school_status_id in filtered_school_status_ids:
            url = f"{anthology_base_url}/ds/campusnexus/StudentEnrollmentPeriods?$filter=SchoolStatusId eq {school_status_id}"
            response = client.get(url=url, headers=headers, timeout=30.0)

            results = response.json()
            students.extend(results["value"])

    # for school_status_id in filtered_school_status_ids:
    #     transport = httpx.HTTPTransport(retries=4)
    #     with httpx.Client(transport=transport) as client:
    #         url = f"{anthology_base_url}/ds/campusnexus/StudentEnrollmentPeriods?$filter=SchoolStatusId eq {school_status_id}"
    #         response = client.get(url=url, headers=headers, timeout=30.0)

    #         results = response.json()
    #         students.extend(results["value"])

    #     response.raise_for_status()

    return students


# def get_anthology_term_id(curr_date: str, anthology_api_key: str, anthology_base_url: str) -> tuple[int, list]:
#     headers = {"ApiKey": anthology_api_key}
#     url = f"{anthology_base_url}"

#     url = f"{anthology_base_url}/ds/campusnexus/Terms"
#     headers = {"ApiKey": anthology_api_key}
#     params = {"$select": "Id, StartDate, EndDate"}

#     transport = httpx.HTTPTransport(retries=3)
#     with httpx.Client(transport=transport) as client:
#         response = client.get(url=url, params=params, headers=headers, timeout=15.0)
#         logging.info(f"Status code of /ds/campusnexus/Terms was {response.status_code}. {response.text}")

#     response.raise_for_status()
#     result = response.json()
#     terms = result["value"]

#     active_term_ids = [
#         term["Id"]
#         for term in terms
#         if term["StartDate"] and term["EndDate"] and term["StartDate"] <= curr_date <= term["EndDate"]
#     ]

#     # we are told that term_id 8 should be removed if it gets pulled
#     active_term_ids.remove(8) if 8 in active_term_ids else None

#     logging.info(f"active_term_ids: {json.dumps(active_term_ids, default=str)}")

#     return active_term_ids


# def get_school_statuses(anthology_base_url: str, anthology_api_key: str) -> list:
#     url = f"{anthology_base_url}/ds/campusnexus/SchoolStatuses"
#     headers = {"ApiKey": anthology_api_key}

#     transport = httpx.HTTPTransport(retries=2)
#     with httpx.Client(transport=transport) as client:
#         response = client.get(url=url, headers=headers, timeout=30.0)

#     results = response.json()
#     school_statuses = results["value"]

#     return school_statuses

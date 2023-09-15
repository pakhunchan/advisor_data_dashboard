import httpx
import json
import logging


def get_anthology_term_info(
    anthology_api_key: str, anthology_base_url: str, curr_date: str, exclude_anthology_term_ids: list
) -> tuple[list, list]:
    url = f"{anthology_base_url}/ds/campusnexus/Terms?$select=Id,Code,StartDate,EndDate"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=15.0)
    response.raise_for_status()

    results = response.json()
    list_of_anthology_terms = results["value"]
    logging.info(f"list_of_anthology_terms: {json.dumps(list_of_anthology_terms, default=str)}")

    anthology_term_id = [
        term["Id"]
        for term in list_of_anthology_terms
        if term["StartDate"] <= curr_date <= term["EndDate"] and term["Id"] not in exclude_anthology_term_ids
    ]
    logging.info(f"anthology_term_id: {json.dumps(anthology_term_id, default=str)}")

    anthology_term_code = {
        term["Code"]
        for term in list_of_anthology_terms
        if term["StartDate"] <= curr_date <= term["EndDate"] and term["Id"] not in exclude_anthology_term_ids
    }
    logging.info(f"anthology_term_code: {json.dumps(anthology_term_code, default=str)}")

    return anthology_term_id, anthology_term_code


def get_canvas_term_id(canvas_bearer_token: str, canvas_base_url: str, anthology_term_code: list) -> list:
    url = f"{canvas_base_url}/api/v1/accounts/1/terms?per_page=100"
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=15.0)
    response.raise_for_status()

    results = response.json()
    list_of_canvas_terms = results["enrollment_terms"]

    canvas_term_id = [term["id"] for term in list_of_canvas_terms if term["sis_term_id"] in anthology_term_code]
    logging.info(f"canvas_term_id: {json.dumps(canvas_term_id)}")

    return canvas_term_id

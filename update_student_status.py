import json
import logging
import httpx


def update_student_status(anthology_api_key, anthology_base_url, studentId):
    logging.info("Starting to try to update the status at student level")
    current_payload = get_student_info(anthology_api_key, anthology_base_url, studentId)
    logging.info(
        f'schoolStatusId at the student level: {current_payload["payload"]["data"]["schoolStatusId"]}'
    )

    # skip student if their status is already "Enrolled"
    if current_payload["payload"]["data"]["schoolStatusId"] == 13:
        logging.info(
            f"Student #{studentId} is already set as Enrolled at the student level"
        )
        return None

    modified_payload = modify_student_info_payload(current_payload)
    logging.info(
        f'Modified schoolStatusId at the student level to: {modified_payload["payload"]["schoolStatusId"]}'
    )

    status_code = update_student_info(
        modified_payload, anthology_api_key, anthology_base_url
    )
    logging.info(
        f"Finished updating status at the student level in Anthology. Status code was {status_code}"
    )

    return None


def get_student_info(anthology_api_key, anthology_base_url, studentId):
    url = f"{anthology_base_url}/api/commands/Common/Student/get"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}
    body = {"payload": {"id": studentId}}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(
            url=url, headers=headers, data=json.dumps(body), timeout=30.0
        )
        logging.info(f"response.text: {response.text}")

    response.raise_for_status()
    results = response.json()

    return results


def modify_student_info_payload(current_payload):
    modified_payload = {"payload": current_payload["payload"]["data"]}
    modified_payload["payload"]["schoolStatusId"] = 13

    return modified_payload


def update_student_info(modified_payload, anthology_api_key, anthology_base_url):
    url = f"{anthology_base_url}/api/commands/Common/Student/save"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}
    body = modified_payload

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(
            url=url, headers=headers, data=json.dumps(body), timeout=30.0
        )
        logging.info(f"response.text: {response.text}")
    response.raise_for_status()

    return response.status_code

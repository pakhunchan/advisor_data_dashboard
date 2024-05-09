import httpx
import json
import logging
import time


def get_all_staff_ids(anthology_api_key: str, anthology_base_url: str) -> dict:
    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/Staff"
    headers = {"ApiKey": anthology_api_key}
    params = {"$select": "Id, FullName"}

    with httpx.Client() as client:
        for attempt in range(max_retries + 1):
            try:
                response = client.get(url=url, headers=headers, params=params, timeout=30.0)
                results = response.json()
                logging.info(f"results: {json.dumps(results, default=str)}")
                break
            except Exception as err:
                logging.exception(err)
                if attempt >= max_retries:
                    response.raise_for_status()
                time.sleep(base_delay * 2**attempt)

    staff_list = results["value"]

    staff_id_dict = {staff["Id"]: staff["FullName"] for staff in staff_list}
    logging.info(f"staff_id_dict: {staff_id_dict}")

    return staff_id_dict


def get_advisors_info(anthology_api_key: str, anthology_base_url: str, staff_id_dict: dict) -> dict:
    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/StudentAdvisors"
    headers = {"ApiKey": anthology_api_key}
    params = {"$filter": "AdvisorModule eq 'AD'", "$select": "StaffId, StudentEnrollmentPeriodId"}

    for attempt in range(max_retries + 1):
        try:
            with httpx.Client() as client:
                response = client.get(url=url, headers=headers, params=params)
                results = response.json()
                advisors_list = results["value"]
                logging.info(f"advisors_list: {advisors_list}")
                break
        except Exception as err:
            logging.exception(err)
            if attempt >= max_retries:
                response.raise_for_status()
            time.sleep(base_delay * 2**attempt)

    advisors_dict = {}
    for advisor in advisors_list:
        student_enrollment_period_id = advisor["StudentEnrollmentPeriodId"]
        advisor_id = advisor["StaffId"]
        advisor_name = staff_id_dict[advisor_id]
        # add to advisors_dict
        advisors_dict[student_enrollment_period_id] = advisor_name

    return advisors_dict

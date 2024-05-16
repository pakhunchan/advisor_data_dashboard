import httpx
import time
import logging


def get_prep_program_dict(anthology_api_key: str, anthology_base_url: str) -> dict[int, str]:
    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/StudentAgencyBranches"
    params = {"$expand": "AgencyBranch($select=Name)"}
    headers = {"ApiKey": anthology_api_key}

    with httpx.Client() as client:
        for attempt in range(max_retries + 1):
            try:
                response = client.get(url=url, headers=headers, params=params, timeout=60.0)
                response.raise_for_status()
                results = response.json()
                break
            except Exception as err:
                logging.exception(err)
                if attempt >= max_retries:
                    response.raise_for_status()
                time.sleep(base_delay * 2**max_retries)

    prep_program_list = results["value"]
    logging.info(f"{prep_program_list = }")

    # prep_program_dict = [
    #     {prep_program["StudentId"]: (prep_program.get("AgencyBranch") or {}).get("Name", None)}
    #     for prep_program in prep_program_list
    # ]
    prep_program_dict = {
        prep_program["StudentId"]: (prep_program.get("AgencyBranch") or {}).get("Name", None)
        for prep_program in prep_program_list
    }
    logging.info(f"{prep_program_dict = }")

    return prep_program_dict

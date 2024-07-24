import httpx
import time
import logging


def get_prep_program_dict(
    anthology_api_key: str,
    anthology_base_url: str,
    curr_americorp_agency_branch_ids: set,
    americorp_agency_branch_ids: set,
) -> dict:

    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/StudentAgencyBranches"
    params = {"$expand": "AgencyBranch($select=Name)", "$select": "StudentId,AgencyBranchId"}
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

    program_list = results["value"]
    logging.info(f"{program_list = }")

    # start with a dictionary that appends the programs into a list[str]
    program_dict = {}
    for program in program_list:
        anthology_student_id = program["StudentId"]

        if program["AgencyBranchId"] in curr_americorp_agency_branch_ids:
            program_dict.setdefault(anthology_student_id, {}).setdefault("americorp_status_list", []).append(
                (program.get("AgencyBranch") or {}).get("Name")
            )
        elif program["AgencyBranchId"] not in americorp_agency_branch_ids:
            program_dict.setdefault(anthology_student_id, {}).setdefault("prep_program_list", []).append(
                (program.get("AgencyBranch") or {}).get("Name")
            )

    # create a new dictionary that takes the list[str] and concatenates the strings
    modified_program_dict = {}
    for anthology_student_id, programs in program_dict.items():
        americorp_status = "; ".join(filter(None, programs.get("americorp_status_list", [])))
        prep_program = "; ".join(filter(None, programs.get("prep_program_list", [])))

        modified_program_dict[anthology_student_id] = {
            "americorp_status": americorp_status or None,
            "prep_program": prep_program or None,
        }

    logging.info(f"{modified_program_dict = }")

    return modified_program_dict

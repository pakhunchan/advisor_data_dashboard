import httpx
import asyncio
import logging


async def get_aos_residency_api_data_asynchronously(
    anthology_api_key: str, anthology_base_url: str, students: list[dict]
) -> list[dict]:
    size_of_chunks = 10
    number_of_chunks = len(students) // size_of_chunks + 1

    modified_students = []
    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_aos_residency_api_data(anthology_api_key, anthology_base_url, student, client)
                for student in students[i * size_of_chunks : (i + 1) * size_of_chunks]
            ]

            results = await asyncio.gather(*tasks)
            modified_students.extend(results)

    return modified_students


async def get_aos_residency_api_data(
    anthology_api_key: str, anthology_base_url: str, student: dict, client: httpx.AsyncClient
) -> dict:
    # get data from API
    enrollment_id = student["student_enrollment_period_id"]

    url = f"{anthology_base_url}/ds/campusnexus/StudentEnrollmentAreaOfStudyLists/CampusNexus.GetSavedProgramVersionAreaOfStudyConfig(studentenrollmentperiodid={enrollment_id})"
    headers = {"ApiKey": anthology_api_key}
    params = {"$select": "AreaOfStudyName"}

    max_retries = 3
    base_delay = 2

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url=url, headers=headers, params=params, timeout=30.0)
            response.raise_for_status()
            results = response.json()
            break
        except Exception as err:
            logging.exception(err)
            if attempt >= max_retries:
                response.raise_for_status()
            await asyncio.sleep(base_delay * 2**attempt)

    # prepare AOS and residency data
    modified_student = get_modified_student_data(results, student)

    return modified_student


def get_modified_student_data(results: dict, student) -> dict:
    # get the list of AOS + residency data
    AOS_residency_list = results.get("value", [])

    # retrieve and format the AOS data
    AOS_list = [
        item.get("AreaOfStudyName")
        for item in AOS_residency_list
        if all(keyword not in item.get("AreaOfStudyName") for keyword in ["Residency", "Pathway"])
    ]
    AOS = "; ".join(AOS_list)

    # retrieve and format the residency data
    residency_list = [
        item.get("AreaOfStudyName")
        for item in AOS_residency_list
        if any(keyword in item.get("AreaOfStudyName") for keyword in ["Residency", "Pathway"])
    ]
    residency = "; ".join(residency_list)

    # generate a new student dictionary with AOS and residency added
    modified_student = {
        **student,
        "area_of_study": AOS if AOS else None,
        "residency": residency if residency else None,
    }

    return modified_student

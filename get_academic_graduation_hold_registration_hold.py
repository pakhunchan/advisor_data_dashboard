import httpx
import asyncio
import logging
import json


async def get_graduation_hold_registration_hold_asynchronously(
    anthology_api_key: str, anthology_base_url: str, students: list[dict]
) -> list[dict]:
    size_per_chunk = 10
    number_of_chunks = len(students) // size_per_chunk + 1

    modified_students = []
    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_graduation_and_registration_holds_from_api(anthology_api_key, anthology_base_url, student, client)
                for student in students[i * size_per_chunk : (i + 1) * size_per_chunk]
            ]

            res = await asyncio.gather(*tasks)
            modified_students.extend(res)

    return modified_students


async def get_graduation_and_registration_holds_from_api(
    anthology_api_key: str, anthology_base_url: str, student: dict, client: httpx.AsyncClient
) -> dict:
    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/StudentGroupMembers/CampusNexus.CheckStudentHoldGroup(studentId={student['anthology_student_id']})"
    headers = {"ApiKey": anthology_api_key}

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url=url, headers=headers, timeout=15.0)
            response.raise_for_status()
            results = response.json()
            logging.info(f"{json.dumps(results)}")
            break
        except Exception as err:
            logging.exception(err)
            if attempt >= max_retries:
                response.raise_for_status()
            await asyncio.sleep(base_delay * 2**max_retries)

    # API returned details of all existing holds
    list_of_holds = results.get("value", [])
    existing_holds = {hold["Name"] for hold in list_of_holds}

    academic_graduation_hold = True if "Academic Graduation" in existing_holds else False
    registration_hold = True if "Register" in existing_holds else False

    modified_student = {
        **student,
        "academic_graduation_hold": academic_graduation_hold,
        "registration_hold": registration_hold,
    }

    return modified_student

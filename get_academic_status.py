import httpx
import asyncio
import logging


async def get_academic_status_asynchronously(
    anthology_api_key: str, anthology_base_url: str, students: list[dict]
) -> list[dict]:

    size_per_chunk = 10
    number_of_chunks = len(students) // size_per_chunk + 1

    modified_students = []
    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_academic_status_from_api(anthology_api_key, anthology_base_url, student, client)
                for student in students[i * size_per_chunk : (i + 1) * size_per_chunk]
            ]

            results = await asyncio.gather(*tasks)
            modified_students.extend(results)

        return modified_students


async def get_academic_status_from_api(
    anthology_api_key: str, anthology_base_url: str, student: dict, client: httpx.AsyncClient
) -> dict:
    max_retries = 3
    base_delay = 2

    anthology_student_id = student["anthology_student_id"]

    url = f"{anthology_base_url}/ds/campusnexus/StudentAcademicStatusHistory/CampusNexus.GetStudentAcademicStatusChangesList(studentId = {anthology_student_id})"
    headers = {"ApiKey": anthology_api_key}
    params = {"$orderby": "CreatedDateTime desc"}
    logging.info(f"Running academic status API call for {anthology_student_id} for URL {url}")

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url=url, headers=headers, params=params)
            response.raise_for_status()
            results = response.json()
            break
        except Exception as err:
            logging.exception(err)
            if attempt >= max_retries:
                response.raise_for_status()
            await asyncio.sleep(base_delay * 2**attempt)

    academic_status = (results.get("value") or [{}])[0].get("NewStatusName")
    logging.info(f"{academic_status = }")

    modified_student = {
        **student,
        "academic_status": academic_status,
    }

    return modified_student

import httpx
import asyncio
import logging
import json


async def get_student_data(
    anthology_api_key: str, anthology_base_url: str, student: dict, client: httpx.AsyncClient
) -> dict:

    student_id = student["studentId"]
    url = f"{anthology_base_url}/api/commands/Common/Student/get"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}
    body = {"payload": {"id": student_id}}

    max_retries = 3
    base_delay = 2

    for attempt in range(max_retries + 1):
        try:
            response = await client.post(url=url, headers=headers, data=json.dumps(body), timeout=15.0)
            response.raise_for_status()
            results = response.json()
            logging.info(f"results: {results}")
            break
        except Exception as err:
            logging.exception(err)
            if attempt >= max_retries:
                raise
            await asyncio.sleep(base_delay * 2 ** (attempt))

    formatted_results = {
        "anthology_student_id": int(body["payload"]["id"]),
        "anthology_student_number": int(results["payload"]["data"]["studentNumber"]),
        "first_name": results["payload"]["data"]["firstName"],
        "last_name": results["payload"]["data"]["lastName"],
        "email": results["payload"]["data"]["emailAddress"],
    }

    return formatted_results


async def get_student_data_asynchronously(anthology_api_key: str, anthology_base_url: str, students: list):
    student_data = []

    amount_per_chunk = 10
    number_of_chunks = (len(students) // amount_per_chunk) + 1

    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_student_data(anthology_api_key, anthology_base_url, student, client)
                for student in students[amount_per_chunk * i : amount_per_chunk * (i + 1)]
            ]

            results = await asyncio.gather(*tasks)
            student_data.extend(results)

    return student_data

import httpx
import json
from typing import Union
import asyncio
import logging


async def get_student_number_api_modified(
    anthology_api_key: str, anthology_base_url: str, studentId: int
) -> Union[None, int]:
    url = f"{anthology_base_url}/api/commands/Common/Student/get"

    body = {"payload": {"id": f"{studentId}"}}
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.AsyncHTTPTransport(retries=3)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.post(
            url=url, data=json.dumps(body), headers=headers, timeout=30.0
        )
        logging.info(f"Status code {response.status_code}. {response.text}")

        response.raise_for_status()
        results = response.json()

        studentNumber_str = (
            results.get("payload", {}).get("data", {}).get("studentNumber")
        )
        studentNumber = int(studentNumber_str) if studentNumber_str else None

    return studentNumber


async def get_student_info(
    anthology_api_key: str,
    anthology_base_url: str,
    student: dict,
    student_id_to_num_dict: dict,
) -> dict:
    if student["studentId"] in student_id_to_num_dict:
        student_info = {
            **student,
            "studentNumber": student_id_to_num_dict[student["studentId"]],
            "is_from_db": True,
        }
    else:
        student_info = {
            **student,
            "studentNumber": await get_student_number_api_modified(
                anthology_api_key, anthology_base_url, student["studentId"]
            ),
            "is_from_db": False,
        }

    return student_info


async def main(
    anthology_api_key: str,
    anthology_base_url: str,
    students: dict,
    student_id_to_num_dict: dict,
):
    amount_per_chunk = 10
    num_of_chunks = (len(students) // amount_per_chunk) + 1

    updated_student_info = []
    for i in range(1, num_of_chunks + 1):
        tasks = [
            get_student_info(
                anthology_api_key, anthology_base_url, student, student_id_to_num_dict
            )
            for student in students[amount_per_chunk * (i - 1) : amount_per_chunk * i]
        ]

        result = await asyncio.gather(*tasks)
        updated_student_info.extend(result)

    return updated_student_info

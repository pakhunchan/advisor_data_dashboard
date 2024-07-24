import httpx
import asyncio
import logging


async def get_canvas_course_name_asynchronously(
    canvas_bearer_token: str, canvas_base_url: str, sis_course_id_list: list[str]
) -> dict[str, dict]:
    course_data = []

    chunk_size = 10
    number_of_chunks = len(sis_course_id_list) // 10 + 1

    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_canvas_course_name(canvas_bearer_token, canvas_base_url, sis_course_id, client)
                for sis_course_id in sis_course_id_list[chunk_size * i : chunk_size * (i + 1)]
            ]

            course_info = await asyncio.gather(*tasks)
            course_data.extend(course_info)

    logging.info(f"course_data: {course_data}")

    # course_id_mappings = {course["sis_course_id"]: course["canvas_course_name"] for course in course_data}
    course_id_mappings = {
        course["sis_course_id"]: {
            "canvas_course_name": course["canvas_course_name"],
            "canvas_course_id": course["canvas_course_id"],
        }
        for course in course_data
    }

    return course_id_mappings


async def get_canvas_course_name(
    canvas_bearer_token: str, canvas_base_url: str, sis_course_id: str, client: httpx.AsyncClient
) -> dict:
    max_retries = 3
    base_delay = 2

    url = f"{canvas_base_url}/api/v1/accounts/11/courses/sis_course_id:{sis_course_id}"
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url=url, headers=headers, timeout=15.0)
            response.raise_for_status()
            results = response.json()
            break
        except Exception as err:
            if response.status_code == 404:
                return {
                    "sis_course_id": sis_course_id,
                    "canvas_course_name": "",
                    "canvas_course_id": None,
                }

            logging.exception(err)
            if attempt >= max_retries:
                response.raise_for_status()
            await asyncio.sleep(base_delay * 2**attempt)

    course_info = {
        "sis_course_id": sis_course_id,
        "canvas_course_name": results.get("name", ""),
        "canvas_course_id": results.get("id", None),
    }

    return course_info

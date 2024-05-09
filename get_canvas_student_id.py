import pymssql
import httpx
import asyncio
import logging
import json


def get_canvas_student_ids_from_database(anthology_student_numbers: tuple, database_connector: dict) -> dict:
    with pymssql.connect(**database_connector) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(
                f"SELECT * FROM student_id_mapping WHERE anthology_student_number IN %s",
                (anthology_student_numbers,),
            )

            results = cursor.fetchall()

    canvas_student_ids_from_database = {
        student["anthology_student_number"]: student["canvas_student_id"] for student in results
    }
    logging.info(f"canvas_student_ids_from_database: {canvas_student_ids_from_database}")

    return canvas_student_ids_from_database


async def get_canvas_student_ids_asynchronously(
    canvas_bearer_token: str, canvas_base_url: str, anthology_student_numbers: list
) -> dict:
    student_ids_info = []

    amount_per_chunk = 12
    number_of_chunks = (len(anthology_student_numbers) // amount_per_chunk) + 1

    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_canvas_student_id(canvas_bearer_token, canvas_base_url, anthology_student_number, client)
                for anthology_student_number in anthology_student_numbers[
                    amount_per_chunk * i : amount_per_chunk * (i + 1)
                ]
            ]

            results = await asyncio.gather(*tasks)
            student_ids_info.extend(results)

    # convert the student_ids_info into a dictionary
    student_ids_dict = {
        student["anthology_student_number"]: student["canvas_student_id"] for student in student_ids_info
    }

    return student_ids_dict


async def get_canvas_student_id(
    canvas_bearer_token: str, canvas_base_url: str, anthology_student_number: int, client: httpx.AsyncClient
) -> dict:

    # url = f"{canvas_base_url}/api/v1/accounts/1/users/"
    url = f"{canvas_base_url}/api/v1/users/sis_user_id:{anthology_student_number}"
    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}
    # params = {"search_term": anthology_student_number}

    max_retries = 3
    base_delay = 2

    # async with httpx.AsyncClient() as client:
    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url=url, headers=headers, timeout=15.0)
            results = response.json()
            break

        except Exception as err:
            logging.exception(err)
            if response.status_code == 404:
                break
            if attempt >= max_retries:
                logging.exception(
                    f"Failed attempt #{attempt}. Status code {response.status_code} for Canvas API /accounts/1/users/."
                )
                response.raise_for_status()
            await asyncio.sleep(base_delay * 2**attempt)

    canvas_student_id = results["id"] if not response.status_code == 404 else None

    # # retrieve the canvas_student_id of the user matching whose sis_user_id field matches the anthology_student_number value
    # canvas_student_id = results["id"]
    # for user in results:
    #     # in test, sometimes sis_user_id is something strange like "USE276" instead of the typical str(int) like "1023"
    #     if not user["sis_user_id"].isdigit():
    #         continue
    #     if int(user["sis_user_id"]) != anthology_student_number:
    #         continue
    #     else:
    #         canvas_student_id = user["id"]

    # update the student info to include canvas_student_id field
    student_id_info = {
        "anthology_student_number": anthology_student_number,
        "canvas_student_id": canvas_student_id,
    }

    return student_id_info


def insert_student_ids_into_database(student_ids_from_api: dict, database_connector: dict):
    ids_to_insert_into_database = [
        {
            "anthology_student_number": anthology_student_number,
            "canvas_student_id": canvas_student_id,
        }
        for anthology_student_number, canvas_student_id in student_ids_from_api.items()
        if canvas_student_id
    ]

    sql_statement_merge_insert_student_id_mapping = """
        MERGE INTO student_id_mapping AS target 
        USING (VALUES (%(anthology_student_number)d, %(canvas_student_id)d)) AS SOURCE (anthology_student_number, canvas_student_id)
        ON 
            target.anthology_student_number = SOURCE.anthology_student_number 
        WHEN NOT MATCHED THEN 
            INSERT (anthology_student_number, canvas_student_id)
            VALUES (SOURCE.anthology_student_number, SOURCE.canvas_student_id);
    """

    with pymssql.connect(**database_connector) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.executemany(
                sql_statement_merge_insert_student_id_mapping,
                ids_to_insert_into_database,
            )
            conn.commit()

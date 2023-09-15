from typing import Union
import httpx
import logging
import json
import pymssql


def get_student_number_sql_server(database_connector: dict, studentId: int) -> Union[int, None]:
    with pymssql.connect(**database_connector) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute("SELECT studentNumber FROM student_info_dimensions WHERE studentId = %d", studentId)
            results = cursor.fetchone()
            logging.info(f"results from sql server: {json.dumps(results, default=str)}")
            studentNumber = int(results["studentNumber"]) if results else None

    logging.info(f"studentNumber: {studentNumber}")

    return studentNumber


def get_student_number_api(anthology_api_key: str, anthology_base_url: str, studentId: int) -> Union[int, None]:
    url = f"{anthology_base_url}/api/commands/Common/Student/get"
    # body["payload"]["id"] = studentId
    body = {"payload": {"id": f"{studentId}"}}
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.post(url=url, data=json.dumps(body), headers=headers, timeout=30.0)
        logging.info(f"Status code {response.status_code}. {response.text}")

    response.raise_for_status()
    results = response.json()

    studentNumber_str = results.get("payload", {}).get("data", {}).get("studentNumber")
    studentNumber = int(studentNumber_str) if studentNumber_str else None

    return studentNumber


def insert_student_number_sql(database_connector: dict, studentId: int, studentNumber: int) -> None:
    import time

    max_retries = 3
    delay = 2

    for count in range(max_retries + 1):
        try:
            with pymssql.connect(**database_connector) as conn:
                with conn.cursor(as_dict=True) as cursor:
                    cursor.executemany(
                        """INSERT INTO student_info_dimensions (studentId, studentNumber)
                        VALUES (%d, %d)""",
                        [(studentId, studentNumber)],
                    )
                    conn.commit()
            break
        except:
            time.sleep(delay**count)
            logging.info(f"Retry #{count} failed to insert student number into the SQL db.")
            if count >= max_retries:
                raise

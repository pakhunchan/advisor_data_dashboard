import httpx
import time
import logging


def get_anthology_attendance_data(
    anthology_api_key: str, anthology_base_url: str, four_months_prior_date: str
) -> list[dict]:

    max_retries = 3
    base_delay = 2

    url = f"{anthology_base_url}/ds/campusnexus/Attendance"
    headers = {"ApiKey": anthology_api_key}
    params = {
        "$filter": f"AttendanceDate ge {four_months_prior_date}",
        "$select": "AttendanceDate, Attended, Absent, IsExcusedAbsence, StudentCourseId",
    }

    with httpx.Client() as client:
        for attempt in range(max_retries):
            try:
                response = client.get(url=url, headers=headers, params=params, timeout=120.0)
                response.raise_for_status()
                results = response.json()
                break
            except Exception as err:
                logging.exception(err)
                if attempt >= max_retries:
                    response.raise_for_status()
                time.sleep(base_delay * 2**attempt)

    list_of_attendance_data = results["value"]
    logging.info(f"list_of_attendance_data: {list_of_attendance_data}")

    return list_of_attendance_data

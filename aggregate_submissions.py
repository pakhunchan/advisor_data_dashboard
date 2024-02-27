import logging
import json


def create_flattened_canvas_dataset(submission_data: list[list[dict]]) -> list[dict]:
    # list of list of dicts
    raw_canvas_data = [json.loads(data) for data in submission_data]
    logging.info(f"raw_canvas_data: {json.dumps(raw_canvas_data)}")

    # list of dicts
    flattened_canvas_data = []
    for list_of_data in raw_canvas_data:
        flattened_canvas_data.extend(list_of_data)

    logging.info(f"flattened_canvas_data: {json.dumps(flattened_canvas_data)}")

    return flattened_canvas_data


def convert_canvas_course_ids_to_anthology_course_ids(
    combined_submission_data_with_canvas_ids: list[dict],
    list_of_canvas_and_anthology_course_ids: list[dict],
) -> list[dict]:
    dict_of_canvas_and_anthology_course_ids = {
        course["canvas_course_id"]: course["anthology_course_id"]
        for course in list_of_canvas_and_anthology_course_ids
    }

    combined_submission_data_with_anthology_ids = [
        {
            "studentNumber": course["studentNumber"],
            "studentEnrollmentPeriodId": course["studentEnrollmentPeriodId"],
            "earliest": course["earliest"],
            "latest": course["latest"],
            "anthology_course_id": dict_of_canvas_and_anthology_course_ids[
                course["canvas_course_id"]
            ],
        }
        for course in combined_submission_data_with_canvas_ids
    ]

    return combined_submission_data_with_anthology_ids


def calculate_participation_for_each_student(
    combined_submission_data: list[dict],
    student_payload: list[dict],
    anthology_base_url: str,
) -> list[dict]:
    # creating a dictionary from the data
    canvas_student_participation_dict = {}
    for submission in combined_submission_data:
        logging.info(
            f"submission in combined_submission_data: {json.dumps(submission, default=str)}"
        )
        canvas_student_participation_dict = update_submissions_dictionary(
            canvas_student_participation_dict, submission
        )

    logging.info(
        f"canvas_student_participation_dict: {json.dumps(canvas_student_participation_dict)}"
    )

    student_id_dict = {
        str(student["studentNumber"]): str(student["studentId"])
        for student in student_payload
    }
    logging.info(f"student_id_dict: {json.dumps(student_id_dict)}")

    canvas_student_participation_list = [
        {
            "studentNumber": k,
            "studentId": student_id_dict[k],
            "Anthology link": f"{anthology_base_url}/#/students/{student_id_dict[k]}",
            **v,
        }
        for k, v in canvas_student_participation_dict.items()
    ]

    logging.info(
        f"canvas_student_participation_list: {json.dumps(canvas_student_participation_list, default=str)}"
    )

    return canvas_student_participation_list


# function that takes in an assignment submission and updates canvas_student_participation as necessary
def update_submissions_dictionary(
    canvas_student_participation: dict, item: dict
) -> dict:
    studentNumber = item["studentNumber"]
    studentEnrollmentPeriodId = item["studentEnrollmentPeriodId"]
    earliest = item["earliest"]
    latest = item["latest"]
    anthology_course_id = item["anthology_course_id"]

    if studentNumber not in canvas_student_participation:
        canvas_student_participation[studentNumber] = {
            "studentEnrollmentPeriodId": studentEnrollmentPeriodId,
            "earliest": earliest,
            "latest": latest,
            "anthology_course_ids": [anthology_course_id],
        }
    else:
        if earliest < canvas_student_participation[studentNumber]["earliest"]:
            canvas_student_participation[studentNumber]["earliest"] = earliest
        if latest > canvas_student_participation[studentNumber]["latest"]:
            canvas_student_participation[studentNumber]["latest"] = latest
        canvas_student_participation[studentNumber]["anthology_course_ids"].append(
            anthology_course_id
        )

    return canvas_student_participation


def convert_from_utc_to_eastern(
    canvas_student_participation_utc: list[dict],
) -> list[dict]:
    # modify timestamps from UTC (Canvas' default for their APIs) to Eastern
    import datetime
    import pytz

    tz = pytz.timezone("US/Eastern")

    for student in canvas_student_participation_utc:
        if student["earliest"]:
            student["earliest"] = (
                datetime.datetime.strptime(student["earliest"], "%Y-%m-%dT%H:%M:%SZ")
                .astimezone(pytz.utc)
                .astimezone(tz)
                .strftime("%Y-%m-%dT%H:%M:%S")
            )
        if student["latest"]:
            student["latest"] = (
                datetime.datetime.strptime(student["latest"], "%Y-%m-%dT%H:%M:%SZ")
                .astimezone(pytz.utc)
                .astimezone(tz)
                .strftime("%Y-%m-%dT%H:%M:%S")
            )

    return canvas_student_participation_utc


def separate_students_with_multiple_enrollment(
    canvas_student_participation_eastern: list[dict],
) -> tuple[list[dict], list[dict]]:
    # separate out the students with multiple active enrollments
    students_with_multiple_enrollment = [
        canvas_student_participation_eastern.pop(i)
        for i, student in enumerate(canvas_student_participation_eastern)
        if isinstance(student["studentEnrollmentPeriodId"], list)
    ]

    return canvas_student_participation_eastern, students_with_multiple_enrollment

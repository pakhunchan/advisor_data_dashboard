import httpx
import json
import logging


def get_school_status_ids(anthology_base_url: str, anthology_api_key: str, school_status_codes: set) -> list:
    url = f"{anthology_base_url}/ds/campusnexus/SchoolStatuses"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=2)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=30.0)
    response.raise_for_status()

    results = response.json()
    school_statuses = results["value"]
    school_status_ids = [status["Id"] for status in school_statuses if status["Code"] in school_status_codes]

    logging.info(f"school_status_ids: {json.dumps(school_status_ids, default=str)}")

    return school_status_ids


def get_students(filtered_school_status_ids: list, anthology_base_url: str, anthology_api_key: str) -> list:
    students = []
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=4)
    with httpx.Client(transport=transport) as client:
        for school_status_id in filtered_school_status_ids:
            url = f"{anthology_base_url}/ds/campusnexus/StudentEnrollmentPeriods?$filter=SchoolStatusId eq {school_status_id}"
            response = client.get(url=url, headers=headers, timeout=30.0)

            results = response.json()
            students.extend(results["value"])

    return students


def get_student_ids_for_single_vs_multiple_enrollments(students: list[dict], check_student_enrollment_ids: set) -> dict:
    students_set = set()
    student_ids_and_enrollment_ids_dict = {}

    for student in students:
        try:
            # ie, process all students if check_student_enrollment_ids is empty
            # or process only students with an enrollment id in check_student_enrollment_ids
            if not check_student_enrollment_ids or student["Id"] in check_student_enrollment_ids:
                studentId = str(student["StudentId"])
                studentEnrollmentPeriodId = str(student["Id"])

                if studentId not in students_set:
                    student_ids_and_enrollment_ids_dict[studentId] = studentEnrollmentPeriodId
                    students_set.add(studentId)
                else:
                    if studentId in student_ids_and_enrollment_ids_dict and isinstance(
                        student_ids_and_enrollment_ids_dict[studentId], str
                    ):
                        student_ids_and_enrollment_ids_dict[studentId] = [
                            student_ids_and_enrollment_ids_dict[studentId],
                            studentEnrollmentPeriodId,
                        ]
                    else:
                        student_ids_and_enrollment_ids_dict[studentId].append(studentEnrollmentPeriodId)

        except Exception as err:
            logging.error(f"Failed student dict was: {json.dumps(student, default=str)}")
            logging.exception(err)
            raise

    print(json.dumps({"student_ids_and_enrollment_ids_dict": student_ids_and_enrollment_ids_dict}, default=str))

    return student_ids_and_enrollment_ids_dict

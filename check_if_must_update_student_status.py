from typing import Union
from datetime import datetime
import httpx
import json
import logging


def check_if_must_update_student_status(
    result: dict,
    earliest: str,
    end_date: str,
    exclude_anthology_course_codes: list,
    anthology_api_key: str,
    anthology_base_url: str,
    studentId: int,
    studentEnrollmentPeriodId: int,
    promotable_enrollment_status_ids: set,
    earliest_participation_date: str,
    error_flags: set,
):
    if result["payload"]["data"]["schoolStatusId"] in promotable_enrollment_status_ids:
        FDP = get_FDP(result)
        list_of_course_ids = get_list_of_course_ids(
            anthology_api_key, anthology_base_url, studentId, studentEnrollmentPeriodId, exclude_anthology_course_codes
        )
        logging.info(f"list_of_course_ids: {json.dumps(list_of_course_ids)}")
        if not list_of_course_ids:
            error_flags.add("error_flag_participation_but_no_registered_courses")
            return False, None, error_flags, []

        EAD, courses_with_missing_attendance_data = get_enrollment_activation_date(
            anthology_api_key, anthology_base_url, list_of_course_ids, end_date, earliest_participation_date
        )
        if not EAD:
            return True, None, error_flags, courses_with_missing_attendance_data
        logging.info(f"EAD: {EAD}")
        logging.info(f"FDP: {FDP}")
        logging.info(f"earliest: {earliest}")

        # if a promotable student has participation earlier than EAD, then we should promote
        if (FDP and FDP <= EAD) or earliest <= EAD:
            must_update_student_status = True
        # otherwise, don't promote. also, EAD < earliest_participation means we have missing attendance data
        else:
            must_update_student_status = False
            error_flags.add("error_flag_EAD_gt_earliest_participation")

        return must_update_student_status, EAD, error_flags, courses_with_missing_attendance_data

    # if student's enrollment status id is not 5,8,9,69, then no need to update
    return False, None, error_flags, []


def get_FDP(result: dict) -> Union[str, None]:
    for extended_property in result["payload"]["data"]["extendedProperties"]:
        if extended_property["name"] == "First Date of Student Participation":
            return extended_property["value"]
    return None


def get_list_of_course_ids(
    anthology_api_key: str,
    anthology_base_url: str,
    studentId: int,
    studentEnrollmentPeriodId: int,
    exclude_anthology_course_codes: list,
) -> list:
    url = f"{anthology_base_url}/ds/campusnexus/StudentCourses?$filter=StudentEnrollmentPeriodId eq {studentEnrollmentPeriodId} and StudentId eq {studentId}"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers)
        logging.info(
            f"Status code for API /ds/campusnexus/StudentCourses was {response.status_code}. response.text: {response.text}"
        )
    response.raise_for_status()

    results = response.json()
    full_course_list = results.get("value", [])

    exclude_anthology_class_section_ids = get_exclude_anthology_class_section_ids(
        exclude_anthology_course_codes, anthology_api_key, anthology_base_url
    )

    relevant_course_list_ids = [
        {"studentCourseId": course["Id"], "classSectionId": course["ClassSectionId"], "drop_date": course["DropDate"]}
        for course in full_course_list
        if (course["Status"] in {"C", "S", "D"})
        and (course["ClassSectionId"] not in exclude_anthology_class_section_ids)
    ]

    return relevant_course_list_ids


def get_exclude_anthology_class_section_ids(
    exclude_anthology_course_codes: list, anthology_api_key: str, anthology_base_url: str
) -> set:
    url = f"{anthology_base_url}/ds/campusnexus/ClassSections"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=30.0)
        logging.info(
            f"Status code for /ds/campusnexus/ClassSections was {response.status_code}. response.text: {response.text}"
        )
    response.raise_for_status()
    results = response.json()

    list_of_courses = results["value"]
    set_of_exclude_anthology_course_codes = set(exclude_anthology_course_codes)
    logging.info(
        f"Excluding the following Anthology classSectionIds: {json.dumps(list(set_of_exclude_anthology_course_codes))}"
    )

    return {course["Id"] for course in list_of_courses if course["CourseCode"] in set_of_exclude_anthology_course_codes}


def get_enrollment_activation_date(
    anthology_api_key: str,
    anthology_base_url: str,
    list_of_course_ids: list[dict],
    end_date: str,
    earliest_participation_date: str,
) -> tuple[Union[str, None], list]:
    all_course_meeting_dates = []
    courses_with_missing_attendance_data = []

    for course_ids in list_of_course_ids:
        logging.info(f"course_ids: {json.dumps(course_ids, default=str)}")

        attendance_dates, has_missing_attendance_data = get_attendance_dates(
            anthology_api_key, anthology_base_url, course_ids, end_date, earliest_participation_date
        )

        all_course_meeting_dates.extend(attendance_dates)

        courses_with_missing_attendance_data.append(
            course_ids["classSectionId"]
        ) if has_missing_attendance_data else None

    logging.info(f"all_course_meeting_dates: {all_course_meeting_dates}")
    logging.info(f"courses_with_missing_attendance_data: {courses_with_missing_attendance_data}")

    if not all_course_meeting_dates:
        return "", courses_with_missing_attendance_data

    # outside func -> if not EAD, then must_update_statuses = true
    ##############

    EAD = min(all_course_meeting_dates)
    modified_EAD = parse_datetime(EAD).strftime("%Y-%m-%dT23:59:59")
    logging.info(f"modified_EAD: {modified_EAD}")

    return modified_EAD, courses_with_missing_attendance_data


# pulls all relevant class meeting dates (excludes absences and cancelled classes)
def get_attendance_dates(
    anthology_api_key: str, anthology_base_url: str, course_ids: dict, end_date: str, earliest_participation_date: str
) -> tuple[list, Union[bool, None]]:
    url = f"{anthology_base_url}/ds/campusnexus/Attendance/CampusNexus.GetStudentAttendanceClassDetailList(studentCourseId={course_ids['studentCourseId']},classSectionId={course_ids['classSectionId']},startDate='1900-01-01',endDate='{end_date}')"
    headers = {"ApiKey": anthology_api_key}

    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(transport=transport) as client:
        response = client.get(url=url, headers=headers, timeout=30.0)
        logging.info(
            f"Status code of /ds/campusnexus/Attendance/CampusNexus.GetStudentAttendanceClassDetailList was {response.status_code}. response.text: {response.text}"
        )

    response.raise_for_status()
    results = response.json()
    course_meetings = results.get("value", [])

    # for dropped classes, only consider attendance dates that are before the drop date
    if course_ids["drop_date"]:
        course_meeting_dates = [
            meeting["AttendanceDate"]
            for meeting in course_meetings
            if (meeting["Attended"] == None or meeting["Attended"] > 0)
            and meeting["Status"] != "C"
            and meeting["AttendanceDate"][:10] < course_ids["drop_date"][:10]
        ]

    # otherwise, just make sure both (Attended != 0) and also (the class meeting wasn't canceled that day)
    else:
        course_meeting_dates = [
            meeting["AttendanceDate"]
            for meeting in course_meetings
            if (meeting["Attended"] == None or meeting["Attended"] > 0) and meeting["Status"] != "C"
        ]

    # check if the course has any missing attendance data with a date earlier than the earliest participation date
    has_missing_attendance_data = False
    for meeting in course_meetings:
        if meeting["AttendanceDate"][:10] < earliest_participation_date[:10] and meeting["Attended"] == None:
            has_missing_attendance_data = True

    return course_meeting_dates, has_missing_attendance_data


def parse_datetime(datetime_str):
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y/%m/%d %I:%M:%S %p",
        "%m/%d/%y %I:%M:%S %p",
        "%d %B %Y %I:%M:%S %p",
        "%d %b %Y %I:%M:%S %p",
        "%B %d, %Y %I:%M:%S %p",
        "%b %d, %Y %I:%M:%S %p",
        "%Y/%m/%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            pass

    raise ValueError(f"Invalid datetime string: {datetime_str}")

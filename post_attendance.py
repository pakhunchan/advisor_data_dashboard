import httpx
import logging
import json
import azure.functions as func


def get_student_course_id(
    anthology_api_key: str,
    anthology_base_url: str,
    anthology_course_ids: set,
    studentId: str,
    studentEnrollmentPeriodId: str,
) -> list:
    with httpx.Client() as client:
        url = f"{anthology_base_url}/ds/campusnexus/StudentCourses?$filter=StudentEnrollmentPeriodId eq {studentEnrollmentPeriodId} and StudentId eq {studentId}"
        headers = {"ApiKey": anthology_api_key}

        response = client.get(url=url, headers=headers)
        logging.info(f"Status code was {response.status_code}. response.text: {json.dumps(response.text, default=str)}")
        results = response.json()

    student_info_with_course_ids = [
        {
            "StudentCourseId": course["Id"],
            "ClassSectionId": course["ClassSectionId"],
            "StartDate": course["StartDate"],
            "EndDate": course["ExpectedEndDate"],
            "StudentId": studentId,
            "StudentEnrollmentPeriodId": studentEnrollmentPeriodId,
        }
        for course in results["value"]
        if course["ClassSectionId"] in anthology_course_ids
    ]

    return student_info_with_course_ids


def get_meeting_date_id(anthology_api_key: str, anthology_base_url: str, ClassSectionId: int) -> dict:
    # {{anthology}}/ds/campusnexus/ClassSectionMeetingDates?$filter=ClassSectionId eq 396&$orderby=MeetingDate asc
    url = f"{anthology_base_url}/ds/campusnexus/ClassSectionMeetingDates?$filter=ClassSectionId eq {ClassSectionId}&$orderby=MeetingDate asc"
    headers = {"ApiKey": anthology_api_key}

    try:
        with httpx.Client() as client:
            response = client.get(url=url, headers=headers)
            logging.info(f"Status code was {response.status_code}. response.text: {response.text}")
            results = response.json()

            meeting_info = {
                "ClassSectionMeetingDateId": results["value"][0]["Id"],
                "MeetingDate": results["value"][0]["MeetingDate"],
            }
            logging.info(f"meeting_info: {json.dumps(meeting_info, default=str)}")
    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(f"{repr(err)}", status_code=400)

    return meeting_info


# post attendance to anthology
def post_attendance_to_anthology(
    anthology_api_key: str,
    anthology_base_url: str,
    ClassSectionId: int,
    StudentId: int,
    StartDate: str,
    EndDate: str,
    ClassSectionMeetingDateId: int,
    AttendanceDate: str,
    StudentCourseId: int,
    StudentEnrollmentPeriodId: int,
) -> int:
    import httpx

    url = f"{anthology_base_url}/api/commands/Academics/Attendance/postAttendance"
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    body = {
        "payload": {
            "ClassSectionId": ClassSectionId,
            "StudentId": StudentId,
            "StartDate": StartDate,
            "EndDate": EndDate,
            "AllowClosedTerm": False,
            "IsPostExternshipOnline": False,
            "Entity": {
                "Id": 27986,
                "ClassSectionMeetingDateId": ClassSectionMeetingDateId,
                "AttendanceDate": AttendanceDate,
                "AttendanceDateStartTime": "1899-12-30T09:00:00",
                "MinutesAttended": None,
                "Attended": 60,
                "Absent": 0,
                "Status": "A",
                "AttendedStatus": "A",
                "Type": "A",
                "UnitType": "M",
                "IsTransferSection": False,
                "IsDependentCourse": False,
                "IsExcusedAbsence": False,
                "Note": "Activated by participation.",
                "delContext": False,
                "StudentCourseId": StudentCourseId,
                "StudentEnrollmentPeriodId": StudentEnrollmentPeriodId,
                "EntityState": 0,
            },
        }
    }

    logging.info(f"Body of API PostAttendance: {body}")

    with httpx.Client() as client:
        response = client.post(url=url, headers=headers, data=json.dumps(body))
        logging.info(f"Status code was {response.status_code}. response.text: {json.dumps(response.text, default=str)}")

    return response.status_code

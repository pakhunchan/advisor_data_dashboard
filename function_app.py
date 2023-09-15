import azure.functions as func
import logging
import json
import traceback
from get_and_submit_anthology import (
    get_anthology,
    get_earliest_participation_date,
    update_request_body,
    post_anthology,
    generate_function_response,
)
from check_if_must_update_student_status import check_if_must_update_student_status
from update_student_status import update_student_status
from create_student_status_history_record import create_student_status_history_record

from get_anthology_and_canvas_term_ids import get_anthology_term_info, get_canvas_term_id
from get_students import get_students, get_school_status_ids
from get_student_number import get_student_number_sql_server, get_student_number_api, insert_student_number_sql
from get_courses import get_canvas_courses, get_zero_credit_anthology_courses
from get_submissions import get_submissions, add_submission, make_student_id_dict, should_not_skip
from aggregate_submissions import (
    update_submissions_dictionary,
    create_flattened_canvas_dataset,
    convert_canvas_course_ids_to_anthology_course_ids,
    calculate_participation_for_each_student,
    convert_from_utc_to_eastern,
    separate_students_with_multiple_enrollment,
)
from update_course_status import update_course_statuses

# from post_attendance import get_student_course_id, get_meeting_date_id, post_attendance_to_anthology

app = func.FunctionApp()


################################################
# Scope1 Part1 -- GetAnthologyAndCanvasTermIds
################################################


@app.function_name(name="GetAnthologyAndCanvasTermIds")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_anthology_and_canvas_term_ids(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        canvas_bearer_token = request.pop("canvas_bearer_token")
        anthology_base_url = request["anthology_base_url"]
        canvas_base_url = request["canvas_base_url"]
        curr_date = request["curr_date"]
        exclude_anthology_term_ids = request["exclude_anthology_term_ids"]

        anthology_term_id, anthology_term_code = get_anthology_term_info(
            anthology_api_key, anthology_base_url, curr_date, exclude_anthology_term_ids
        )
        canvas_term_id = get_canvas_term_id(canvas_bearer_token, canvas_base_url, anthology_term_code)

        return func.HttpResponse(
            json.dumps({"anthology_term_id": anthology_term_id, "canvas_term_id": canvas_term_id}, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


######################################
# Scope1 -Part2 - Get List of Students
######################################


@app.function_name(name="GetListOfStudents")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_list_of_students(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # retrieve payload and initialize variables
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        logging.info(json.dumps({"Request payload": request}, default=str))

        anthology_base_url = request["anthology_base_url"]
        ################################
        term_id = request["term_id"]
        school_status_codes = set(request["school_status_codes"])
        check_student_enrollment_ids = set(request.get("check_student_enrollment_ids") or {})

        # get school_status_ids of the active groups of students
        school_status_ids = get_school_status_ids(anthology_base_url, anthology_api_key, school_status_codes)

        # get list of active students by filtering by school_status_ids
        students = get_students(school_status_ids, anthology_base_url, anthology_api_key)

        # format the final results
        studentid_and_enrollmentperiodid = [
            {"studentEnrollmentPeriodId": student["Id"], "studentId": student["StudentId"], "termId": term_id}
            for student in students
            if not check_student_enrollment_ids or student["Id"] in check_student_enrollment_ids
        ]
        logging.info(f"studentid_and_enrollmentperiodid: {json.dumps(studentid_and_enrollmentperiodid, default=str)}")

        return func.HttpResponse(json.dumps(studentid_and_enrollmentperiodid, default=str), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


#####################################
# Scope1 -Part3 - Get Student Number
#####################################

import httpx
import json
from typing import Union
import asyncio
import pymssql


async def get_student_number_api_modified(
    anthology_api_key: str, anthology_base_url: str, studentId: int
) -> Union[None, int]:
    url = f"{anthology_base_url}/api/commands/Common/Student/get"

    body = {"payload": {"id": f"{studentId}"}}
    headers = {"ApiKey": anthology_api_key, "Content-Type": "application/json"}

    transport = httpx.AsyncHTTPTransport(retries=3)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.post(url=url, data=json.dumps(body), headers=headers, timeout=30.0)
        logging.info(f"Status code {response.status_code}. {response.text}")

        response.raise_for_status()
        results = response.json()

        studentNumber_str = results.get("payload", {}).get("data", {}).get("studentNumber")
        studentNumber = int(studentNumber_str) if studentNumber_str else None

    return studentNumber


async def get_student_info(
    anthology_api_key: str, anthology_base_url: str, student: dict, student_id_to_num_dict: dict
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


async def main(anthology_api_key: str, anthology_base_url: str, students: dict, student_id_to_num_dict: dict):
    amount_per_chunk = 10
    num_of_chunks = (len(students) // amount_per_chunk) + 1

    updated_student_info = []
    for i in range(1, num_of_chunks + 1):
        tasks = [
            get_student_info(anthology_api_key, anthology_base_url, student, student_id_to_num_dict)
            for student in students[amount_per_chunk * (i - 1) : amount_per_chunk * i]
        ]

        result = await asyncio.gather(*tasks)
        updated_student_info.extend(result)

    return updated_student_info


@app.function_name(name="GetStudentNumberInBulk")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_student_number_in_bulk(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        database_connector = request["database_connector"]
        students = request["students"]
        student_ids = tuple(student["studentId"] for student in students)

        anthology_api_key = request.pop("anthology_api_key")
        anthology_base_url = request["anthology_base_url"]

        # get student numbers from database in bulk
        with pymssql.connect(**database_connector) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(
                    f"SELECT studentId, studentNumber from student_info_dimensions WHERE studentId IN {student_ids}"
                )
                student_id_to_num_dict = {row["studentId"]: row["studentNumber"] for row in cursor}

        # start creating final results with the available data
        updated_student_info = asyncio.run(
            main(anthology_api_key, anthology_base_url, students, student_id_to_num_dict)
        )

        # remove the "is_from_db" flag and collect list of studentIds that need to be
        student_numbers_not_in_database = list(
            {
                (student["studentId"], student["studentNumber"])
                for student in updated_student_info
                if not student.pop("is_from_db")
            }
        )

        with pymssql.connect(**database_connector) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.executemany(
                    "INSERT INTO student_info_dimensions VALUES (%d, %d)", student_numbers_not_in_database
                )
                rows_affected = cursor.rowcount
                conn.commit()

        return func.HttpResponse(
            json.dumps(
                {
                    "rows_affected": rows_affected,
                    "students": updated_student_info,
                },
                default=str,
            ),
            status_code=200,
        )
    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


##################################
# Scope2 -- Get List of Courses
##################################


@app.function_name(name="GetListOfCourses")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_list_of_courses(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # retrieving payload + initializing variables
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        canvas_bearer_token = request.pop("canvas_bearer_token")
        logging.info(f"request: {json.dumps(request)}")
        anthology_base_url = request["anthology_base_url"]
        canvas_base_url = request["canvas_base_url"]
        term_id = int(request["term_id"])

        zero_credit_anthology_course_ids = get_zero_credit_anthology_courses(anthology_api_key, anthology_base_url)

        list_of_courses = get_canvas_courses(canvas_bearer_token, canvas_base_url, term_id)
        logging.info(f"list_of_courses: {json.dumps(list_of_courses)}")

        canvas_course_ids = [
            course["id"]
            for course in list_of_courses
            if course["sis_course_id"] not in zero_credit_anthology_course_ids
        ]
        logging.info(f"canvas_course_id: {json.dumps(canvas_course_ids, default=str)}")

        list_of_canvas_and_anthology_course_ids = [
            {
                "canvas_course_id": course["id"],
                "anthology_course_id": int(course["sis_course_id"].lstrip("AdClassSched_")),
            }
            for course in list_of_courses
        ]
        logging.info(
            f"list_of_canvas_and_anthology_course_ids: {json.dumps(list_of_canvas_and_anthology_course_ids, default=str)}"
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "canvas_course_ids": canvas_course_ids,
                    "list_of_canvas_and_anthology_course_ids": list_of_canvas_and_anthology_course_ids,
                },
                default=str,
            ),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


#########################################
# Scope3 Part1 -- Get Canvas Submissions
#########################################


@app.function_name(name="GetSubmissions")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_submissions_function(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # retrieve incoming request payload
        request = req.get_json()
        canvas_bearer_token = request.pop("canvas_bearer_token")
        student_payload = request.pop("student_payload")
        logging.info(f"request without canvas_bearer_token and student_payload shown: {request}")

        # initialize variables
        canvas_base_url = request["canvas_base_url"]
        course_id = request["course_id"]
        submitted_since = request["lastSuccessTimestamp"]

        # # Only in TEST. return empty list if the course_id is one of the fake course_ids 55, 62, 63 or else the API requests will error out
        # fake_course_ids = [8, 36, 55, 58, 62, 63, 325, 331, 333, 334]
        # if course_id in fake_course_ids:
        #     return func.HttpResponse(json.dumps([], default=str), status_code=200)

        submission_results = get_submissions(canvas_base_url, canvas_bearer_token, course_id, submitted_since)
        logging.info(f"submission_results: {json.dumps(submission_results, default=str)}")

        # Canvas' API response includes studentNumber but not studentEnrollmentPeriodId
        # Anthology's API request body requires studentEnrollmentPeriodId

        # # use student_payload to create a dictionary of studentNumber:studentEnrollmentPeriodId
        # student_id_dict = {str(student["studentNumber"]): str(student["studentEnrollmentPeriodId"]) for student in student_payload}

        # need a dictionary of studentNumber:studentEnrollmentPeriodId. For k:v pair, set v as a str(int) if 1 value per key, but set v as a list of str(int)s if many values per key

        student_id_dict = make_student_id_dict(student_payload)
        logging.info(f"student_id_dict: {json.dumps(student_id_dict)}")

        # student_id_dict = dict()
        # for student in student_payload:
        #     if student["studentNumber"] in student_id_dict and isinstance(student_id_dict[student["studentNumber"]], int):
        #         student_id_dict[str(student)]
        #     else:
        #         student_id_dict[str(student["studentNumber"])] = str(student["studentEnrollmentPeriodId"])

        # iterate through each submission and update canvas_student_participation
        canvas_student_participation_dict = {}
        for student in submission_results:
            for submission in student["submissions"]:
                # only updating canvas_student_participation when the student is also in the student_payload/student_id_dict
                if should_not_skip(submission, student, student_id_dict):
                    add_submission(
                        canvas_student_participation=canvas_student_participation_dict,
                        studentNumber=student["sis_user_id"],
                        submitted_at=submission["submitted_at"],
                        course_id=course_id,
                    )

        logging.info(f"canvas_student_participation_dict: {json.dumps(canvas_student_participation_dict, default=str)}")

        # convert to a more readable JSON format
        canvas_student_participation = [
            {"studentNumber": k, "studentEnrollmentPeriodId": student_id_dict[k], **v}
            for k, v in canvas_student_participation_dict.items()
        ]

        logging.info(f"canvas_student_participation: {json.dumps(canvas_student_participation, default=str)}")

        return func.HttpResponse(
            json.dumps(canvas_student_participation, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


#############################################
# Scope3 Part2 -- Aggregate Submissions Data
#############################################


@app.function_name(name="AggregateCanvasSubmissions")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def aggregate_canvas_submissions(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        logging.info(f"request: {json.dumps(request)}")

        student_payload = request["student_payload"]
        submission_data = request["submission_data"]
        list_of_canvas_and_anthology_course_ids = request["list_of_canvas_and_anthology_course_ids"]

        # request only has student_payload and submission_data. Logging these separately since they are long and cut off in the logs.
        logging.info(f"student_payload of request: {json.dumps(student_payload, default=str)}")
        logging.info(f"submission data of request: {json.dumps(submission_data, default=str)}")

        combined_submission_data_with_canvas_ids = create_flattened_canvas_dataset(submission_data)
        combined_submission_data_with_anthology_ids = convert_canvas_course_ids_to_anthology_course_ids(
            combined_submission_data_with_canvas_ids, list_of_canvas_and_anthology_course_ids
        )
        canvas_student_participation_utc = calculate_participation_for_each_student(
            combined_submission_data_with_anthology_ids, student_payload
        )
        canvas_student_participation_eastern = convert_from_utc_to_eastern(canvas_student_participation_utc)

        students_with_single_enrollment, students_with_multiple_enrollment = separate_students_with_multiple_enrollment(
            canvas_student_participation_eastern
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "students_with_single_enrollment": students_with_single_enrollment,
                    "students_with_multiple_enrollment": students_with_multiple_enrollment,
                },
                default=str,
            ),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


###########################################
# Scope4 Part1 -- Get and Update Anthology
###########################################


@app.function_name(name="GetAndUpdateAnthology")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_and_update_anthology(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        if not isinstance(request, dict):
            request = json.loads(request)
        anthology_api_key = request.pop("anthology_api_key")
        logging.info(f"request: {json.dumps(request)}")

        anthology_base_url = request["anthology_base_url"]
        studentEnrollmentPeriodId = request["student"]["studentEnrollmentPeriodId"]
        studentId = request["student"]["studentId"]
        earliest = request["student"]["earliest"]
        latest = request["student"]["latest"]
        anthology_course_ids = request["student"]["anthology_course_ids"]
        end_date = request["end_date"]
        exclude_anthology_course_codes = request["exclude_anthology_course_codes"]
        promotable_enrollment_status_ids = set(request["promotable_enrollment_status_ids"])

        result = get_anthology(anthology_base_url, studentEnrollmentPeriodId, anthology_api_key)
        logging.info(
            f'result["payload"]["data"]["schoolStatusId"] == 13: {result["payload"]["data"]["schoolStatusId"] == 13}'
        )
        is_enrolled_student = result["payload"]["data"]["schoolStatusId"] == 13

        earliest_participation_date = get_earliest_participation_date(result, earliest)

        (
            must_update_student_status,
            EAD,
            error_flags,
            courses_with_missing_attendance_data,
        ) = check_if_must_update_student_status(
            result,
            earliest,
            end_date,
            exclude_anthology_course_codes,
            anthology_api_key,
            anthology_base_url,
            studentId,
            studentEnrollmentPeriodId,
            promotable_enrollment_status_ids,
            earliest_participation_date,
            error_flags=set(),
        )
        logging.info(f"must_update_student_status: {must_update_student_status}")
        logging.info(f"error_flags: {error_flags}")

        result, update_FDP, update_LDP = update_request_body(result, earliest, latest, must_update_student_status)

        # return func.HttpResponse("Finished testing. Stopping before we update the Anthology records.", status_code=200)
        course_status_change_logs = None

        if update_FDP or update_LDP:
            update_student_status(
                anthology_api_key, anthology_base_url, studentId
            ) if must_update_student_status else None
            post_anthology(anthology_base_url, result, anthology_api_key)
            create_student_status_history_record(
                anthology_api_key, anthology_base_url, studentId, studentEnrollmentPeriodId, earliest_participation_date
            ) if must_update_student_status else None
            course_status_change_logs = (
                update_course_statuses(
                    anthology_api_key, anthology_base_url, anthology_course_ids, studentEnrollmentPeriodId, studentId
                )
                if must_update_student_status or is_enrolled_student
                else None
            )

        function_response = generate_function_response(
            studentEnrollmentPeriodId,
            update_FDP,
            update_LDP,
            earliest_participation_date,
            EAD,
            must_update_student_status,
            error_flags,
            course_status_change_logs,
            courses_with_missing_attendance_data,
        )
        logging.info(f"function response: {json.dumps(function_response)}")

        return func.HttpResponse(
            json.dumps(function_response, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


###############################################################
# Scope4 Part2a -- Prepare Email for No Registered Course Error
###############################################################


@app.function_name(name="GetDataForNoRegisteredCourseError")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_data_for_no_registered_course_error(req: func.HttpRequest) -> func.HttpResponse:
    import httpx

    try:
        request = req.get_json()
        canvas_bearer_token = request.pop("canvas_bearer_token")
        logging.info(f"request: {json.dumps(request, default=str)}")
        canvas_base_url = request["canvas_base_url"]
        student = request["student"]
        anthology_course_ids = student["anthology_course_ids"]
        canvas_term_id = request["canvas_term_id"][0]

        sis_course_ids = {f"AdClassSched_{course_id}" for course_id in anthology_course_ids}

        url = f"{canvas_base_url}/api/v1/accounts/11/courses"
        page = 1
        headers = {"Authorization": f"Bearer {canvas_bearer_token}"}

        list_of_courses = []

        transport = httpx.HTTPTransport(retries=3)
        with httpx.Client(transport=transport) as client:
            while True:
                params = {"per_page": 100, "page": page, "enrollment_term_id": canvas_term_id}
                response = client.get(url=url, params=params, headers=headers)
                results = response.json()
                list_of_courses.extend(results)

                if "next" not in response.headers.get("Link"):
                    break
                page += 1

        courses_with_participation_data = [
            f"ClassSectionId #{course['sis_course_id'].lstrip('AdClassSched_')} - {course['course_code']}"
            for course in list_of_courses
            if course["sis_course_id"] in sis_course_ids
        ]

        # formatting the final results
        del student["anthology_course_ids"]
        student_and_course_info = {
            **request["student"],
            "courses_with_participation_data": courses_with_participation_data,
        }

        return func.HttpResponse(json.dumps(student_and_course_info, default=str), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


#############################################################
# Scope4 Part2b -- Prepare Email for Missing Attendance Data Error
#############################################################

from get_data_for_missing_attendance_error import get_student_name, get_course_info


@app.function_name(name="GetDataForMissingAttendanceError")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_data_for_missing_attendance_error(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        canvas_bearer_token = request.pop("canvas_bearer_token")
        anthology_api_key = request.pop("anthology_api_key")
        logging.info(f"request: {json.dumps(request, default=str)}")
        canvas_base_url = request["canvas_base_url"]
        anthology_base_url = request["anthology_base_url"]
        student = request["student"]
        anthology_course_ids = student["anthology_course_ids"]
        canvas_term_id = request["canvas_term_id"][0]

        student_name = get_student_name(anthology_api_key, anthology_base_url, student["studentId"])
        courses_with_missing_attendance_data = get_course_info(
            canvas_bearer_token, canvas_base_url, canvas_term_id, anthology_course_ids
        )

        # formatting the final results
        student_and_course_info = {
            "studentName": student_name,
            "studentNumber": student["studentNumber"],
            "studentId": student["studentId"],
            "studentEnrollmentPeriodId": student["studentEnrollmentPeriodId"],
            "Anthology link": student["Anthology link"],
            "Courses with missing attendance data": courses_with_missing_attendance_data,
        }

        # # formatting the final results
        # del student["anthology_course_ids"]
        # student_and_course_info = {
        #     **request["student"],
        #     "courses_with_missing_attendance_data": courses_with_missing_attendance_data,
        #     "student_name": student_name,
        # }

        return func.HttpResponse(json.dumps(student_and_course_info, default=str), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)

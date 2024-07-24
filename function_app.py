import azure.functions as func
import logging
import json
import traceback
import pymssql
import asyncio


from get_anthology_and_canvas_term_ids import (
    get_anthology_term_info,
    get_canvas_term_id,
)
from get_students import (
    get_school_status_ids,
    get_students,
    get_student_ids_for_single_vs_multiple_enrollments,
)

from get_student_number_email_first_last_name import get_student_data_asynchronously
from get_canvas_student_id import (
    get_canvas_student_ids_from_database,
    get_canvas_student_ids_asynchronously,
    insert_student_ids_into_database,
)
from get_aos_residency import get_aos_residency_api_data_asynchronously
from get_prep_program import get_prep_program_dict
from get_academic_graduation_hold_registration_hold import get_graduation_hold_registration_hold_asynchronously
from get_academic_status import get_academic_status_asynchronously
from get_students_courses import get_all_students_courses
from get_students_academic_advisor import get_all_staff_ids, get_advisors_info


# from get_student_number import main
from get_courses import (
    get_canvas_courses,
    get_zero_credit_anthology_courses,
    get_exclude_canvas_course_ids,
)

from get_canvas_course_name import get_canvas_course_name_asynchronously

from get_course_score_grade_link import get_canvas_enrollments_in_bulk_asynchronously, get_canvas_enrollments

from get_attendance_data import get_anthology_attendance_data


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
            json.dumps(
                {
                    "anthology_term_id": anthology_term_id,
                    "canvas_term_id": canvas_term_id,
                },
                default=str,
            ),
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
        school_status_codes = set(request["school_status_codes"])
        check_student_enrollment_ids = set(request.get("check_student_enrollment_ids") or {})

        # get school_status_ids of the active groups of students
        school_status_ids = get_school_status_ids(anthology_base_url, anthology_api_key, school_status_codes)

        # get list of active students by filtering by school_status_ids
        students = get_students(school_status_ids, anthology_base_url, anthology_api_key)

        # format the students info data into a list[dicts]
        students_info = [
            {
                "anthology_student_id": student.get("StudentId"),
                "student_enrollment_period_id": student.get("Id"),
                "sis_link": anthology_base_url + "/#/" + str(student.get("StudentId", "")),
                "status": (student.get("SchoolStatus") or {}).get("Name"),
                "program": student.get("ProgramVersionName"),
                "location": (student.get("Campus") or {}).get("Name"),
                "last_date_of_attendance": student.get("Lda"),
                "enrollment_date": student.get("EnrollmentDate"),
                "graduation_date": student.get("GraduationDate"),
            }
            for student in students
            if not check_student_enrollment_ids or student.get("Id") in check_student_enrollment_ids
        ]

        return func.HttpResponse(
            json.dumps({"students": students_info}, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


# ######################################
# # Scope1 -Part2 - Get List of Students
# ######################################


# @app.function_name(name="GetListOfStudents")
# @app.route(route="", auth_level=func.AuthLevel.FUNCTION)
# def get_list_of_students(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         # retrieve payload and initialize variables
#         request = req.get_json()
#         anthology_api_key = request.pop("anthology_api_key")
#         logging.info(json.dumps({"Request payload": request}, default=str))

#         anthology_base_url = request["anthology_base_url"]
#         term_id = request["term_id"]
#         school_status_codes = set(request["school_status_codes"])
#         check_student_enrollment_ids = set(request.get("check_student_enrollment_ids") or {})

#         # get school_status_ids of the active groups of students
#         school_status_ids = get_school_status_ids(anthology_base_url, anthology_api_key, school_status_codes)

#         # get list of active students by filtering by school_status_ids
#         students = get_students(school_status_ids, anthology_base_url, anthology_api_key)

#         student_ids_and_enrollment_ids_dict = get_student_ids_for_single_vs_multiple_enrollments(
#             students, check_student_enrollment_ids
#         )

#         student_ids_and_enrollment_ids = [
#             {"studentId": k, "studentEnrollmentPeriodId": v, "termId": term_id}
#             for k, v in student_ids_and_enrollment_ids_dict.items()
#         ]

#         logging.info(f"student_ids_and_enrollment_ids: {json.dumps(student_ids_and_enrollment_ids, default=str)}")

#         return func.HttpResponse(json.dumps(student_ids_and_enrollment_ids, default=str), status_code=200)

#     except Exception as err:
#         logging.exception(err)
#         return func.HttpResponse(traceback.format_exc(), status_code=400)


################################################################
# Scope1 -Part3 - Get Student Number, Email, First + Last Names
################################################################


@app.function_name(name="GetStudentNumberEmailFirstLastNames")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_student_number_in_bulk(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        logging.info(f"request: {json.dumps(request, default=str)}")
        students = request["students"]
        anthology_base_url = request["anthology_base_url"]

        # Use asyncio + httpx to retrieve student_number, first_name, last_name, email through a faster asynchronous approach
        student_data = asyncio.run(get_student_data_asynchronously(anthology_api_key, anthology_base_url, students))

        # return the student data
        return func.HttpResponse(json.dumps({"students": student_data}), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


##################################################
# Scope1 -Part4 - Get Canvas Student Id
##################################################


@app.function_name(name="GetCanvasStudentId")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_canvas_student_id(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        canvas_bearer_token = request.pop("canvas_bearer_token")
        logging.info(f"request: {json.dumps(request, default=str)}")
        canvas_base_url = request["canvas_base_url"]
        database_connector = request["database_connector"]
        students = request["students"]

        anthology_student_numbers = {student["anthology_student_number"] for student in students}

        # first, get canvas_student_ids from database if the data is present
        student_ids_from_database = get_canvas_student_ids_from_database(
            tuple(anthology_student_numbers), database_connector
        )
        logging.info(f"student_ids_from_database: {student_ids_from_database}")

        anthology_student_numbers_from_database = set(student_ids_from_database.keys())

        # second, get canvas_student_ids from Canvas API if not present in the database
        student_ids_to_retrieve_from_api = list(anthology_student_numbers - anthology_student_numbers_from_database)
        logging.info(f"student_ids_to_retrieve_from_api: {student_ids_to_retrieve_from_api}")

        student_ids_from_api = asyncio.run(
            get_canvas_student_ids_asynchronously(
                canvas_bearer_token, canvas_base_url, student_ids_to_retrieve_from_api
            )
        )
        logging.info(f"student_ids_from_api: {student_ids_from_api}")

        # third, insert the Canvas API data into the database
        if student_ids_from_api:
            insert_student_ids_into_database(student_ids_from_api, database_connector)

        # combine the database + API data
        full_student_ids_dict = {**student_ids_from_database, **student_ids_from_api}
        logging.info(f"full_student_ids_dict: {full_student_ids_dict}")

        # generate the final results, with the canvas_student_id field added
        modified_students_data = [
            {
                **student,
                "canvas_student_id": full_student_ids_dict.get(student["anthology_student_number"], None),
            }
            for student in students
        ]

        return func.HttpResponse(json.dumps({"students": modified_students_data}, default=str), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)


##########################################
# Scope1 -Part4.4 - Get AOS and Residency
##########################################


@app.function_name(name="GetAOSResidency")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_aos_residency(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request["anthology_api_key"]
        anthology_base_url = request["anthology_base_url"]
        students = request["students"]

        # gets the api data + updates the student dictionary with the AOS + residency info
        modified_students = asyncio.run(
            get_aos_residency_api_data_asynchronously(anthology_api_key, anthology_base_url, students)
        )

        return func.HttpResponse(json.dumps({"students": modified_students}), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)


#####################################
# Scope1 -Part4.5 - Get Prep Program
#####################################


@app.function_name(name="GetPrepProgram")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_prep_program(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request["anthology_api_key"]
        anthology_base_url = request["anthology_base_url"]
        students = request["students"]
        curr_americorp_agency_branch_ids = set(request["curr_americorp_agency_branch_ids"])
        prev_americorp_agency_branch_ids = set(request["prev_americorp_agency_branch_ids"])

        americorp_agency_branch_ids = curr_americorp_agency_branch_ids.union(prev_americorp_agency_branch_ids)

        # get data from API and create dictionary of anthology_student_id: prep_program
        prep_program_dict = get_prep_program_dict(
            anthology_api_key, anthology_base_url, curr_americorp_agency_branch_ids, americorp_agency_branch_ids
        )

        # add prep_program data in
        modified_students = [
            {
                **student,
                "prep_program": prep_program_dict.get(student["anthology_student_id"], {}).get("prep_program"),
                "americorp_status": prep_program_dict.get(student["anthology_student_id"], {}).get("americorp_status"),
            }
            for student in students
        ]

        return func.HttpResponse(
            json.dumps(
                {"students": modified_students},
                default=str,
            ),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), indent=2), status_code=400)


#######################################################################
# Scope1 -Part4.6 - Get Academic Graduation Hold and Registration Hold
#######################################################################


@app.function_name(name="GetAcademicGraduationHoldRegistrationHold")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_academic_graduation_hold_registration_hold(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request["anthology_api_key"]
        anthology_base_url = request["anthology_base_url"]
        students = request["students"]

        modified_students = asyncio.run(
            get_graduation_hold_registration_hold_asynchronously(anthology_api_key, anthology_base_url, students)
        )

        return func.HttpResponse(json.dumps({"students": modified_students}, default=str), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)


########################################
# Scope1 -Part4.7 - Get Academic Status
########################################


@app.function_name(name="GetAcademicStatus")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_academic_status(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        # anthology_api_key = request["anthology_api_key"]
        anthology_api_key = request.pop("anthology_api_key")
        logging.info(f"{request = }")
        anthology_base_url = request["anthology_base_url"]
        students = request["students"]

        modified_students = asyncio.run(
            get_academic_status_asynchronously(anthology_api_key, anthology_base_url, students)
        )

        return func.HttpResponse(json.dumps({"students": modified_students}), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


###########################################################
# Scope1 - Part4 - Get Student Courses And Enrollment Ids
###########################################################


@app.function_name(name="GetSisCourseIdsEnrollmentId")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_sis_course_ids_enrollment_id(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        ## logging.info(f"request: {json.dumps(request)}")
        anthology_base_url = request["anthology_base_url"]
        term_id = request["term_id"]
        students = request["students"]
        # exclude_anthology_course_codes = set(request["exclude_anthology_course_codes"])

        # convert the `students` list[dict] into a dict
        student_info_dict = {
            int(student["anthology_student_id"]): {
                "status": student["status"],
                "anthology_student_number": student["anthology_student_number"],
                "first_name": student["first_name"],
                "last_name": student["last_name"],
                "sis_link": student["sis_link"],
                "email": student["email"],
                "program": student["program"],
                "area_of_study": student["area_of_study"],
                "residency": student["residency"],
                "location": student["location"],
                "prep_program": student["prep_program"],
                "academic_graduation_hold": student["academic_graduation_hold"],
                "registration_hold": student["registration_hold"],
                "americorp_status": student["americorp_status"],
                "academic_status": student["academic_status"],
                "canvas_student_id": student["canvas_student_id"],
                "last_date_of_attendance": student["last_date_of_attendance"],
                "enrollment_date": student["enrollment_date"],
                "graduation_date": student["graduation_date"],
            }
            for student in students
        }

        # get all student courses from Anthology for the current term
        student_courses = get_all_students_courses(anthology_api_key, anthology_base_url, term_id)

        # for student_courses that match a student in the student_info_dict, add fields and append the data into modified_students_data
        student_courses_data = []
        for student_course in student_courses:
            # skip if the student_id isn't in our working list
            if student_course["StudentId"] not in student_info_dict:
                continue

            # else, process the data
            student_info = {
                "anthology_student_id": student_course["StudentId"],
                **student_info_dict[student_course["StudentId"]],
                "sis_course_id": f'AdClassSched_{student_course["ClassSectionId"]}',
                "class_section_id": student_course["ClassSectionId"],
                "student_enrollment_period_id": student_course["StudentEnrollmentPeriodId"],
                "anthology_student_course_id": student_course["Id"],
            }

            student_courses_data.append(student_info)

        logging.info(f"student_courses_data: {student_courses_data}")

        return func.HttpResponse(json.dumps({"student_courses": student_courses_data}, default=str), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


##################################################
# Scope1 -Part5 - Get Student's Academic Advisor
##################################################


@app.function_name(name="GetStudentsAcademicAdvisor")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_students_academic_advisor(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        logging.info(f"request: {json.dumps(request, default=str)}")
        student_courses = request["student_courses"]
        anthology_base_url = request["anthology_base_url"]

        # first, get all staff data
        staff_id_dict = get_all_staff_ids(anthology_api_key, anthology_base_url)

        # next, get each student's academic advisor
        advisors_dict = get_advisors_info(anthology_api_key, anthology_base_url, staff_id_dict)

        # finally, format the data
        modified_student_courses_data = []
        for student in student_courses:
            student_info = {
                **student,
                "advisor_name": advisors_dict.get(student["student_enrollment_period_id"], None),
            }

            modified_student_courses_data.append(student_info)

        return func.HttpResponse(json.dumps({"student_courses": modified_student_courses_data}), status_code=200)

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(traceback.format_exc(), status_code=400)


###############################################
# Scope1 - Part6 - Get Canvas Course Name
###############################################


@app.function_name(name="GetCanvasCourseName")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_canvas_course_name(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        canvas_bearer_token = request.pop("canvas_bearer_token")
        # logging.info(f"request: {json.dumps(request)}")
        canvas_base_url = request["canvas_base_url"]
        student_courses = request["student_courses"]

        sis_course_id_list = list({course["sis_course_id"] for course in student_courses})
        logging.info(f"sis_course_id_list: {sis_course_id_list}")

        course_id_mappings = asyncio.run(
            get_canvas_course_name_asynchronously(canvas_bearer_token, canvas_base_url, sis_course_id_list)
        )
        logging.info(f"course_id_mappings: {course_id_mappings}")

        modified_student_courses_data = [
            {
                **course,
                "course_name": course_id_mappings[course["sis_course_id"]]["canvas_course_name"],
                "canvas_course_id": course_id_mappings[course["sis_course_id"]]["canvas_course_id"],
                # canvas_grade_link = {canvas_base_url}/courses/{canvas_course_id}/grades/{canvas_student_id}
                "canvas_grade_link": (
                    f'{canvas_base_url}/courses/{course_id_mappings[course["sis_course_id"]]["canvas_course_id"]}/grades/{course["canvas_student_id"]}'
                    if (course_id_mappings[course["sis_course_id"]]["canvas_course_id"] and course["canvas_student_id"])
                    else None
                ),
            }
            for course in student_courses
        ]
        logging.info(f"modified_student_courses_data: {modified_student_courses_data}")

        return func.HttpResponse(
            json.dumps({"student_courses": modified_student_courses_data}, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)


###############################################
# Scope2 - Part1a - Get Course Score Grade Link
###############################################


@app.function_name(name="GetCourseScoreGradeLink")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_course_score_grade_link(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        canvas_bearer_token = request.pop("canvas_bearer_token")
        # logging.info(f"request: {json.dumps(request)}")
        canvas_base_url = request["canvas_base_url"]
        student_courses = request["student_courses"]
        database_connector = request["database_connector"]

        # first, generate course_dict to filter Canvas enrollments later
        student_course_dict = {}
        for course in student_courses:
            student_number = course["anthology_student_number"]
            course_id = course["sis_course_id"]
            student_course_dict[student_number] = student_course_dict.get(student_number, []) + [course_id]
        logging.info(f"student_course_dict: {json.dumps(student_course_dict, default=str)}")

        # second, query Canvas API
        list_of_canvas_enrollment_data = asyncio.run(
            get_canvas_enrollments_in_bulk_asynchronously(canvas_bearer_token, canvas_base_url, student_course_dict)
        )
        logging.info(f"list_of_canvas_enrollment_data: {list_of_canvas_enrollment_data}")

        # third, merge the Canvas data into our current data
        dict_of_canvas_enrollment_data = {
            anthology_student_number: course_data
            for enrollment_data in list_of_canvas_enrollment_data
            for anthology_student_number, course_data in enrollment_data.items()
        }
        logging.info(f"dict_of_canvas_enrollment_data: {json.dumps(dict_of_canvas_enrollment_data, default=str)}")

        modified_student_courses_data = []
        for course in student_courses:
            # this should only happen in test Canvas
            if not course["course_name"]:
                continue

            anthology_student_number = course["anthology_student_number"]
            sis_course_id = course["sis_course_id"]

            modified_student_courses_data.append(
                {
                    **course,
                    **dict_of_canvas_enrollment_data[anthology_student_number][sis_course_id],
                }
            )

        # fourth, add data into staging tables
        import pymssql

        sql_statement_merge_insert_staging_student_course_performance = """
            MERGE INTO staging_student_course_performance AS target 
            USING (VALUES (%(anthology_student_id)d, %(course_name)s, %(current_score)d, %(current_grade)s, %(canvas_grade_link)s, %(class_section_id)d)) AS SOURCE (anthology_student_id, course_name, current_score, current_grade, canvas_grade_link, class_section_id)
            ON 
                target.anthology_student_id = SOURCE.anthology_student_id AND 
                target.course_name = SOURCE.course_name
            WHEN NOT MATCHED THEN 
                INSERT (anthology_student_id, course_name, current_score, current_grade, canvas_grade_link, class_section_id)
                VALUES (SOURCE.anthology_student_id, SOURCE.course_name, SOURCE.current_score, SOURCE.current_grade, SOURCE.canvas_grade_link, SOURCE.class_section_id);
        """

        sql_statement_merge_insert_staging_student_info = """
            MERGE INTO staging_student_info AS target 
            USING (VALUES (%(anthology_student_id)d, %(anthology_student_number)d, %(canvas_student_id)d, %(first_name)s, %(last_name)s, %(email)s, %(advisor_name)s)) AS SOURCE (anthology_student_id, anthology_student_number, canvas_student_id, first_name, last_name, email, advisor_name)
            ON 
                target.anthology_student_id = SOURCE.anthology_student_id 
            WHEN MATCHED THEN
                UPDATE SET 
                    target.anthology_student_number = SOURCE.anthology_student_number, 
                    target.canvas_student_id = SOURCE.canvas_student_id, 
                    target.first_name = SOURCE.first_name, 
                    target.last_name = SOURCE.last_name, 
                    target.email = SOURCE.email, 
                    target.advisor_name = SOURCE.advisor_name
            WHEN NOT MATCHED THEN 
                INSERT (anthology_student_id, anthology_student_number, canvas_student_id, first_name, last_name, email, advisor_name)
                VALUES (SOURCE.anthology_student_id, SOURCE.anthology_student_number, SOURCE.canvas_student_id, SOURCE.first_name, SOURCE.last_name, SOURCE.email, SOURCE.advisor_name);
        """

        with pymssql.connect(**database_connector) as conn:
            with conn.cursor(as_dict=True) as cursor:
                # insert student_course_performance data into the staging table
                cursor.executemany(
                    sql_statement_merge_insert_staging_student_course_performance,
                    modified_student_courses_data,
                )

                cursor.executemany(
                    sql_statement_merge_insert_staging_student_info,
                    modified_student_courses_data,
                )

                conn.commit()

        return func.HttpResponse(
            json.dumps({"student_courses": modified_student_courses_data}, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)


########################################
# Scope2 - Part1b - Get Attendance Data
########################################


@app.function_name(name="GetAttendanceData")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def get_attendance_data(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        anthology_api_key = request.pop("anthology_api_key")
        # canvas_bearer_token = request.pop("canvas_bearer_token")
        # logging.info(f"request: {json.dumps(request)}")
        anthology_base_url = request["anthology_base_url"]
        # canvas_base_url = request["canvas_base_url"]
        thirty_days_ago_datetime = request["thirty_days_ago_datetime"]
        database_connector = request["database_connector"]
        student_courses = request["student_courses"]

        # first, get list of student course ids
        # student_course_id_set = {course["anthology_student_course_id"] for course in student_courses}
        student_course_id_dict = {
            course["anthology_student_course_id"]: {
                "anthology_student_id": course["anthology_student_id"],
                "course_name": course["course_name"],
                "class_section_id": course["class_section_id"],
            }
            for course in student_courses
        }
        logging.info(f"student_course_id_dict: {student_course_id_dict}")

        list_of_attendance_data = get_anthology_attendance_data(
            anthology_api_key, anthology_base_url, thirty_days_ago_datetime
        )
        logging.info(f"len(list_of_attendance_data): {len(list_of_attendance_data)}")

        student_attendance_data = [
            {
                "attendance_date": attendance["AttendanceDate"],
                "attended_minutes": attendance["Attended"],
                "absent_minutes": attendance["Absent"],
                "is_excused_absence": 1 if attendance["IsExcusedAbsence"] else 0,
                **student_course_id_dict[attendance["StudentCourseId"]],
            }
            for attendance in list_of_attendance_data
            if attendance["StudentCourseId"] in student_course_id_dict
        ]

        logging.info(f"student_attendance_data: {student_attendance_data}")

        # add data into staging tables
        import pymssql

        sql_statement_merge_insert_staging_student_course_attendance = """
            MERGE INTO staging_student_course_attendance AS target 
            USING (VALUES (%(anthology_student_id)d, %(course_name)s, %(attendance_date)s, %(attended_minutes)d, %(absent_minutes)d, %(is_excused_absence)d, %(class_section_id)d)) AS SOURCE (anthology_student_id, course_name, attendance_date, attended_minutes, absent_minutes, is_excused_absence, class_section_id)
            ON 
                target.anthology_student_id = SOURCE.anthology_student_id AND 
                target.course_name = SOURCE.course_name AND
                target.attendance_date = SOURCE.attendance_date
            WHEN NOT MATCHED THEN 
                INSERT (anthology_student_id, course_name, attendance_date, attended_minutes, absent_minutes, is_excused_absence, class_section_id)
                VALUES (SOURCE.anthology_student_id, SOURCE.course_name, SOURCE.attendance_date, SOURCE.attended_minutes, SOURCE.absent_minutes, SOURCE.is_excused_absence, SOURCE.class_section_id);
        """

        with pymssql.connect(**database_connector) as conn:
            with conn.cursor(as_dict=True) as cursor:
                # insert student_course_performance data into the staging table

                cursor.executemany(
                    sql_statement_merge_insert_staging_student_course_attendance,
                    student_attendance_data,
                )

                conn.commit()

        return func.HttpResponse(
            json.dumps({"student_attendance_data": student_attendance_data}, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)


#######################################################
# Scope2 - Part1c - Insert Master Student Tracker Data
#######################################################


@app.function_name(name="CalculateAndInsertMasterStudentTrackerData")
@app.route(route="", auth_level=func.AuthLevel.FUNCTION)
def calculate_and_insert_master_student_tracker_data(req: func.HttpRequest) -> func.HttpResponse:
    try:
        request = req.get_json()
        database_connector = request["database_connector"]
        student_courses = request["student_courses"]

        master_student_tracker_data = []
        seen = set()
        for student in student_courses:
            if student["anthology_student_id"] not in seen:
                master_student_tracker_data.append(
                    {
                        "anthology_student_id": student["anthology_student_id"],
                        "sis_link": student["sis_link"],
                        "program": student["program"],
                        "area_of_study": student["area_of_study"],
                        "residency": student["residency"],
                        "location": student["location"],
                        "prep_program": student["prep_program"],
                        "academic_graduation_hold": 1 if student["academic_graduation_hold"] else 0,
                        "registration_hold": 1 if student["registration_hold"] else 0,
                        "americorp_status": student["americorp_status"],
                        "academic_status": student["academic_status"],
                        "last_date_of_attendance": student["last_date_of_attendance"],
                        "enrollment_date": student["enrollment_date"],
                        "graduation_date": student["graduation_date"],
                    }
                )
            seen.add(student["anthology_student_id"])

        # add data into staging tables
        import pymssql

        sql_statement_merge_insert_staging_master_student_tracker = """
            MERGE INTO staging_master_student_tracker AS target 

            USING (VALUES (
                %(anthology_student_id)d, 
                %(sis_link)s, 
                %(program)s, 
                %(area_of_study)s, 
                %(residency)s, 
                %(location)s, 
                %(prep_program)s, 
                %(academic_graduation_hold)d, 
                %(registration_hold)d, 
                %(americorp_status)s, 
                %(academic_status)s,
                %(last_date_of_attendance)s, 
                %(enrollment_date)s, 
                %(graduation_date)s
            )) AS SOURCE (anthology_student_id, sis_link, program, area_of_study, residency, location, prep_program, academic_graduation_hold, registration_hold, americorp_status, academic_status, last_date_of_attendance, enrollment_date, graduation_date)
            
            ON target.anthology_student_id = SOURCE.anthology_student_id

            WHEN NOT MATCHED THEN 
                INSERT (anthology_student_id, sis_link, program, area_of_study, residency, location, prep_program, academic_graduation_hold, registration_hold, americorp_status, academic_status, last_date_of_attendance, enrollment_date, graduation_date)
                VALUES (source.anthology_student_id, source.sis_link, source.program, source.area_of_study, source.residency, source.location, source.prep_program, source.academic_graduation_hold, source.registration_hold, source.americorp_status, source.academic_status, source.last_date_of_attendance, source.enrollment_date, source.graduation_date);
        """

        with pymssql.connect(**database_connector) as conn:
            with conn.cursor(as_dict=True) as cursor:
                # insert student_course_performance data into the staging table

                cursor.executemany(
                    sql_statement_merge_insert_staging_master_student_tracker,
                    master_student_tracker_data,
                )

                conn.commit()

        return func.HttpResponse(
            json.dumps({"master_student_tracker_data": master_student_tracker_data}, default=str),
            status_code=200,
        )

    except Exception as err:
        logging.exception(err)
        return func.HttpResponse(json.dumps(traceback.format_exc(), default=str), status_code=400)

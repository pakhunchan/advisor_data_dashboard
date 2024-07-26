import httpx
import asyncio
import json
import logging


async def get_canvas_enrollments_in_bulk_asynchronously(canvas_bearer_token, canvas_base_url, student_course_dict):
    student_numbers_list = list(student_course_dict.keys())
    list_of_canvas_enrollment_data = []

    size_per_chunk = 10
    number_of_chunks = len(student_numbers_list) // size_per_chunk + 1

    async with httpx.AsyncClient() as client:
        for i in range(number_of_chunks):
            tasks = [
                get_canvas_enrollments(
                    canvas_bearer_token, canvas_base_url, student_number, student_course_dict, client
                )
                for student_number in student_numbers_list[size_per_chunk * i : size_per_chunk * (i + 1)]
            ]

            response = await asyncio.gather(*tasks)
            list_of_canvas_enrollment_data.extend(response)

    return list_of_canvas_enrollment_data


async def get_canvas_enrollments(
    canvas_bearer_token: str,
    canvas_base_url: str,
    anthology_student_number: int,
    student_course_dict: dict,
    client: httpx.AsyncClient,
) -> dict:
    max_retries = 3
    base_delay = 2

    url = f"{canvas_base_url}/api/v1/users/sis_user_id:{anthology_student_number}/enrollments?per_page=100"
    for sis_course_id in student_course_dict[anthology_student_number]:
        url += f"&sis_course_id[]={sis_course_id}"

    headers = {"Authorization": f"Bearer {canvas_bearer_token}"}
    params = {"per_page": 100}

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url=url, headers=headers, params=params, timeout=30.0)
            response.raise_for_status()
            break
        except httpx.HTTPStatusError as err:
            # Canvas returns a 404 error if the enrollment doesn't exist
            if err.response.status_code == 404:
                logging.exception(err)
                # Return nulls when enrollment doesn't exist
                return {
                    anthology_student_number: {
                        sis_course_id: {
                            "current_score": None,
                            "current_grade": None,
                        }
                        for sis_course_id in student_course_dict[anthology_student_number]
                    }
                }
            else:
                raise
        except Exception as err:
            logging.exception(err)
            if attempt >= max_retries:
                response.raise_for_status()
            await asyncio.sleep(base_delay * 2**attempt)

    enrollment_results = get_formatted_results(anthology_student_number, response, student_course_dict)

    return enrollment_results


def get_formatted_results(anthology_student_number, response, student_course_dict):
    # pre-create the enrollment_results
    enrollment_results = {
        anthology_student_number: {
            sis_course_id: {
                "current_score": None,
                "current_grade": None,
            }
            for sis_course_id in student_course_dict[anthology_student_number]
        }
    }

    # return nulls if no results from API. I've seen 2 formats for an empty Canvas API result:
    # 1) empty array []
    # 2) response status = 404 with results = {'errors': [{'message': 'The specified resource does not exist.'}]}
    results = response.json()
    if response.status_code == 404 or not results:
        return enrollment_results

    for enrollment in results:
        sis_course_id = enrollment["sis_course_id"]
        sis_user_id = int(enrollment["sis_user_id"])

        if sis_user_id in student_course_dict and sis_course_id in student_course_dict[sis_user_id]:
            enrollment_results[anthology_student_number][sis_course_id] = {
                "canvas_grade_link": enrollment.get("grades", {}).get("html_url", None),
                "current_score": enrollment.get("grades", {}).get("current_score", None),
                "current_grade": enrollment.get("grades", {}).get("current_grade", None),
            }

    return enrollment_results


# ################
# ################

# import httpx
# import json
# import logging


# def get_canvas_enrollments(
#     canvas_bearer_token: str, canvas_base_url: str, anthology_student_number: int, student_course_dict: dict
# ) -> dict:

#     url = f"{canvas_base_url}/api/v1/users/sis_user_id:{anthology_student_number}/enrollments?per_page=100"
#     for sis_course_id in student_course_dict[anthology_student_number]:
#         url += f"&sis_course_id[]={sis_course_id}"

#     headers = {"Authorization": f"Bearer {canvas_bearer_token}"}
#     params = {"per_page": 100}

#     with httpx.Client() as client:
#         try:
#             response = client.get(url=url, headers=headers, params=params, timeout=60.0)
#             # results = response.json()
#         except Exception as err:
#             logging.exception(err)
#             raise

#     enrollment_results = get_formatted_results(anthology_student_number, response, student_course_dict)

#     return enrollment_results


# def get_formatted_results(anthology_student_number, response, student_course_dict):
#     # pre-create the enrollment_results
#     enrollment_results = {anthology_student_number: {}}
#     # return nulls if no results from API. I've seen 2 formats for an empty Canvas API result:
#     # 1) empty array []
#     # 2) response status = 404 with results = {'errors': [{'message': 'The specified resource does not exist.'}]}
#     results = response.json()
#     if response.status_code == 404 or not results:
#         enrollment_results[anthology_student_number] = {
#             sis_course_id: {
#                 "canvas_grade_link": None,
#                 "current_score": None,
#                 "current_grade": None,
#             }
#             for sis_course_id in student_course_dict[anthology_student_number]
#         }
#         return enrollment_results

#     for enrollment in results:
#         sis_course_id = enrollment["sis_course_id"]
#         sis_user_id = int(enrollment["sis_user_id"])
#         logging.info(f"sis_course_id: {sis_course_id}")
#         logging.info(f"sis_user_id: {sis_user_id}")
#         logging.info(f"type(sis_user_id): {type(sis_user_id)}")
#         logging.info(f"sis_user_id in student_course_dict: {sis_user_id in student_course_dict}")
#         logging.info(
#             f"sis_course_id in student_course_dict[sis_user_id]: {sis_course_id in student_course_dict[sis_user_id]}"
#         )
#         if sis_user_id in student_course_dict and sis_course_id in student_course_dict[sis_user_id]:
#             enrollment_results[anthology_student_number][sis_course_id] = {
#                 "canvas_grade_link": enrollment.get("grades", {}).get("html_url", None),
#                 "current_score": enrollment.get("grades", {}).get("current_score", None),
#                 "current_grade": enrollment.get("grades", {}).get("html_url", None),
#             }
#         else:
#             enrollment_results[anthology_student_number][sis_course_id] = {
#                 "canvas_grade_link": None,
#                 "current_score": None,
#                 "current_grade": None,
#             }

#     return enrollment_results

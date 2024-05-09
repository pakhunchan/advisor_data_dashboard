l1 = [
    {
        "anthology_student_id": 358,
        "anthology_student_number": 464,
        "first_name": "TESTPortia2",
        "last_name": "TESTDavenport2",
        "email": "test.testdavenport@student.relay.edux",
        "canvas_student_id": 3339,
    },
    {
        "anthology_student_id": 581,
        "anthology_student_number": 687,
        "first_name": "Bryanna",
        "last_name": "Bonner",
        "email": "bryanna.bonner@student.relay.edux",
        "canvas_student_id": 1739,
    },
    {
        "anthology_student_id": 1779,
        "anthology_student_number": 1893,
        "first_name": "test",
        "last_name": "cmc",
        "email": "test@cmc.com",
        "canvas_student_id": 3851,
    },
    {
        "anthology_student_id": 1789,
        "anthology_student_number": 1903,
        "first_name": "Justin",
        "last_name": "Fields",
        "email": "",
        "canvas_student_id": None,
    },
    {
        "anthology_student_id": 170,
        "anthology_student_number": 276,
        "first_name": "Ariauna",
        "last_name": "Brown",
        "email": "ariauna.brown@student.relay.edux",
        "canvas_student_id": 1212,
    },
    {
        "anthology_student_id": 1736,
        "anthology_student_number": 1848,
        "first_name": "Judithliana",
        "last_name": "Serrano",
        "email": "judithliana.serrano@student.relay.edux",
        "canvas_student_id": 3626,
    },
]


l2 = [
    {
        "anthology_student_id": 358,
        "anthology_student_number": 464,
        "first_name": "TESTPortia2",
        "last_name": "TESTDavenport2",
        "email": "test.testdavenport@student.relay.edux",
    },
    {
        "anthology_student_id": 581,
        "anthology_student_number": 687,
        "first_name": "Bryanna",
        "last_name": "Bonner",
        "email": "bryanna.bonner@student.relay.edux",
    },
    {
        "anthology_student_id": 1779,
        "anthology_student_number": 1893,
        "first_name": "test",
        "last_name": "cmc",
        "email": "test@cmc.com",
    },
    {
        "anthology_student_id": 1789,
        "anthology_student_number": 1903,
        "first_name": "Justin",
        "last_name": "Fields",
        "email": "",
    },
    {
        "anthology_student_id": 170,
        "anthology_student_number": 276,
        "first_name": "Ariauna",
        "last_name": "Brown",
        "email": "ariauna.brown@student.relay.edux",
    },
    {
        "anthology_student_id": 1736,
        "anthology_student_number": 1848,
        "first_name": "Judithliana",
        "last_name": "Serrano",
        "email": "judithliana.serrano@student.relay.edux",
    },
]

print(len(l1), len(l2))

for student in l1:
    student.pop("canvas_student_id")

print(l1)
l1_modified = [{}]
l1.sort(key=lambda x: x["anthology_student_id"])
l2.sort(key=lambda x: x["anthology_student_id"])
print(l1 == l2)

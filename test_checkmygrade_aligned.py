import random
import shutil
import tempfile
import time
import unittest
from pathlib import Path

import checkmygrade_aligned as cmg


COURSES = ["DATA200", "CS101", "MATH301", "BIO110", "PHYS200"]


def make_student(idx: int, course: str = "DATA200") -> cmg.Student:
    marks = round(random.uniform(40.0, 100.0), 2)
    return cmg.Student(
        f"First{idx}",
        f"Last{idx}",
        f"student{idx}@testcsu.edu",
        course,
        cmg.grade_scale.letter_for_marks(marks),
        marks,
    )


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp(prefix="cmg_test_")
        cmg.set_data_dir(Path(cls._tmpdir))
        cmg.CSVFileHandler.initialise_files()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def setUp(self):
        for table in cmg.FILE_MAP:
            cmg.CSVFileHandler.write_all(table, [])
        for cid in COURSES:
            cmg.Course.add_new_course(cmg.Course(cid, f"Course {cid}", f"Description for {cid}"))


class TestEncryption(BaseTestCase):
    def test_roundtrip(self):
        for pw in ["Welcome12#_", "Pa$$w0rd!", "AQ10134"]:
            self.assertEqual(cmg._decrypt_password(cmg._encrypt_password(pw)), pw)

    def test_encrypted_differs(self):
        self.assertNotEqual(cmg._encrypt_password("Welcome12#_"), "Welcome12#_")


class TestStudentCRUD(BaseTestCase):
    def test_add_delete_modify_student(self):
        student = cmg.Student("Sam", "Carpenter", "sam@test.edu", "DATA200", "A", 96.0)
        cmg.Student.add_new_student(student)
        self.assertEqual(len(cmg.Student.get_by_email("sam@test.edu")), 1)

        student.update_student_record(marks=84.0)
        row = cmg.CSVFileHandler.find_rows("students", "email_address", "sam@test.edu")[0]
        self.assertEqual(float(row["marks"]), 84.0)
        self.assertEqual(row["grades"], "B")

        deleted = cmg.Student.delete_enrollment("sam@test.edu", "DATA200")
        self.assertTrue(deleted)
        self.assertEqual(cmg.Student.get_by_email("sam@test.edu"), [])

    def test_same_student_multiple_courses(self):
        cmg.Student.add_new_student(cmg.Student("Sam", "Carpenter", "sam@test.edu", "DATA200", "A", 96.0))
        cmg.Student.add_new_student(cmg.Student("Sam", "Carpenter", "sam@test.edu", "CS101", "B", 82.0))
        self.assertEqual(len(cmg.Student.get_by_email("sam@test.edu")), 2)

    def test_requires_existing_course(self):
        with self.assertRaises(ValueError):
            cmg.Student.add_new_student(cmg.Student("A", "B", "x@test.edu", "NOPE101", "A", 91))

    def test_insert_1000_records_with_timing(self):
        start = time.perf_counter()
        for idx in range(1000):
            cmg.Student.add_new_student(make_student(idx, random.choice(COURSES)))
        elapsed = time.perf_counter() - start
        print(f"\nInserted 1000 student rows in {elapsed * 1000:.2f} ms")
        self.assertGreaterEqual(len(cmg.Student.get_all()), 1000)


class TestSearchAndSort(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        for table in cmg.FILE_MAP:
            cmg.CSVFileHandler.write_all(table, [])
        for cid in COURSES:
            cmg.Course.add_new_course(cmg.Course(cid, f"Course {cid}", f"Description for {cid}"))
        for idx in range(1000):
            cmg.Student.add_new_student(make_student(idx, random.choice(COURSES)))
        cls.students = cmg.Student.get_all()

    def test_binary_search_by_email(self):
        unique = list({s.email: s for s in self.students}.values())
        sorted_rows, _ = cmg.SearchSort.sort_by(unique, lambda s: s.email)
        result, elapsed = cmg.SearchSort.binary_search_by_email(sorted_rows, "student500@testcsu.edu")
        print(f"\nBinary search time: {elapsed * 1000:.4f} ms")
        self.assertIsNotNone(result)

    def test_linear_search_by_course(self):
        results, elapsed = cmg.SearchSort.linear_search(self.students, lambda s: s.course_id, "DATA200")
        print(f"\nLinear search time: {elapsed * 1000:.4f} ms")
        self.assertGreater(len(results), 0)

    def test_sort_by_marks(self):
        sorted_rows, elapsed = cmg.SearchSort.quick_sort(self.students, lambda s: s.marks)
        print(f"\nQuick sort time: {elapsed * 1000:.4f} ms")
        marks = [s.marks for s in sorted_rows]
        self.assertEqual(marks, sorted(marks))

    def test_sort_by_email(self):
        sorted_rows, _ = cmg.SearchSort.sort_by(self.students, lambda s: s.email)
        emails = [s.email for s in sorted_rows]
        self.assertEqual(emails, sorted(emails))


class TestCourseCRUD(BaseTestCase):
    def test_add_delete_modify_course(self):
        cmg.Course.add_new_course(cmg.Course("ENG101", "English", "Writing"))
        course = next(c for c in cmg.Course.get_all() if c.course_id == "ENG101")
        course.modify_course(course_name="English I")
        row = cmg.CSVFileHandler.find_row("courses", "course_id", "ENG101")
        self.assertEqual(row["course_name"], "English I")
        deleted = cmg.Course.delete_new_course("ENG101")
        self.assertTrue(deleted)

    def test_prevent_delete_referenced_course(self):
        cmg.Student.add_new_student(cmg.Student("Sam", "C", "sam@test.edu", "DATA200", "A", 95))
        with self.assertRaises(ValueError):
            cmg.Course.delete_new_course("DATA200")


class TestProfessorCRUD(BaseTestCase):
    def test_add_delete_modify_professor(self):
        cmg.Professor.add_new_professor(cmg.Professor("Dr. Smith", "smith@test.edu", "Professor", "DATA200"))
        prof = cmg.Professor.get_by_email("smith@test.edu")[0]
        prof.modify_professor_details(rank="Senior Professor")
        row = cmg.CSVFileHandler.find_row("professors", "professor_id", "smith@test.edu")
        self.assertEqual(row["rank"], "Senior Professor")
        self.assertTrue(cmg.Professor.delete_professor("smith@test.edu"))

    def test_professor_requires_existing_course(self):
        with self.assertRaises(ValueError):
            cmg.Professor.add_new_professor(cmg.Professor("Dr. X", "x@test.edu", "Professor", "NOPE101"))


class TestRoleAndSync(BaseTestCase):
    def test_register_student_is_atomic(self):
        with self.assertRaises(ValueError):
            cmg.register_user(
                "badstudent@test.edu",
                "Pass1!",
                "student",
                first_name="Bad",
                last_name="Student",
                course_id="NOPE101",
                marks=50,
            )
        self.assertIsNone(cmg.CSVFileHandler.find_row("login", "user_id", "badstudent@test.edu"))

    def test_admin_delete_user_cleans_profiles(self):
        cmg.register_user(
            "sam@test.edu",
            "Pass1!",
            "student",
            first_name="Sam",
            last_name="Carpenter",
            course_id="DATA200",
            marks=96,
        )
        admin = cmg.Admin("admin@test.edu", cmg._encrypt_password("Admin1!"), "admin")
        admin.delete_user("sam@test.edu")
        self.assertIsNone(cmg.CSVFileHandler.find_row("login", "user_id", "sam@test.edu"))
        self.assertEqual(cmg.Student.get_by_email("sam@test.edu"), [])

    def test_role_change_blocks_inconsistent_profiles(self):
        cmg.register_user(
            "sam@test.edu",
            "Pass1!",
            "student",
            first_name="Sam",
            last_name="Carpenter",
            course_id="DATA200",
            marks=96,
        )
        admin = cmg.Admin("admin@test.edu", cmg._encrypt_password("Admin1!"), "admin")
        with self.assertRaises(ValueError):
            admin.change_user_role("sam@test.edu", "professor")


if __name__ == "__main__":
    unittest.main(verbosity=2)

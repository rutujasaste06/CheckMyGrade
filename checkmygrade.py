from __future__ import annotations

import base64
import csv
import os
import statistics
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional


BASE_DIR = Path(__file__).parent                    
DATA_DIR = BASE_DIR / "data"

HEADERS = {
    "students": ["email_address", "first_name", "last_name", "course_id", "grades", "marks"],
    "courses": ["course_id", "course_name", "description"],
    "professors": ["professor_id", "professor_name", "rank", "course_id"],
    "login": ["user_id", "password", "role"],
}

FILE_MAP: dict[str, Path] = {}


def set_data_dir(path: Path | str) -> None:
    global DATA_DIR, FILE_MAP
    DATA_DIR = Path(path)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FILE_MAP = {
        "students": DATA_DIR / "students.csv",
        "courses": DATA_DIR / "courses.csv",
        "professors": DATA_DIR / "professors.csv",
        "login": DATA_DIR / "login.csv",
    }


set_data_dir(DATA_DIR)


class CSVFileHandler:
    @staticmethod
    def initialise_files() -> None:
        for table, path in FILE_MAP.items():
            if not path.exists():
                with path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=HEADERS[table])
                    writer.writeheader()

    @staticmethod
    def read_all(table: str) -> list[dict[str, str]]:
        path = FILE_MAP[table]
        if not path.exists():
            return []
        with path.open("r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def write_all(table: str, rows: list[dict[str, object]]) -> None:
        path = FILE_MAP[table]
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS[table])
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def write_row(table: str, row: dict[str, object]) -> None:
        path = FILE_MAP[table]
        exists = path.exists() and os.path.getsize(path) > 0
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS[table])
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    @staticmethod
    def find_row(table: str, key_field: str, key_value: str) -> Optional[dict[str, str]]:
        needle = key_value.strip().lower()
        for row in CSVFileHandler.read_all(table):
            if row.get(key_field, "").strip().lower() == needle:
                return row
        return None

    @staticmethod
    def find_rows(table: str, key_field: str, key_value: str) -> list[dict[str, str]]:
        needle = key_value.strip().lower()
        return [
            row for row in CSVFileHandler.read_all(table)
            if row.get(key_field, "").strip().lower() == needle
        ]

    @staticmethod
    def exists(table: str, key_field: str, key_value: str) -> bool:
        return CSVFileHandler.find_row(table, key_field, key_value) is not None

    @staticmethod
    def delete_rows(table: str, key_field: str, key_value: str) -> int:
        rows = CSVFileHandler.read_all(table)
        needle = key_value.strip().lower()
        kept = [r for r in rows if r.get(key_field, "").strip().lower() != needle]
        deleted = len(rows) - len(kept)
        if deleted:
            CSVFileHandler.write_all(table, kept)
        return deleted

    @staticmethod
    def delete_one_row(table: str, conditions: dict[str, str]) -> bool:
        rows = CSVFileHandler.read_all(table)
        normalized = {k: v.strip().lower() for k, v in conditions.items()}
        for i, row in enumerate(rows):
            if all(row.get(k, "").strip().lower() == v for k, v in normalized.items()):
                del rows[i]
                CSVFileHandler.write_all(table, rows)
                return True
        return False

    @staticmethod
    def update_row(table: str, key_field: str, key_value: str, updated: dict[str, object]) -> bool:
        rows = CSVFileHandler.read_all(table)
        needle = key_value.strip().lower()
        for i, row in enumerate(rows):
            if row.get(key_field, "").strip().lower() == needle:
                row.update(updated)
                rows[i] = row
                CSVFileHandler.write_all(table, rows)
                return True
        return False

    @staticmethod
    def update_matching_row(table: str, conditions: dict[str, str], updated: dict[str, object]) -> bool:
        rows = CSVFileHandler.read_all(table)
        normalized = {k: v.strip().lower() for k, v in conditions.items()}
        for i, row in enumerate(rows):
            if all(row.get(k, "").strip().lower() == v for k, v in normalized.items()):
                row.update(updated)
                rows[i] = row
                CSVFileHandler.write_all(table, rows)
                return True
        return False


_SHIFT = 7
_SALT = "CMG_SALT"


def _encrypt_password(plain: str) -> str:
    shifted = "".join(chr((ord(c) + _SHIFT) % 256) for c in plain)
    return base64.b64encode((_SALT + shifted).encode("utf-8")).decode("utf-8")


def _decrypt_password(encrypted: str) -> str:
    decoded = base64.b64decode(encrypted.encode("utf-8")).decode("utf-8")
    if decoded.startswith(_SALT):
        decoded = decoded[len(_SALT):]
    return "".join(chr((ord(c) - _SHIFT) % 256) for c in decoded)


class Person(ABC):
    def __init__(self, name: str, email: str):
        if not name or not name.strip():
            raise ValueError("Name cannot be empty.")
        if not email or "@" not in email:
            raise ValueError(f"Invalid email: {email!r}")
        self._name = name.strip()
        self._email = email.strip().lower()

    @property
    def name(self) -> str:
        return self._name

    @property
    def email(self) -> str:
        return self._email

    @abstractmethod
    def display(self) -> str:
        raise NotImplementedError


class DataValidationError(ValueError):
    pass


class ParseMixin:
    @staticmethod
    def _load_objects(table: str, ctor: Callable[[dict[str, str]], object]) -> list[object]:
        result: list[object] = []
        for idx, row in enumerate(CSVFileHandler.read_all(table), start=2):
            try:
                result.append(ctor(row))
            except Exception as exc:
                raise DataValidationError(f"Invalid row in {table}.csv at line {idx}: {row}. Error: {exc}") from exc
        return result


@dataclass
class GradeScale:
    grade: str
    min_marks: float
    max_marks: float

    def in_range(self, marks: float) -> bool:
        return self.min_marks <= marks <= self.max_marks


class Grades:
    def __init__(self):
        self._scale = [
            GradeScale("A", 90.0, 100.0),
            GradeScale("B", 80.0, 89.99),
            GradeScale("C", 70.0, 79.99),
            GradeScale("D", 60.0, 69.99),
            GradeScale("F", 0.0, 59.99),
        ]

    def letter_for_marks(self, marks: float) -> str:
        for gs in sorted(self._scale, key=lambda x: x.min_marks, reverse=True):
            if gs.in_range(marks):
                return gs.grade
        return "F"

    def add_grade(self, grade: str, min_marks: float, max_marks: float) -> None:
        grade = grade.upper().strip()
        if any(x.grade == grade for x in self._scale):
            raise ValueError(f"Grade {grade!r} already exists.")
        if min_marks > max_marks:
            raise ValueError("min_marks must be <= max_marks")
        self._scale.append(GradeScale(grade, min_marks, max_marks))

    def delete_grade(self, grade: str) -> None:
        grade = grade.upper().strip()
        before = len(self._scale)
        self._scale = [g for g in self._scale if g.grade != grade]
        if len(self._scale) == before:
            raise KeyError(f"Grade {grade!r} not found.")

    def modify_grade(self, grade: str, min_marks: float, max_marks: float) -> None:
        if min_marks > max_marks:
            raise ValueError("min_marks must be <= max_marks")
        grade = grade.upper().strip()
        for g in self._scale:
            if g.grade == grade:
                g.min_marks = min_marks
                g.max_marks = max_marks
                return
        raise KeyError(f"Grade {grade!r} not found.")

    def display_grade_report(self) -> str:
        rows = [f"  {'Grade':<8} {'Min':>6} {'Max':>6}", "  " + "-" * 22]
        for g in sorted(self._scale, key=lambda x: x.min_marks, reverse=True):
            rows.append(f"  {g.grade:<8} {g.min_marks:>6.1f} {g.max_marks:>6.1f}")
        return "\n".join(rows)


grade_scale = Grades()


class Student(Person, ParseMixin):
    VALID_GRADES = {"A", "B", "C", "D", "F"}

    def __init__(self, first_name: str, last_name: str, email: str, course_id: str, grades: str = "F", marks: float = 0.0):
        super().__init__(f"{first_name} {last_name}", email)
        if not first_name.strip() or not last_name.strip():
            raise ValueError("First name and last name cannot be empty.")
        if not course_id or not course_id.strip():
            raise ValueError("course_id cannot be empty.")
        grades = grades.strip().upper()
        if grades not in self.VALID_GRADES:
            raise ValueError(f"Invalid grade {grades!r}")
        marks = float(marks)
        if not (0.0 <= marks <= 100.0):
            raise ValueError("Marks must be between 0 and 100.")
        self.first_name = first_name.strip()
        self.last_name = last_name.strip()
        self.course_id = course_id.strip().upper()
        self.grades = grades
        self.marks = marks

    def display(self) -> str:
        return (
            f"  Email   : {self.email}\n"
            f"  Name    : {self.name}\n"
            f"  Course  : {self.course_id}\n"
            f"  Grade   : {self.grades}\n"
            f"  Marks   : {self.marks:.1f}"
        )

    def display_records(self) -> str:
        return self.display()

    def check_my_grades(self) -> str:
        rows = Student.get_by_email(self.email)
        if not rows:
            return f"  No records found for {self.email}."
        lines = [f"\n  Grades for {self.name} ({self.email})", f"  {'Course':<12} {'Grade':<8} {'Marks':>6}", "  " + "-" * 30]
        for row in rows:
            lines.append(f"  {row.course_id:<12} {row.grades:<8} {row.marks:>6.1f}")
        return "\n".join(lines)

    def check_my_marks(self) -> float:
        return self.marks

    def to_dict(self) -> dict[str, object]:
        return {
            "email_address": self.email,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "course_id": self.course_id,
            "grades": self.grades,
            "marks": self.marks,
        }

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> "Student":
        return cls(d["first_name"], d["last_name"], d["email_address"], d["course_id"], d["grades"], float(d["marks"]))

    @classmethod
    def add_new_student(cls, student: "Student") -> None:
        if not Course.exists(student.course_id):
            raise ValueError(f"Course {student.course_id!r} does not exist.")
        for row in CSVFileHandler.find_rows("students", "email_address", student.email):
            if row.get("course_id", "").strip().upper() == student.course_id:
                raise ValueError(f"Student {student.email!r} is already enrolled in {student.course_id!r}.")
        CSVFileHandler.write_row("students", student.to_dict())

    @classmethod
    def delete_new_student(cls, email: str) -> int:
        count = CSVFileHandler.delete_rows("students", "email_address", email)
        CSVFileHandler.delete_rows("login", "user_id", email)
        return count

    @classmethod
    def delete_enrollment(cls, email: str, course_id: str) -> bool:
        deleted = CSVFileHandler.delete_one_row("students", {"email_address": email, "course_id": course_id})
        if deleted and not CSVFileHandler.find_rows("students", "email_address", email):
            row = CSVFileHandler.find_row("login", "user_id", email)
            if row and row.get("role", "").strip().lower() == "student":
                CSVFileHandler.delete_rows("login", "user_id", email)
        return deleted

    @classmethod
    def get_all(cls) -> list["Student"]:
        return cls._load_objects("students", cls.from_dict)  # type: ignore[return-value]

    @classmethod
    def get_by_email(cls, email: str) -> list["Student"]:
        result = []
        for idx, row in enumerate(CSVFileHandler.find_rows("students", "email_address", email), start=2):
            try:
                result.append(cls.from_dict(row))
            except Exception as exc:
                raise DataValidationError(f"Invalid student row near line {idx}: {row}. Error: {exc}") from exc
        return result

    @classmethod
    def get_by_course(cls, course_id: str) -> list["Student"]:
        result = []
        for idx, row in enumerate(CSVFileHandler.find_rows("students", "course_id", course_id), start=2):
            try:
                result.append(cls.from_dict(row))
            except Exception as exc:
                raise DataValidationError(f"Invalid student row near line {idx}: {row}. Error: {exc}") from exc
        return result

    def update_student_record(self, first_name: Optional[str] = None, last_name: Optional[str] = None, grades: Optional[str] = None, marks: Optional[float] = None) -> None:
        if first_name:
            self.first_name = first_name.strip()
        if last_name:
            self.last_name = last_name.strip()
        self._name = f"{self.first_name} {self.last_name}"
        if marks is not None:
            marks = float(marks)
            if not (0.0 <= marks <= 100.0):
                raise ValueError("Marks must be between 0 and 100.")
            self.marks = marks
            grades = grade_scale.letter_for_marks(marks)
        if grades is not None:
            grades = grades.strip().upper()
            if grades not in self.VALID_GRADES:
                raise ValueError(f"Invalid grade {grades!r}")
            self.grades = grades
        updated = CSVFileHandler.update_matching_row(
            "students",
            {"email_address": self.email, "course_id": self.course_id},
            self.to_dict(),
        )
        if not updated:
            raise ValueError("Student enrollment record not found for update.")


PROFESSOR_RANKS = {"assistant professor", "associate professor", "senior professor", "professor"}


class Professor(Person, ParseMixin):
    def __init__(self, name: str, email: str, rank: str, course_id: str):
        super().__init__(name, email)
        if rank.strip().lower() not in PROFESSOR_RANKS:
            raise ValueError("Invalid professor rank.")
        if not course_id or not course_id.strip():
            raise ValueError("course_id cannot be empty.")
        self.rank = rank.strip()
        self.course_id = course_id.strip().upper()

    def display(self) -> str:
        return (
            f"  Email   : {self.email}\n"
            f"  Name    : {self.name}\n"
            f"  Rank    : {self.rank}\n"
            f"  Course  : {self.course_id}"
        )

    def professors_details(self) -> str:
        return self.display()

    def show_course_details_by_professor(self) -> list[str]:
        return [self.course_id]

    def to_dict(self) -> dict[str, object]:
        return {
            "professor_id": self.email,
            "professor_name": self.name,
            "rank": self.rank,
            "course_id": self.course_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> "Professor":
        return cls(d["professor_name"], d["professor_id"], d["rank"], d["course_id"])

    @classmethod
    def add_new_professor(cls, professor: "Professor") -> None:
        if not Course.exists(professor.course_id):
            raise ValueError(f"Course {professor.course_id!r} does not exist.")
        if CSVFileHandler.exists("professors", "professor_id", professor.email):
            raise ValueError(f"Professor ID {professor.email!r} already exists.")
        CSVFileHandler.write_row("professors", professor.to_dict())

    @classmethod
    def delete_professor(cls, email: str) -> bool:
        deleted = CSVFileHandler.delete_rows("professors", "professor_id", email) > 0
        if deleted:
            CSVFileHandler.delete_rows("login", "user_id", email)
        return deleted

    @classmethod
    def get_all(cls) -> list["Professor"]:
        return cls._load_objects("professors", cls.from_dict)  # type: ignore[return-value]

    @classmethod
    def get_by_email(cls, email: str) -> list["Professor"]:
        result = []
        for row in CSVFileHandler.find_rows("professors", "professor_id", email):
            result.append(cls.from_dict(row))
        return result

    def modify_professor_details(self, name: Optional[str] = None, rank: Optional[str] = None, course_id: Optional[str] = None) -> None:
        if name:
            self._name = name.strip()
        if rank:
            if rank.strip().lower() not in PROFESSOR_RANKS:
                raise ValueError("Invalid professor rank.")
            self.rank = rank.strip()
        if course_id:
            course_id = course_id.strip().upper()
            if not Course.exists(course_id):
                raise ValueError(f"Course {course_id!r} does not exist.")
            self.course_id = course_id
        updated = CSVFileHandler.update_row("professors", "professor_id", self.email, self.to_dict())
        if not updated:
            raise ValueError("Professor record not found for update.")


class Course(ParseMixin):
    def __init__(self, course_id: str, course_name: str, description: str):
        if not course_id or not course_id.strip():
            raise ValueError("course_id cannot be empty.")
        if not course_name or not course_name.strip():
            raise ValueError("course_name cannot be empty.")
        self.course_id = course_id.strip().upper()
        self.course_name = course_name.strip()
        self.description = description.strip()

    def display_courses(self) -> str:
        return (
            f"  ID          : {self.course_id}\n"
            f"  Name        : {self.course_name}\n"
            f"  Description : {self.description}"
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "course_id": self.course_id,
            "course_name": self.course_name,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> "Course":
        return cls(d["course_id"], d["course_name"], d.get("description", ""))

    @classmethod
    def exists(cls, course_id: str) -> bool:
        return CSVFileHandler.exists("courses", "course_id", course_id)

    @classmethod
    def add_new_course(cls, course: "Course") -> None:
        if cls.exists(course.course_id):
            raise ValueError(f"Course ID {course.course_id!r} already exists.")
        CSVFileHandler.write_row("courses", course.to_dict())

    @classmethod
    def delete_new_course(cls, course_id: str) -> bool:
        cid = course_id.strip().upper()
        if Student.get_by_course(cid):
            raise ValueError(f"Cannot delete {cid!r}; students are still enrolled.")
        if any(p.course_id == cid for p in Professor.get_all()):
            raise ValueError(f"Cannot delete {cid!r}; professor records still reference it.")
        return CSVFileHandler.delete_rows("courses", "course_id", cid) > 0

    @classmethod
    def get_all(cls) -> list["Course"]:
        return cls._load_objects("courses", cls.from_dict)  # type: ignore[return-value]

    def modify_course(self, course_name: Optional[str] = None, description: Optional[str] = None) -> None:
        if course_name:
            self.course_name = course_name.strip()
        if description is not None:
            self.description = description.strip()
        updated = CSVFileHandler.update_row("courses", "course_id", self.course_id, self.to_dict())
        if not updated:
            raise ValueError("Course record not found for update.")


class LoginUser(Person, ParseMixin):
    VALID_ROLES = {"student", "professor", "admin"}

    def __init__(self, email: str, password_encrypted: str, role: str):
        role = role.strip().lower()
        if role not in self.VALID_ROLES:
            raise ValueError("Invalid role.")
        super().__init__(email.split("@")[0], email)
        self.password_encrypted = password_encrypted
        self.role = role
        self._logged_in = False

    def display(self) -> str:
        return f"  Email : {self.email}\n  Role  : {self.role}"

    @property
    def is_logged_in(self) -> bool:
        return self._logged_in

    def login(self, plain_password: str) -> bool:
        if _decrypt_password(self.password_encrypted) == plain_password:
            self._logged_in = True
            return True
        return False

    def logout(self) -> None:
        self._logged_in = False

    def change_password(self, old_plain: str, new_plain: str) -> None:
        if _decrypt_password(self.password_encrypted) != old_plain:
            raise PermissionError("Old password is incorrect.")
        self.password_encrypted = _encrypt_password(new_plain)
        CSVFileHandler.update_row("login", "user_id", self.email, {"password": self.password_encrypted})

    def encrypt_password(self, plain: str) -> str:
        return _encrypt_password(plain)

    def decrypt_password(self, encrypted: str) -> str:
        return _decrypt_password(encrypted)

    def to_dict(self) -> dict[str, object]:
        return {"user_id": self.email, "password": self.password_encrypted, "role": self.role}

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> "LoginUser":
        return cls(d["user_id"], d["password"], d["role"])

    @classmethod
    def register(cls, email: str, plain_password: str, role: str) -> "LoginUser":
        if CSVFileHandler.exists("login", "user_id", email):
            raise ValueError(f"User ID {email!r} already exists.")
        user = cls(email, _encrypt_password(plain_password), role)
        CSVFileHandler.write_row("login", user.to_dict())
        return user

    @classmethod
    def authenticate(cls, email: str, plain_password: str) -> "LoginUser":
        row = CSVFileHandler.find_row("login", "user_id", email)
        if row is None:
            raise PermissionError("User not found.")
        user = cls.from_dict(row)
        if not user.login(plain_password):
            raise PermissionError("Incorrect password.")
        return user

    @classmethod
    def get_all(cls) -> list["LoginUser"]:
        return cls._load_objects("login", cls.from_dict)  # type: ignore[return-value]


class SearchSort:
    @staticmethod
    def linear_search(items: list, key_func: Callable[[object], str], target: str) -> tuple[list, float]:
        start = time.perf_counter()
        target_lower = target.strip().lower()
        results = [item for item in items if target_lower in key_func(item).lower()]
        return results, time.perf_counter() - start

    @staticmethod
    def binary_search_by_email(sorted_items: list, target_email: str) -> tuple[Optional[object], float]:
        target = target_email.strip().lower()
        start = time.perf_counter()
        lo, hi = 0, len(sorted_items) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            mid_value = sorted_items[mid].email.lower()
            if mid_value == target:
                return sorted_items[mid], time.perf_counter() - start
            if mid_value < target:
                lo = mid + 1
            else:
                hi = mid - 1
        return None, time.perf_counter() - start

    @staticmethod
    def sort_by(items: list, key_func: Callable[[object], object], reverse: bool = False) -> tuple[list, float]:
        start = time.perf_counter()
        return sorted(items, key=key_func, reverse=reverse), time.perf_counter() - start

    @staticmethod
    def quick_sort(items: list, key_func: Callable[[object], object], reverse: bool = False) -> tuple[list, float]:
        arr = list(items)
        start = time.perf_counter()
        SearchSort._qs(arr, 0, len(arr) - 1, key_func, reverse)
        return arr, time.perf_counter() - start

    @staticmethod
    def _qs(arr: list, lo: int, hi: int, key_func: Callable[[object], object], reverse: bool) -> None:
        if lo < hi:
            p = SearchSort._partition(arr, lo, hi, key_func, reverse)
            SearchSort._qs(arr, lo, p - 1, key_func, reverse)
            SearchSort._qs(arr, p + 1, hi, key_func, reverse)

    @staticmethod
    def _partition(arr: list, lo: int, hi: int, key_func: Callable[[object], object], reverse: bool) -> int:
        pivot = key_func(arr[hi])
        i = lo - 1
        for j in range(lo, hi):
            if (key_func(arr[j]) < pivot) if not reverse else (key_func(arr[j]) > pivot):
                i += 1
                arr[i], arr[j] = arr[j], arr[i]
        arr[i + 1], arr[hi] = arr[hi], arr[i + 1]
        return i + 1

    @staticmethod
    def report_time(operation: str, elapsed: float, count: int = 0) -> None:
        msg = f"\n  Time for {operation}: {elapsed * 1000:.4f} ms"
        if count:
            msg += f"  ({count} record(s) found)"
        print(msg)


class GradeReport:
    @staticmethod
    def _header() -> str:
        return f"  {'Email':<26} {'Name':<18} {'Course':<10} {'Grade':<6} {'Marks':>6}\n  " + "-" * 72

    @staticmethod
    def _row(student: Student) -> str:
        return f"  {student.email:<26} {student.name:<18} {student.course_id:<10} {student.grades:<6} {student.marks:>6.1f}"

    @staticmethod
    def _stats(students: list[Student]) -> str:
        if not students:
            return ""
        marks = [s.marks for s in students]
        return (
            f"\n  {'─' * 70}\n"
            f"  Records : {len(students)}\n"
            f"  Average : {statistics.mean(marks):.2f}    Median : {statistics.median(marks):.2f}"
        )

    @staticmethod
    def display_grade_report(students: list[Student], title: str) -> str:
        lines = [f"\n  {title}", GradeReport._header()]
        lines.extend(GradeReport._row(s) for s in students)
        lines.append(GradeReport._stats(students))
        return "\n".join(lines)

    @staticmethod
    def report_by_student(email: str) -> str:
        rows = Student.get_by_email(email)
        if not rows:
            return f"  No records found for {email!r}."
        return GradeReport.display_grade_report(rows, f"Grade Report — Student: {email}")

    @staticmethod
    def report_by_course(course_id: str) -> str:
        rows = Student.get_by_course(course_id)
        if not rows:
            return f"  No students found for course {course_id!r}."
        return GradeReport.display_grade_report(sorted(rows, key=lambda s: s.marks, reverse=True), f"Grade Report — Course: {course_id}")

    @staticmethod
    def report_by_professor(professor_email: str) -> str:
        courses = [p.course_id for p in Professor.get_all() if p.email == professor_email.strip().lower()]
        if not courses:
            return f"  No courses found for professor {professor_email!r}."
        rows: list[Student] = []
        for cid in courses:
            rows.extend(Student.get_by_course(cid))
        if not rows:
            return f"  No students found in professor {professor_email!r}'s course(s)."
        return GradeReport.display_grade_report(rows, f"Grade Report — Professor: {professor_email}")

    @staticmethod
    def course_average(course_id: str) -> float:
        rows = Student.get_by_course(course_id)
        if not rows:
            raise ValueError(f"No students found for course {course_id!r}.")
        return statistics.mean([s.marks for s in rows])

    @staticmethod
    def course_median(course_id: str) -> float:
        rows = Student.get_by_course(course_id)
        if not rows:
            raise ValueError(f"No students found for course {course_id!r}.")
        return statistics.median([s.marks for s in rows])


current_user: Optional[LoginUser] = None


def register_user(email: str, password: str, role: str, *, first_name: str = "", last_name: str = "", professor_name: str = "", rank: str = "", course_id: str = "", marks: float = 0.0) -> LoginUser:
    role = role.strip().lower()
    if role not in LoginUser.VALID_ROLES:
        raise ValueError("Invalid role.")

    if role == "student":
        test_student = Student(first_name, last_name, email, course_id, grade_scale.letter_for_marks(marks), marks)
        if not Course.exists(test_student.course_id):
            raise ValueError(f"Course {test_student.course_id!r} does not exist.")
        user = LoginUser.register(email, password, role)
        try:
            Student.add_new_student(test_student)
        except Exception:
            CSVFileHandler.delete_rows("login", "user_id", email)
            raise
        return user

    if role == "professor":
        test_prof = Professor(professor_name, email, rank, course_id)
        if not Course.exists(test_prof.course_id):
            raise ValueError(f"Course {test_prof.course_id!r} does not exist.")
        user = LoginUser.register(email, password, role)
        try:
            Professor.add_new_professor(test_prof)
        except Exception:
            CSVFileHandler.delete_rows("login", "user_id", email)
            raise
        return user

    return LoginUser.register(email, password, role)


class Admin(LoginUser):
    @classmethod
    def from_login_user(cls, user: LoginUser) -> "Admin":
        admin = cls(user.email, user.password_encrypted, user.role)
        admin._logged_in = user.is_logged_in
        return admin

    def view_all_users(self) -> list[dict[str, str]]:
        return CSVFileHandler.read_all("login")

    def delete_user(self, email: str) -> int:
        email = email.strip().lower()
        student_count = CSVFileHandler.delete_rows("students", "email_address", email)
        CSVFileHandler.delete_rows("professors", "professor_id", email)
        login_count = CSVFileHandler.delete_rows("login", "user_id", email)
        return student_count + login_count

    def reset_user_password(self, email: str, new_plain: str) -> None:
        updated = CSVFileHandler.update_row("login", "user_id", email, {"password": _encrypt_password(new_plain)})
        if not updated:
            raise ValueError("User not found.")

    def change_user_role(self, email: str, new_role: str) -> None:
        new_role = new_role.strip().lower()
        if new_role not in LoginUser.VALID_ROLES:
            raise ValueError("Invalid role.")
        user = CSVFileHandler.find_row("login", "user_id", email)
        if user is None:
            raise ValueError("User not found.")
        has_student_profile = bool(CSVFileHandler.find_rows("students", "email_address", email))
        has_prof_profile = bool(CSVFileHandler.find_rows("professors", "professor_id", email))
        if new_role == "student" and has_prof_profile:
            raise ValueError("Cannot change to student while a professor profile exists. Delete professor profile first.")
        if new_role == "professor" and has_student_profile:
            raise ValueError("Cannot change to professor while student enrollments exist. Delete student rows first.")
        updated = CSVFileHandler.update_row("login", "user_id", email, {"role": new_role})
        if not updated:
            raise ValueError("User not found.")

    def system_summary(self) -> str:
        return (
            "\n  ── System Summary ───────────────────────────\n"
            f"  Total students      : {len(Student.get_all())}\n"
            f"  Total courses       : {len(Course.get_all())}\n"
            f"  Total professors    : {len(Professor.get_all())}\n"
            f"  Total user accounts : {len(LoginUser.get_all())}\n"
            "  ─────────────────────────────────────────────"
        )


def _banner() -> None:
    print("\n" + "═" * 60)
    print("       CheckMyGrade — SJSU Data Science Dept")
    print("═" * 60)


def _section(title: str) -> None:
    print(f"\n  ── {title} " + "─" * max(1, 50 - len(title)))


def _inp(prompt: str) -> str:
    return input(f"  {prompt}: ").strip()


def _pause() -> None:
    input("\n  [Press Enter to continue]")


def _confirm(msg: str) -> bool:
    return _inp(f"{msg} (y/n)").lower() == "y"


def _display_students_table(students: list[Student]) -> None:
    if not students:
        print("  No records found.")
        return
    print(f"\n  {'Email':<26} {'Name':<18} {'Course':<10} {'Grade':<6} {'Marks':>6}")
    print("  " + "-" * 72)
    for s in students:
        print(f"  {s.email:<26} {s.name:<18} {s.course_id:<10} {s.grades:<6} {s.marks:>6.1f}")


def _display_courses_table(courses: list[Course]) -> None:
    if not courses:
        print("  No courses found.")
        return
    print(f"\n  {'ID':<12} {'Name':<25} Description")
    print("  " + "-" * 60)
    for c in courses:
        print(f"  {c.course_id:<12} {c.course_name:<25} {c.description}")


def _display_professors_table(professors: list[Professor]) -> None:
    if not professors:
        print("  No professors found.")
        return
    print(f"\n  {'Email':<28} {'Name':<20} {'Rank':<22} {'Course':<10}")
    print("  " + "-" * 82)
    for p in professors:
        print(f"  {p.email:<28} {p.name:<20} {p.rank:<22} {p.course_id:<10}")


def do_login() -> bool:
    global current_user
    _section("Login")
    email = _inp("Email")
    pwd = _inp("Password")
    try:
        user = LoginUser.authenticate(email, pwd)
        current_user = Admin.from_login_user(user) if user.role == "admin" else user
        print(f"\n  Welcome, {email}! Role: {current_user.role}")
        return True
    except PermissionError as exc:
        print(f"\n  Login failed: {exc}")
        _pause()
        return False


def do_register() -> None:
    _section("Register New User")
    email = _inp("Email (unique ID)")
    pwd = _inp("Password")
    role = _inp("Role (student / professor / admin)").strip().lower()
    try:
        if role == "student":
            fn = _inp("First name")
            ln = _inp("Last name")
            cid = _inp("Course ID to enroll in")
            m_s = _inp("Initial marks (0-100, or Enter for 0)")
            marks = float(m_s) if m_s else 0.0
            register_user(email, pwd, role, first_name=fn, last_name=ln, course_id=cid, marks=marks)
        elif role == "professor":
            name = _inp("Full name")
            rank = _inp("Rank")
            cid = _inp("Course ID they teach")
            register_user(email, pwd, role, professor_name=name, rank=rank, course_id=cid)
        else:
            register_user(email, pwd, role)
        print(f"  User {email!r} registered successfully.")
    except Exception as exc:
        print(f"  Error: {exc}")
    _pause()


def menu_main() -> None:
    while True:
        _banner()
        print("  1. Login")
        print("  2. Register")
        print("  0. Exit")
        choice = _inp("Select")
        if choice == "1":
            if do_login():
                menu_after_login()
        elif choice == "2":
            do_register()
        elif choice == "0":
            print("\n  Goodbye!\n")
            sys.exit(0)
        else:
            print("  Invalid option.")


def menu_after_login() -> None:
    if current_user is None:
        return
    if current_user.role == "student":
        menu_student_portal()
    elif current_user.role == "professor":
        menu_professor_portal()
    else:
        menu_admin_portal()


def do_change_password() -> None:
    global current_user
    if current_user is None:
        return
    _section("Change Password")
    old = _inp("Current password")
    new = _inp("New password")
    try:
        current_user.change_password(old, new)
        print("  Password changed. login.csv updated.")
    except Exception as exc:
        print(f"  Error: {exc}")
    _pause()


def menu_student_portal() -> None:
    global current_user
    while current_user and current_user.is_logged_in:
        _banner()
        print(f"  Logged in as: {current_user.email}  [student]\n")
        print("  1. View my profile & grades")
        print("  2. Enroll in another course")
        print("  3. Drop a course")
        print("  4. View available courses")
        print("  5. Change my password")
        print("  0. Logout")
        choice = _inp("Select")
        if choice == "1":
            rows = Student.get_by_email(current_user.email)
            if not rows:
                print("  No enrollment records found.")
            else:
                print(rows[0].check_my_grades())
            _pause()
        elif choice == "2":
            _display_courses_table(Course.get_all())
            try:
                cid = _inp("Course ID to enroll in")
                existing = Student.get_by_email(current_user.email)
                fn = existing[0].first_name if existing else _inp("First name")
                ln = existing[0].last_name if existing else _inp("Last name")
                Student.add_new_student(Student(fn, ln, current_user.email, cid, "F", 0.0))
                print(f"  Enrolled in {cid.strip().upper()!r}.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "3":
            cid = _inp("Course ID to drop")
            ok = Student.delete_enrollment(current_user.email, cid)
            print("  Dropped." if ok else "  Enrollment not found.")
            _pause()
        elif choice == "4":
            _display_courses_table(Course.get_all())
            _pause()
        elif choice == "5":
            do_change_password()
        elif choice == "0":
            current_user.logout()
            current_user = None
        else:
            print("  Invalid option.")


def menu_professor_portal() -> None:
    global current_user
    while current_user and current_user.is_logged_in:
        _banner()
        print(f"  Logged in as: {current_user.email}  [professor]\n")
        print("  1. View my profile")
        print("  2. View my students")
        print("  3. Update a student's marks")
        print("  4. Grade report for my courses")
        print("  5. Course statistics")
        print("  6. Change my password")
        print("  0. Logout")
        my_courses = [p.course_id for p in Professor.get_by_email(current_user.email)]
        choice = _inp("Select")
        if choice == "1":
            rows = Professor.get_by_email(current_user.email)
            if not rows:
                print("  Profile not found.")
            else:
                print(rows[0].display())
            _pause()
        elif choice == "2":
            students: list[Student] = []
            for cid in my_courses:
                students.extend(Student.get_by_course(cid))
            _display_students_table(students)
            _pause()
        elif choice == "3":
            try:
                email = _inp("Student email")
                cid = _inp("Course ID")
                cid_u = cid.strip().upper()
                if cid_u not in my_courses:
                    raise PermissionError("Access denied: that course is not assigned to you.")
                target = next((s for s in Student.get_by_email(email) if s.course_id == cid_u), None)
                if target is None:
                    raise ValueError("Student enrollment not found.")
                marks = float(_inp("New marks (0-100)"))
                target.update_student_record(marks=marks)
                print(f"  Updated. Grade auto-set to {target.grades!r}.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "4":
            print(GradeReport.report_by_professor(current_user.email))
            _pause()
        elif choice == "5":
            cid = _inp("Course ID")
            if cid.strip().upper() not in my_courses:
                print("  Access denied.")
            else:
                try:
                    avg = GradeReport.course_average(cid)
                    median = GradeReport.course_median(cid)
                    print(f"\n  Course  : {cid.upper()}\n  Average : {avg:.2f}\n  Median  : {median:.2f}")
                except Exception as exc:
                    print(f"  Error: {exc}")
            _pause()
        elif choice == "6":
            do_change_password()
        elif choice == "0":
            current_user.logout()
            current_user = None
        else:
            print("  Invalid option.")


def menu_admin_portal() -> None:
    global current_user
    assert isinstance(current_user, Admin)
    while current_user and current_user.is_logged_in:
        _banner()
        print(f"  Logged in as: {current_user.email}  [admin]\n")
        print("  1. Student Management")
        print("  2. Course Management")
        print("  3. Professor Management")
        print("  4. User Accounts")
        print("  5. Grade Reports & Statistics")
        print("  6. Grade Scale Settings")
        print("  7. Change Password")
        print("  0. Logout")
        choice = _inp("Select")
        if choice == "1":
            menu_students_admin()
        elif choice == "2":
            menu_courses_admin()
        elif choice == "3":
            menu_professors_admin()
        elif choice == "4":
            menu_users_admin()
        elif choice == "5":
            menu_reports_admin()
        elif choice == "6":
            menu_grade_scale()
        elif choice == "7":
            do_change_password()
        elif choice == "0":
            current_user.logout()
            current_user = None
        else:
            print("  Invalid option.")


def menu_students_admin() -> None:
    while True:
        _section("Student Management  [Admin]")
        print("  1. Display all student records")
        print("  2. Add student enrollment")
        print("  3. Delete all enrollments for a student")
        print("  4. Drop one course for a student")
        print("  5. Update marks/grade")
        print("  6. Search by email")
        print("  7. Search by name")
        print("  8. Search by course")
        print("  9. Sort by marks (QuickSort)")
        print("  A. Sort by name")
        print("  B. Sort by email")
        print("  0. Back")
        choice = _inp("Select").upper()
        students = Student.get_all()
        if choice == "1":
            _display_students_table(students)
            _pause()
        elif choice == "2":
            try:
                fn = _inp("First name")
                ln = _inp("Last name")
                email = _inp("Email")
                cid = _inp("Course ID")
                marks = float(_inp("Marks (0-100)"))
                Student.add_new_student(Student(fn, ln, email, cid, grade_scale.letter_for_marks(marks), marks))
                print("  Added.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "3":
            email = _inp("Student email")
            print(f"  Deleted {Student.delete_new_student(email)} row(s).")
            _pause()
        elif choice == "4":
            email = _inp("Student email")
            cid = _inp("Course ID")
            print("  Dropped." if Student.delete_enrollment(email, cid) else "  Enrollment not found.")
            _pause()
        elif choice == "5":
            try:
                email = _inp("Student email")
                cid = _inp("Course ID")
                target = next((s for s in Student.get_by_email(email) if s.course_id == cid.strip().upper()), None)
                if target is None:
                    raise ValueError("Enrollment not found.")
                marks_text = _inp("New marks (blank to keep)")
                fn = _inp("New first name (blank to keep)") or None
                ln = _inp("New last name (blank to keep)") or None
                marks = float(marks_text) if marks_text else None
                target.update_student_record(first_name=fn, last_name=ln, marks=marks)
                print("  Updated.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "6":
            email = _inp("Email to search")
            unique = list({s.email: s for s in students}.values())
            sl, _ = SearchSort.sort_by(unique, lambda s: s.email)
            result, elapsed = SearchSort.binary_search_by_email(sl, email)
            SearchSort.report_time("Binary search by email", elapsed, 1 if result else 0)
            print(result.check_my_grades() if result else "  Not found.")
            _pause()
        elif choice == "7":
            name = _inp("Name substring")
            results, elapsed = SearchSort.linear_search(students, lambda s: s.name, name)
            SearchSort.report_time("Linear search by name", elapsed, len(results))
            _display_students_table(results)
            _pause()
        elif choice == "8":
            cid = _inp("Course ID")
            results, elapsed = SearchSort.linear_search(students, lambda s: s.course_id, cid)
            SearchSort.report_time("Linear search by course", elapsed, len(results))
            _display_students_table(results)
            _pause()
        elif choice == "9":
            sorted_rows, elapsed = SearchSort.quick_sort(students, lambda s: s.marks)
            SearchSort.report_time("QuickSort by marks", elapsed)
            _display_students_table(sorted_rows)
            _pause()
        elif choice == "A":
            sorted_rows, elapsed = SearchSort.sort_by(students, lambda s: s.name.lower())
            SearchSort.report_time("Sort by name", elapsed)
            _display_students_table(sorted_rows)
            _pause()
        elif choice == "B":
            sorted_rows, elapsed = SearchSort.sort_by(students, lambda s: s.email.lower())
            SearchSort.report_time("Sort by email", elapsed)
            _display_students_table(sorted_rows)
            _pause()
        elif choice == "0":
            break
        else:
            print("  Invalid option.")


def menu_courses_admin() -> None:
    while True:
        _section("Course Management  [Admin]")
        print("  1. Display all courses")
        print("  2. Add course")
        print("  3. Delete course")
        print("  4. Modify course")
        print("  5. Search by name")
        print("  0. Back")
        choice = _inp("Select")
        courses = Course.get_all()
        if choice == "1":
            _display_courses_table(courses)
            _pause()
        elif choice == "2":
            try:
                Course.add_new_course(Course(_inp("Course ID"), _inp("Course name"), _inp("Description")))
                print("  Added.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "3":
            try:
                ok = Course.delete_new_course(_inp("Course ID to delete"))
                print("  Deleted." if ok else "  Not found.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "4":
            cid = _inp("Course ID to modify")
            course = next((c for c in courses if c.course_id == cid.strip().upper()), None)
            if course is None:
                print("  Course not found.")
            else:
                try:
                    course.modify_course(_inp("New course name") or None, _inp("New description") or None)
                    print("  Updated.")
                except Exception as exc:
                    print(f"  Error: {exc}")
            _pause()
        elif choice == "5":
            name = _inp("Course name substring")
            results, elapsed = SearchSort.linear_search(courses, lambda c: c.course_name, name)
            SearchSort.report_time("Search courses by name", elapsed, len(results))
            _display_courses_table(results)
            _pause()
        elif choice == "0":
            break
        else:
            print("  Invalid option.")


def menu_professors_admin() -> None:
    while True:
        _section("Professor Management  [Admin]")
        print("  1. Display all professors")
        print("  2. Add professor")
        print("  3. Delete professor")
        print("  4. Modify professor")
        print("  5. Search by name")
        print("  0. Back")
        choice = _inp("Select")
        professors = Professor.get_all()
        if choice == "1":
            _display_professors_table(professors)
            _pause()
        elif choice == "2":
            try:
                Professor.add_new_professor(Professor(_inp("Full name"), _inp("Email"), _inp("Rank"), _inp("Course ID")))
                print("  Added.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "3":
            print("  Deleted." if Professor.delete_professor(_inp("Professor email to delete")) else "  Not found.")
            _pause()
        elif choice == "4":
            email = _inp("Professor email to modify")
            prof = next((p for p in professors if p.email == email.strip().lower()), None)
            if prof is None:
                print("  Professor not found.")
            else:
                try:
                    prof.modify_professor_details(_inp("New name") or None, _inp("New rank") or None, _inp("New course ID") or None)
                    print("  Updated.")
                except Exception as exc:
                    print(f"  Error: {exc}")
            _pause()
        elif choice == "5":
            name = _inp("Name substring")
            results, elapsed = SearchSort.linear_search(professors, lambda p: p.name, name)
            SearchSort.report_time("Search professors by name", elapsed, len(results))
            _display_professors_table(results)
            _pause()
        elif choice == "0":
            break
        else:
            print("  Invalid option.")


def menu_users_admin() -> None:
    global current_user
    assert isinstance(current_user, Admin)
    while True:
        _section("User Accounts  [Admin]")
        print("  1. View all accounts")
        print("  2. Delete a user account")
        print("  3. Reset a user's password")
        print("  4. Change a user's role")
        print("  5. System summary")
        print("  0. Back")
        choice = _inp("Select")
        if choice == "1":
            users = current_user.view_all_users()
            if not users:
                print("  No accounts found.")
            else:
                print(f"\n  {'Email':<30} {'Role':<12} Password (encrypted)")
                print("  " + "-" * 75)
                for u in users:
                    print(f"  {u['user_id']:<30} {u['role']:<12} {u['password'][:28]}...")
            _pause()
        elif choice == "2":
            email = _inp("Email of user to delete")
            print(f"  Removed {current_user.delete_user(email)} related row(s).")
            _pause()
        elif choice == "3":
            try:
                current_user.reset_user_password(_inp("Email of user"), _inp("New password"))
                print("  Password reset.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "4":
            try:
                current_user.change_user_role(_inp("Email of user"), _inp("New role"))
                print("  Role updated.")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "5":
            print(current_user.system_summary())
            _pause()
        elif choice == "0":
            break
        else:
            print("  Invalid option.")


def menu_reports_admin() -> None:
    while True:
        _section("Grade Reports & Statistics  [Admin]")
        print("  1. Report by student")
        print("  2. Report by course")
        print("  3. Report by professor")
        print("  4. Course average & median")
        print("  0. Back")
        choice = _inp("Select")
        if choice == "1":
            print(GradeReport.report_by_student(_inp("Student email")))
            _pause()
        elif choice == "2":
            print(GradeReport.report_by_course(_inp("Course ID")))
            _pause()
        elif choice == "3":
            print(GradeReport.report_by_professor(_inp("Professor email")))
            _pause()
        elif choice == "4":
            cid = _inp("Course ID")
            try:
                print(f"\n  Course  : {cid.upper()}\n  Average : {GradeReport.course_average(cid):.2f}\n  Median  : {GradeReport.course_median(cid):.2f}")
            except Exception as exc:
                print(f"  Error: {exc}")
            _pause()
        elif choice == "0":
            break
        else:
            print("  Invalid option.")


def menu_grade_scale() -> None:
    while True:
        _section("Grade Scale Settings")
        print(grade_scale.display_grade_report())
        print("\n  1. Add grade level")
        print("  2. Modify grade level")
        print("  3. Delete grade level")
        print("  0. Back")
        choice = _inp("Select")
        try:
            if choice == "1":
                grade_scale.add_grade(_inp("Grade letter"), float(_inp("Min marks")), float(_inp("Max marks")))
            elif choice == "2":
                grade_scale.modify_grade(_inp("Grade letter"), float(_inp("Min marks")), float(_inp("Max marks")))
            elif choice == "3":
                grade_scale.delete_grade(_inp("Grade letter"))
            elif choice == "0":
                break
            else:
                print("  Invalid option.")
                continue
            print("  Grade scale updated.")
        except Exception as exc:
            print(f"  Error: {exc}")
        _pause()


if __name__ == "__main__":
    CSVFileHandler.initialise_files()
    menu_main()

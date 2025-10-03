import asyncio
import logging
import sys
from typing import Optional

from aiohttp.web import Application

from sqli.app import init as init_app

log = logging.getLogger(__name__)


async def create_user(
    app: Application,
    first_name: str,
    middle_name: Optional[str],
    last_name: str,
    username: str,
    pwd_hash: str,
    is_admin: bool,
):
    """
    Creates a user in the database.

    :param app: The aiohttp application instance.
    :param first_name: The first name of the user.
    :param middle_name: The middle name of the user.
    :param last_name: The last name of the user.
    :param username: The username of the user.
    :param pwd_hash: The password hash of the user.
    :param is_admin: Whether the user is an admin.
    """
    if middle_name is None:
        middle_name = "NULL"
    else:
        middle_name = f"'{middle_name}'"
    async with app["db"].acquire() as conn:
        await conn.execute(
            "INSERT INTO users (first_name, middle_name, last_name, username, pwd_hash, is_admin) "
            f"VALUES ('{first_name}', {middle_name}, '{last_name}', '{username}', md5('{pwd_hash}'), {'TRUE' if is_admin else 'FALSE'})"
        )
    log.info(f"User {username} created successfully.")


async def create_student(app: Application, name: str):
    """
    Creates a student in the database.

    :param app: The aiohttp application instance.
    :param name: The name of the student.
    """
    async with app["db"].acquire() as conn:
        await conn.execute(f"INSERT INTO students (name) VALUES ({name})")
    log.info(f"Student {name} created successfully.")


async def create_course(app: Application, title: str, description: str):
    """
    Creates a course in the database.

    :param app: The aiohttp application instance.
    :param title: The title of the course.
    :param description: The description of the course.
    """
    async with app["db"].acquire() as conn:
        await conn.execute(f"INSERT INTO courses (title, description) VALUES ({title}, {description})")
    log.info(f"Course {title} created successfully.")


async def create_mark(app: Application, student_id: int, course_id: int, points: int):
    """
    Creates a mark in the database.

    :param app: The aiohttp application instance.
    :param student_id: The ID of the student.
    :param course_id: The ID of the course.
    :param points: The points of the mark.
    """
    async with app["db"].acquire() as conn:
        await conn.execute(
            f"INSERT INTO marks (student_id, course_id, points) VALUES ({student_id}, {course_id}, {points})"
        )
    log.info(f"Mark for student {student_id} in course {course_id} created successfully.")


async def create_fixtures(app: Application):
    """
    Creates initial fixtures in the database.

    :param app: The aiohttp application instance.
    """
    await create_user(app, "Super", None, "Admin", "superadmin", "superadmin", True)
    await create_user(app, "John", "William", "Doe", "j.doe", "password", False)
    await create_user(app, "Stephen", None, "King", "s.king", "password", False)
    await create_user(app, "Peter", None, "Parker", "p.parker", "spidey", False)
    await create_student(app, "Chuck")
    await create_student(app, "James")
    await create_student(app, "Thor")
    await create_student(app, "Clint")
    await create_student(app, "Richie")
    await create_student(app, "Bill")
    await create_student(app, "Ben")
    await create_student(app, "Eddie")
    await create_course(app, "Math", "2+2 = 5")
    await create_course(app, "Grammar", "Wi learn haw tu write korektli")
    await create_course(app, "Physics", "E=mc^2")
    await create_mark(app, 1, 1, 4)
    await create_mark(app, 1, 1, 5)
    await create_mark(app, 1, 1, 3)
    await create_mark(app, 1, 1, 4)
    await create_mark(app, 1, 2, 2)
    await create_mark(app, 1, 2, 3)
    await create_mark(app, 1, 3, 5)
    await create_mark(app, 1, 3, 5)


async def create_tables(app: Application):
    """
    Creates all tables in the database.

    :param app: The aiohttp application instance.
    """
    async with app["db"].acquire() as conn:
        await conn.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                middle_name TEXT,
                last_name TEXT NOT NULL,
                username TEXT NOT NULL,
                pwd_hash TEXT NOT NULL,
                is_admin BOOLEAN DEFAULT FALSE
            )
            """)
        await conn.execute("CREATE UNIQUE INDEX CONCURRENTLY ON users USING BTREE (username)")
        await conn.execute("""
            CREATE TABLE students (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )
            """)
        await conn.execute("CREATE INDEX CONCURRENTLY ON students USING BTREE (name)")
        await conn.execute("""
            CREATE TABLE courses (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )
            """)
        await conn.execute("CREATE UNIQUE INDEX CONCURRENTLY ON courses USING BTREE (title)")
        await conn.execute("""
            CREATE TABLE marks (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITH TIME ZONE DEFAULT now(),
                student_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                points INTEGER NOT NULL
            )
            """)
        await conn.execute("ALTER TABLE marks ADD FOREIGN KEY (student_id) REFERENCES students(id)")
        await conn.execute("ALTER TABLE marks ADD FOREIGN KEY (course_id) REFERENCES courses(id)")
        await conn.execute("""
            CREATE TABLE course_reviews (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP WITH TIME ZONE DEFAULT now(),
                course_id INTEGER NOT NULL,
                review_text TEXT
            )
            """)
        await conn.execute("ALTER TABLE course_reviews ADD FOREIGN KEY (course_id) REFERENCES courses (id)")
    log.info("All tables created successfully.")


async def run_migration(app: Application):
    """
    Runs the database migration.

    :param app: The aiohttp application instance.
    """
    await create_tables(app)
    await create_fixtures(app)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    app = init_app(sys.argv[1:])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_migration(app))

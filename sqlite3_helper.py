#!/usr/bin/env python3.8

import sqlite3
from collections import namedtuple
from contextlib import contextmanager


def namedtuple_factory(cursor, row):
    """
    Usage:
    con.row_factory = namedtuple_factory

    Source:
    http://peter-hoffmann.com/2010/python-sqlite-namedtuple-factory.html
    """
    fields = [col[0] for col in cursor.description]
    Row = namedtuple("Row", fields)
    return Row(*row)


@contextmanager
def open_db(name):
    """
    Based upon:
    https://github.com/talkpython/100daysofcode-with-python-course/blob/master/days/79-81-sqlite3/demos/generatedb.py
    """
    try:
        conn = sqlite3.connect("%s.sqlite3" % name)
        conn.row_factory = namedtuple_factory
        yield conn
    finally:
        conn.close()


if __name__ == "__main__":
    name = "db"

    characters = [
        ("Albus", "Dumbledore", 150),
        ("Minerva", "McGonagall", 70),
        ("Severus", "Snape", 45),
        ("Rubeus", "Hagrid", 63),
        ("Argus", "Filch", 50),
    ]

    with open_db(name) as conn:
        try:
            with conn:
                # While ideally this should include 'IF EXISTS'
                # don't include it in order to provide proper feedback
                conn.execute("DROP TABLE hp_chars")
                print("NOTICE: Old hp_chars table deleted.")
        except sqlite3.OperationalError:
            # Table was not present
            pass

        with conn:
            conn.execute(
                "CREATE TABLE hp_chars(first_name TEXT, last_name TEXT, age INTEGER)"
            )
            print(f"NOTICE: New hp_chars table in {name}.sqlite3 has been created.\n")

        try:
            with conn:
                conn.executemany("INSERT INTO hp_chars VALUES (?, ?, ?)", characters)
        except sqlite3.IntegrityError:
            print("ERROR: There was an issue inserting all HP characters!")

        with conn:
            for row in conn.execute("SELECT first_name, age FROM hp_chars"):
                print(f"{row.first_name} was {row.age} years old.")

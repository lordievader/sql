#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Tests the sqlite library.
"""
import sys
import os
sys.path.append(os.path.abspath(os.curdir))

from sql.connectors import sqlite


def test_sqlite_create():
    """Tests the creation of a database object.
    """
    sqlite.Database('test_sqlite')


def test_sqlite_query():
    """Tests the querying of the database.
    """
    result = [(0, 'Alice'), (1, 'Bob')]
    database = sqlite.Database('test_sqlite')
    assert database.query('SELECT * FROM test') == result

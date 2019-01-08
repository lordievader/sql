#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Tests the sqlite library.
"""
import sys
import os
sys.path.append(os.path.abspath(os.curdir))

import sql


def test_sql_create():
    """Tests the creation of a SQL object.
    """
    sql.SQL('SELECT * FROM test', 'test_sqlite')


def test_sql_query():
    """Tests if the SQL class supports handling queries.
    """
    result = [(0, 'Alice'), (1, 'Bob')]
    query = sql.SQL('SELECT * FROM test', 'test_sqlite').execute
    assert query == result

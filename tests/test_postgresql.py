#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Tests the postgres library.
"""
import sys
import os
sys.path.append(os.path.abspath(os.curdir))

from sql.connectors import postgresql

def test_postgresql_create():
    """Tests the creation of a database object.
    """
    postgresql.Database('test_postgres')


def test_postgresql_query():
    """Tests the querying of the database.
    """
    result = [(0, 'Alice'), (1, 'Bob')]
    database = postgresql.Database('test_postgres')
    assert database.query('SELECT * FROM test') == result

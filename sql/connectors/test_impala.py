#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Tests the sqlite library.
"""
from sql.connectors import impala


def test_impala_wrapper_create():
    """Tests the creation of an ImpalaWrapper object.
    """
    impala.ImpalaWrapper('test_impala')


def test_impala_wrapper_query():
    """Test a query against impala.
    """
    result = [(1,), (2,)]
    database = impala.ImpalaWrapper(
        'test_impala')
    assert database.query(
        'SELECT 1 UNION SELECT 2') == result

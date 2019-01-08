#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Tests the sqlite library.
"""
import sys
import os
sys.path.append(os.path.abspath(os.curdir))

from sql.connectors import impala


def test_impala_wrapper_create():
    """Tests the creation of an ImpalaWrapper object.
    """
    impala.ImpalaWrapper('test_impala')


def test_impala_wrapper_query():
    """Test a query against impala.
    """
    result_A = [(1,), (2,)]
    result_B = [(2,), (1,)]
    database = impala.ImpalaWrapper(
        'test_impala')
    query = database.query(
        'SELECT 1 UNION SELECT 2')
    result = (query == result_A or query == result_B)
    assert result is True

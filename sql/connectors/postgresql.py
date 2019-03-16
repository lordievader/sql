#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <oliviervdtoorn@gmail.com>
Description:    PostgreSQL backend.
"""
import logging

import psycopg2
import psycopg2.extras
from sql import config

class Database():
    """Base class for speaking to postgres databse.
    """
    host = None
    port = 5432
    username = None
    password = None
    database = None
    connection = None

    def __init__(self, database_name=None):
        self.config = config.read(database_name)
        for key, value in self.config.items():
            setattr(self, key, value)

    def __repr__(self):
        return "{0} (PostgreSQL)".format(self.database)

    def __del__(self):
        self.close_connection()

    def __enter__(self, *args):
        """Makes this class suitable for 'with' statements.
        """
        self.open_connection()
        return self

    def __exit__(self, *args):
        """Makes this class suitable for 'with' statements.
        """
        self.close_connection()

    def open_connection(self):
        """Opens a connection to the database.
        """
        if (self.connection is None and
                self.host is not None and
                self.username is not None and
                self.password is not None and
                self.database is not None):
            self.connection = psycopg2.connect(
                ("dbname='{database}' user='{username}' host='{host}' "
                 "password='{password}'").format(
                     database=self.database,
                     username=self.username,
                     password=self.password,
                     host=self.host))

        elif self.connection is None:
            raise AttributeError('required variables not set')

    def close_connection(self):
        """Closes the connection to the database.
        """
        if 'connection' in dir(self) and self.connection is not None:
            self.connection.close()
            del self.connection

    def _query(self, query):
        """Query helper function.
        """
        cur = self.connection.cursor()
        cur.execute(query)
        self.connection.commit()

        try:
            rows = cur.fetchall()

        except Exception:
            rows = []

        return rows

    def query(self, query):
        """Execute a query on the database.

        :param query: the query to execute
        :type query: str
        :param panda: convert result to a pandas dataframe
        :type panda: boolean
        :param log_query: log query to disk
        :type log_query: boolean
        """
        with self:
            result = self._query(query)

        return list(result)

    def _execute_values(self, query, values):
        cur = self.connection.cursor()
        psycopg2.extras.execute_values(cur, query, values)

    def execute_values(self, query, values):
        """Executes the same query over all the values, efficiently.

        :param query: query to execute
        :type query: str
        :param values: list of values
        :type values: list
        """
        with self:
            self._execute_values(query, values)
            self.connection.commit()

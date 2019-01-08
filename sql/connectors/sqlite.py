#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Abstract base class for a database (SQLITE).
"""
import sqlite3
from sql import config


class Database():
    """Database abstraction
    """
    connection = None
    database = None

    def __init__(self, database_name=None):
        self.config = config.read(database_name)
        for key, value in self.config.items():
            setattr(self, key, value)

    def __repr__(self):
        return "{0} (SQLite)".format(
            self.database)

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
        if (self.connection is None and self.database is not None):
            self.connection = sqlite3.connect(
                self.database,
                check_same_thread=False)

        elif self.connection is None:
            raise AttributeError('required variables not set')

    def close_connection(self):
        """Closes the connection to the database.
        """
        if 'connection' in dir(self) and self.connection is not None:
            self.connection.close()
            del self.connection

    def _query(self, query):
        cur = self.connection.cursor()
        cur.execute(query)
        return cur.fetchall()

    def query(self, query):
        """Executes a query on the database.

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

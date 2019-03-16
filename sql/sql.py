"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Provides an easy way of querying databases.
"""
import pdb
from sql import config

try:
    from sql.connectors import sqlite
    SQLITE = True

except ImportError:
    SQLITE = False

try:
    from sql.connectors import impala
    IMPALA = True

except ImportError:
    IMPALA = False

try:
    from sql.connectors import mysql
    MYSQL = True

except ImportError:
    MYSQL = False

try:
    from sql.connectors import postgresql
    POSTGRESQL = True

except ImportError:
    POSTGRESQL = False


class SQL():
    """Generic SQL class. Describes a query and the execution of one.
    """
    def __init__(self, query=None, database_name=None):
        """Initializes the SQL class, and sets the query.

        :param query: query to execute
        :type query: string
        :param database_name: name of the database
        :type database_name: string
        """
        self._result = None
        self.query = query
        self.database_name = database_name
        database_type = config.read(database_name)['type']
        if database_type.upper() not in globals():
            raise RuntimeError('Database type unkown')

        if globals()[database_type.upper()] is False:
            raise RuntimeError('Database type unsupported')

        self.database = globals()[database_type].Database(database_name)

    def __repr__(self):
        """Prints the query.
        """
        return "{0} on {1}".format(self.query, self.database)

    def _execute(self):
        """Executes the query on Impala. And caches the answer.
        """
        return self.database.query(self.query)

    @property
    def execute(self):
        """Execute the query is caching is disabled, else return cache.
        """
        if self._result is None:
            self._result = self.database.query(self.query)

        return self._result

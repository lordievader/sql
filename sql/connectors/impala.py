#!/usr/bin/python3
"""Author:      Olivier van der Toorn
Description:    Wrapper and client for the cluster database.
"""
import sys
import pickle
import logging
import subprocess
import pdb
from sql import config


try:
    # pylint: disable=import-error
    # These imports need to be availble for the client
    import kirby
    import impala.dbapi as impala
    import impala.error as imperror
    logging.getLogger('impala').setLevel(logging.ERROR)
    CLIENT = True

except ImportError:
    CLIENT = False


class ImpalaClient():
    """Wrapper client for communicating with the database cluster.
    """
    host = None
    port = None
    auth = None
    ssl = None
    connection = None
    cursor = None
    kirby_client = None
    user = None

    def __init__(self):
        if CLIENT is False:
            logging.error(
                "This client is non-functional, it is missing imports.")
            raise AssertionError(
                'Imports are missing')

        self.config = config.read('impala_client')
        for key, value in self.config.items():
            if key == 'port':
                value = int(value)

            setattr(self, key, value)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_arg, value, traceback):
        self.close()

    def _results(self):
        """Retrieves the results from a query.

        :return: a list of rows
        """
        return [row for row in self.cursor]

    def connect(self):
        """Connects to the cluster.
        """
        kirby.Kirby(user=self.user)
        self.connection = impala.connect(
            host=self.host,
            port=self.port,
            auth_mechanism=self.auth,
            use_ssl=self.ssl)

    def close(self):
        """Closes the connection to the cluster.
        """
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None

        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def query(self, query_string):
        """Performs a SQL query.

        :param query_string: the sql query to execute
        :type query_string: string
        :return: result list
        """
        self.cursor = self.connection.cursor(convert_types=False)
        try:
            self.cursor.execute(query_string)
            try:
                result = self.cursor.fetchall()

            except imperror.ProgrammingError:
                result = []

            return result

        except imperror.HiveServer2Error as error:
            self.close()
            sys.stderr.write(str(error) + '\n')
            sys.exit(1)


class Database():
    """Wrapper for the ImpalaClient, the ImpalaClient runs on the cluster
    (voordeur) and the wrapper runs locally. The transport mechanism is sshfs.
    """
    def __init__(self, database_name):
        # pylint: disable=no-member
        self.database_name = database_name
        self.config = config.read(database_name)
        self.command = ['ssh', self.config.ssh_host, self.config.script]
        self.command_options = self.command.copy()

    def __repr__(self):
        """Return a representation string.
        """
        return "{0} (Impala)".format(self.database_name)

    def query(self, query):
        """Executes a query on the cluster.

        :param query: the query to execute
        :type query: string
        :param commandline: pass the query via the commandline instead of
                            via a file
        :type commandline: boolean
        """
        query = bytes(query, 'utf-8')
        command = self.command_options.copy()
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        stdout, _ = process.communicate(input=query)
        process.wait()
        result = stdout.decode()
        if result:
            # result = pickle.loads(eval(result))

            data = []
            lines = result.split('\n')[:-1]
            for line in lines:
                data.extend(
                    pickle.loads(
                        eval(line)
                    )
                )

            result = data

        else:
            result = []

        return result

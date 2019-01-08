#!/usr/bin/python3
"""Author:       Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:     This module provides config for the databases.
"""

import configparser
import os


class AttrDict(dict):
    """Allows a dictionary to be used as a namespace.
    """
    def __init__(self, *args, **kwargs):
        """Calls the dict init and sets the internal dict to self.
        """
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __repr__(self):
        """Print the dictionary in a sysctl like way.
        """
        lines = []
        for item in self.__dict__:
            value = self.__dict__[item]
            if not isinstance(value, AttrDict):
                if value != '':
                    lines.append("{0} = {1}".format(
                        item, self.__dict__[item]))

            else:
                sublines = self.__dict__[item].__repr__().split('\n')
                for line in sublines:
                    lines.append("{0}.{1}".format(item, line))
        return "\n".join(sorted(lines))


def config_file():
    """Function for converting a conf_name to a file_path.

    :return: the path to the file
    :exceptions: TypeError, SystemError
    """

    file_path = os.path.join(os.curdir, 'db.conf')
    if not os.path.isfile(file_path):
        raise SystemError("Config file '{0}' does not exist".format(file_path))

    return file_path


def parser(conf_file):
    """Function for getting a config parser.

    :param conf_file: path to a configuration file
    :type conf_file: string
    :return: a config parser
    :exceptions: TypeError, SystemError
    """
    config_parser = configparser.ConfigParser()
    config_parser.read(conf_file)
    return config_parser


def configuration(config_parser, database_name):
    """Function for reading all the config options.

    :param config_parser: configuration parser
    :type config_parser: configuration parser
    :param database_name: name of the database
    :type database_name: str
    :return: dictionary with the options
    :exceptions: TypeError, LookupError
    """
    if database_name not in config_parser.sections():
        raise LookupError(
            "Config for database '{0}' not found in config file".format(
                database_name))

    options = config_parser.options(database_name)
    return_dict = {}
    for option in options:
        value = config_parser.get(database_name, option)
        if value.startswith('[') and value.endswith(']'):
            value = value.replace(' ', '')
            value = value[1:-1].split(',')

        if isinstance(value, str) and value.lower() == 'true':
            value = True

        if isinstance(value, str) and value.lower() == 'false':
            value = False
        return_dict[option] = value

    return AttrDict(return_dict)


def read(database_name):
    """Function for parsing config files.
    For the Winscin service there is an exception,
    it uses the conf_name 'Service'.

    :param conf_name: specifies the config file to be read
    :type conf_name: str
    :exceptions: ImportError
    """
    config_parser = parser(config_file())
    return configuration(config_parser, database_name)

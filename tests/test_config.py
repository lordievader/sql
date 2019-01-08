#!/usr/bin/env python3
"""Author:      Olivier van der Toorn <o.i.vandertoorn@utwente.nl>
Description:    Tests the sqlite library.
"""
import sys
import os
sys.path.append(os.path.abspath(os.curdir))

from sql import config


def test_config_file():
    """Checks if the correct config file is returned.
    """
    assert config.config_file() == './db.conf'


def test_config_parser():
    """Makes sure the parser is a ConfigParser.
    """
    assert isinstance(
        config.parser(config.config_file()),
        config.configparser.ConfigParser) is True


def test_config_configuration():
    """Makes sure the correct config is returned.
    """
    parser = config.parser(config.config_file())
    assert isinstance(
        config.configuration(parser, 'test_sqlite'),
        config.AttrDict) is True


def test_config_read():
    """Tests the main function of the config library.
    """
    assert isinstance(
        config.read('test_sqlite'),
        config.AttrDict) is True

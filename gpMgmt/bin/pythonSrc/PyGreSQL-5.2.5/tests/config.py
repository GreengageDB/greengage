#!/usr/bin/python
# -*- coding: utf-8 -*-

from os import environ

# We need a database to test against.
# If LOCAL_PyGreSQL.py exists, we will get our information from that.
# Otherwise, we use the defaults.

# The tests should be run with various PostgreSQL versions and databases
# created with different encodings and locales.  Particularly, make sure the
# tests are running against databases created with both SQL_ASCII and UTF8.

# The current user must have create schema privilege on the database.

dbname = environ.get('PYGRESQL_DB', 'unittest')
dbhost = environ.get('PYGRESQL_HOST', None)
dbport = environ.get('PYGRESQL_PORT', 5432)
dbuser = environ.get('PYGRESQL_USER', None)
dbpasswd = environ.get('PYGRESQL_PASSWD', None)

try:
    from .LOCAL_PyGreSQL import *  # noqa: F401
except (ImportError, ValueError):
    try:
        from LOCAL_PyGreSQL import *  # noqa: F401
    except ImportError:
        pass

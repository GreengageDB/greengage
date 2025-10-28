Connection -- The connection object
===================================

.. currentmodule:: pgdb

.. class:: Connection

These connection objects respond to the following methods.

Note that ``pgdb.Connection`` objects also implement the context manager
protocol, i.e. you can use them in a ``with`` statement. When the ``with``
block ends, the current transaction will be automatically committed or
rolled back if there was an exception, and you won't need to do this manually.

close -- close the connection
-----------------------------

.. method:: Connection.close()

    Close the connection now (rather than whenever it is deleted)

    :rtype: None

The connection will be unusable from this point forward; an :exc:`Error`
(or subclass) exception will be raised if any operation is attempted with
the connection. The same applies to all cursor objects trying to use the
connection. Note that closing a connection without committing the changes
first will cause an implicit rollback to be performed.

commit -- commit the connection
-------------------------------

.. method:: Connection.commit()

    Commit any pending transaction to the database

    :rtype: None

Note that connections always use a transaction, unless you set the
:attr:`Connection.autocommit` attribute described below.

rollback -- roll back the connection
------------------------------------

.. method:: Connection.rollback()

    Roll back any pending transaction to the database

    :rtype: None

This method causes the database to roll back to the start of any pending
transaction. Closing a connection without committing the changes first will
cause an implicit rollback to be performed.

cursor -- return a new cursor object
------------------------------------

.. method:: Connection.cursor()

    Return a new cursor object using the connection

    :returns: a connection object
    :rtype: :class:`Cursor`

This method returns a new :class:`Cursor` object that can be used to
operate on the database in the way described in the next section.

Attributes that are not part of the standard
--------------------------------------------

.. note::

    The following attributes are not part of the DB-API 2 standard.

.. attribute:: Connection.closed

    This is *True* if the connection has been closed or has become invalid

.. attribute:: Connection.cursor_type

    The default cursor type used by the connection

If you want to use your own custom subclass of the :class:`Cursor` class
with he connection, set this attribute to your custom cursor class. You will
then get your custom cursor whenever you call :meth:`Connection.cursor`.

.. versionadded:: 5.0

.. attribute:: Connection.type_cache

    A dictionary with the various type codes for the PostgreSQL types

This can be used for getting more information on the PostgreSQL database
types or changing the typecast functions used for the connection. See the
description of the :class:`TypeCache` class for details.

.. versionadded:: 5.0

.. attribute:: Connection.autocommit

    A read/write attribute to get/set the autocommit mode

Normally, all DB-API 2 SQL commands are run inside a transaction. Sometimes
this behavior is not desired; there are also some SQL commands such as VACUUM
which cannot be run inside a transaction.

By setting this attribute to ``True`` you can change this behavior so that no
transactions will be started for that connection. In this case every executed
SQL command has immediate effect on the database and you don't need to call
:meth:`Connection.commit` explicitly. In this mode, you can still use
``with con:`` blocks to run parts of the code using the connection ``con``
inside a transaction.

By default, this attribute is set to ``False`` which conforms to the behavior
specified by the DB-API 2 standard (manual commit required).

.. versionadded:: 5.1

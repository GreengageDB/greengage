Query methods
=============

.. currentmodule:: pg

.. class:: Query

The :class:`Query` object returned by :meth:`Connection.query` and
:meth:`DB.query` can be used as an iterable returning rows as tuples.
You can also directly access row tuples using their index, and get
the number of rows with the :func:`len` function.
The :class:`Query` class also provides the following methods for accessing
the results of the query:

getresult -- get query values as list of tuples
-----------------------------------------------

.. method:: Query.getresult()

    Get query values as list of tuples

    :returns: result values as a list of tuples
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns query results as a list of tuples.
More information about this result may be accessed using
:meth:`Query.listfields`, :meth:`Query.fieldname`
and :meth:`Query.fieldnum` methods.

Note that since PyGreSQL 5.0 this method will return the values of array
type columns as Python lists.

Since PyGreSQL 5.1 the :class:`Query` can be also used directly as
an iterable sequence, i.e. you can iterate over the :class:`Query`
object to get the same tuples as returned by :meth:`Query.getresult`.
This is slightly more efficient than getting the full list of results,
but note that the full result is always fetched from the server anyway
when the query is executed.

You can also call :func:`len` on a query to find the number of rows
in the result, and access row tuples using their index directly on
the :class:`Query` object.

When the :class:`Query` object was returned by :meth:`Connection.send_query`,
other return values are also possible, as documented there.

dictresult/dictiter -- get query values as dictionaries
-------------------------------------------------------

.. method:: Query.dictresult()

    Get query values as list of dictionaries

    :returns: result values as a list of dictionaries
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns query results as a list of dictionaries which have
the field names as keys.

If the query has duplicate field names, you will get the value for the
field with the highest index in the query.

Note that since PyGreSQL 5.0 this method will return the values of array
type columns as Python lists.

.. method:: Query.dictiter()

    Get query values as iterable of dictionaries

    :returns: result values as an iterable of dictionaries
    :rtype: iterable
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns query results as an iterable of dictionaries which have
the field names as keys. This is slightly more efficient than getting the full
list of results as dictionaries, but note that the full result is always
fetched from the server anyway when the query is executed.

If the query has duplicate field names, you will get the value for the
field with the highest index in the query.

When the :class:`Query` object was returned by :meth:`Connection.send_query`,
other return values are also possible, as documented there.

.. versionadded:: 5.1

namedresult/namediter -- get query values as named tuples
---------------------------------------------------------

.. method:: Query.namedresult()

    Get query values as list of named tuples

    :returns: result values as a list of named tuples
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises TypeError: named tuples not supported
    :raises MemoryError: internal memory error

This method returns query results as a list of named tuples with
proper field names.

Column names in the database that are not valid as field names for
named tuples (particularly, names starting with an underscore) are
automatically renamed to valid positional names.

Note that since PyGreSQL 5.0 this method will return the values of array
type columns as Python lists.

.. versionadded:: 4.1

.. method:: Query.namediter()

    Get query values as iterable of named tuples

    :returns: result values as an iterable of named tuples
    :rtype: iterable
    :raises TypeError: too many (any) parameters
    :raises TypeError: named tuples not supported
    :raises MemoryError: internal memory error

This method returns query results as an iterable of named tuples with
proper field names. This is slightly more efficient than getting the full
list of results as named tuples, but note that the full result is always
fetched from the server anyway when the query is executed.

Column names in the database that are not valid as field names for
named tuples (particularly, names starting with an underscore) are
automatically renamed to valid positional names.

When the :class:`Query` object was returned by :meth:`Connection.send_query`,
other return values are also possible, as documented there.

.. versionadded:: 5.1

scalarresult/scalariter -- get query values as scalars
------------------------------------------------------

.. method:: Query.scalarresult()

    Get first fields from query result as list of scalar values

    :returns: first fields from result as a list of scalar values
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns the first fields from the query results as a list of
scalar values in the order returned by the server.

.. versionadded:: 5.1

.. method:: Query.scalariter()

    Get first fields from query result as iterable of scalar values

    :returns: first fields from result as an iterable of scalar values
    :rtype: list
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

This method returns the first fields from the query results as an iterable
of scalar values in the order returned by the server. This is slightly more
efficient than getting the full list of results as rows or scalar values,
but note that the full result is always fetched from the server anyway when
the query is executed.

.. versionadded:: 5.1

one/onedict/onenamed/onescalar -- get one result of a query
-----------------------------------------------------------

.. method:: Query.one()

    Get one row from the result of a query as a tuple

    :returns: next row from the query results as a tuple of fields
    :rtype: tuple or None
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns only one row from the result as a tuple of fields.

This method can be called multiple times to return more rows.
It returns None if the result does not contain one more row.

.. versionadded:: 5.1

.. method:: Query.onedict()

    Get one row from the result of a query as a dictionary

    :returns: next row from the query results as a dictionary
    :rtype: dict or None
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns only one row from the result as a dictionary with the field names
used as the keys.

This method can be called multiple times to return more rows.
It returns None if the result does not contain one more row.

.. versionadded:: 5.1

.. method:: Query.onenamed()

    Get one row from the result of a query as named tuple

    :returns: next row from the query results as a named tuple
    :rtype: namedtuple or None
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns only one row from the result as a named tuple with proper field names.

Column names in the database that are not valid as field names for
named tuples (particularly, names starting with an underscore) are
automatically renamed to valid positional names.

This method can be called multiple times to return more rows.
It returns None if the result does not contain one more row.

.. versionadded:: 5.1

.. method:: Query.onescalar()

    Get one row from the result of a query as scalar value

    :returns: next row from the query results as a scalar value
    :rtype: type of first field or None
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns the first field of the next row from the result as a scalar value.

This method can be called multiple times to return more rows as scalars.
It returns None if the result does not contain one more row.

.. versionadded:: 5.1

single/singledict/singlenamed/singlescalar -- get single result of a query
--------------------------------------------------------------------------

.. method:: Query.single()

    Get single row from the result of a query as a tuple

    :returns: single row from the query results as a tuple of fields
    :rtype: tuple
    :raises pg.InvalidResultError: result does not have exactly one row
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns a single row from the result as a tuple of fields.

This method returns the same single row when called multiple times.
It raises an :exc:`pg.InvalidResultError` if the result does not have exactly
one row. More specifically, this will be of type :exc:`pg.NoResultError` if it
is empty and of type :exc:`pg.MultipleResultsError` if it has multiple rows.

.. versionadded:: 5.1

.. method:: Query.singledict()

    Get single row from the result of a query as a dictionary

    :returns: single row from the query results as a dictionary
    :rtype: dict
    :raises pg.InvalidResultError: result does not have exactly one row
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns a single row from the result as a dictionary with the field names
used as the keys.

This method returns the same single row when called multiple times.
It raises an :exc:`pg.InvalidResultError` if the result does not have exactly
one row. More specifically, this will be of type :exc:`pg.NoResultError` if it
is empty and of type :exc:`pg.MultipleResultsError` if it has multiple rows.

.. versionadded:: 5.1

.. method:: Query.singlenamed()

    Get single row from the result of a query as named tuple

    :returns: single row from the query results as a named tuple
    :rtype: namedtuple
    :raises pg.InvalidResultError: result does not have exactly one row
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns single row from the result as a named tuple with proper field names.

Column names in the database that are not valid as field names for
named tuples (particularly, names starting with an underscore) are
automatically renamed to valid positional names.

This method returns the same single row when called multiple times.
It raises an :exc:`pg.InvalidResultError` if the result does not have exactly
one row. More specifically, this will be of type :exc:`pg.NoResultError` if it
is empty and of type :exc:`pg.MultipleResultsError` if it has multiple rows.

.. versionadded:: 5.1

.. method:: Query.singlescalar()

    Get single row from the result of a query as scalar value

    :returns: single row from the query results as a scalar value
    :rtype: type of first field
    :raises pg.InvalidResultError: result does not have exactly one row
    :raises TypeError: too many (any) parameters
    :raises MemoryError: internal memory error

Returns the first field of a single row from the result as a scalar value.

This method returns the same single row as scalar when called multiple times.
It raises an :exc:`pg.InvalidResultError` if the result does not have exactly
one row. More specifically, this will be of type :exc:`pg.NoResultError` if it
is empty and of type :exc:`pg.MultipleResultsError` if it has multiple rows.

.. versionadded:: 5.1

listfields -- list field names of query result
----------------------------------------------

.. method:: Query.listfields()

    List field names of query result

    :returns: field names
    :rtype: list
    :raises TypeError: too many parameters

This method returns the list of field names defined for the query result.
The fields are in the same order as the result values.

fieldname, fieldnum -- field name/number conversion
---------------------------------------------------

.. method:: Query.fieldname(num)

    Get field name from its number

    :param int num: field number
    :returns: field name
    :rtype: str
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises ValueError: invalid field number

This method allows to find a field name from its rank number. It can be
useful for displaying a result. The fields are in the same order as the
result values.

.. method:: Query.fieldnum(name)

    Get field number from its name

    :param str name: field name
    :returns: field number
    :rtype: int
    :raises TypeError: invalid connection, bad parameter type, or too many parameters
    :raises ValueError: unknown field name

This method returns a field number given its name. It can be used to
build a function that converts result list strings to their correct
type, using a hardcoded table definition. The number returned is the
field rank in the query result.

fieldinfo -- detailed info about query result fields
----------------------------------------------------

.. method:: Query.fieldinfo([field])

    Get information on one or all fields of the query

    :param field: a column number or name (optional)
    :type field: int or str
    :returns: field info tuple(s) for all fields or given field
    :rtype: tuple
    :raises IndexError: field does not exist
    :raises TypeError: too many parameters

If the ``field`` is specified by passing either a column number or a field
name, a four-tuple with information for the specified field of the query
result will be returned. If no ``field`` is specified, a tuple of four-tuples
for every field of the previous query result will be returned, in the same
order as they appear in the query result.

The four-tuples contain the following information: The field name, the
internal OID number of the field type, the size in bytes of the column or a
negative value if it is of variable size, and a type-specific modifier value.

.. versionadded:: 5.2

ntuples -- return number of tuples in query object
--------------------------------------------------

.. method:: Query.ntuples()

    Return number of tuples in query object

    :returns: number of tuples in :class:`Query`
    :rtype: int
    :raises TypeError: Too many arguments.

This method returns the number of tuples in the query result.

.. deprecated:: 5.1
   You can use the normal :func:`len` function instead.

memsize -- return number of bytes allocated by query result
-----------------------------------------------------------

.. method:: Query.memsize()

    Return number of bytes allocated by query result

    :returns: number of bytes allocated for the query result
    :rtype: int
    :raises TypeError: Too many arguments.

This method returns the number of bytes allocated for the query result.

.. versionadded:: 5.2 (needs PostgreSQL >= 12)

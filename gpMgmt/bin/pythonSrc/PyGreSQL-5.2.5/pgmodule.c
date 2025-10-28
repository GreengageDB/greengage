/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * This is the main file for the C extension module.
 *
 * Copyright (c) 2023 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Note: This should be linked against the same C runtime lib as Python */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

/* The type definitions from <server/catalog/pg_type.h> */
#include "pgtypes.h"

/* Macros for single-source Python 2/3 compatibility */
#include "py3c.h"

static PyObject *Error, *Warning, *InterfaceError, *DatabaseError,
                *InternalError, *OperationalError, *ProgrammingError,
                *IntegrityError, *DataError, *NotSupportedError,
                *InvalidResultError, *NoResultError, *MultipleResultsError;

#define _TOSTRING(x) #x
#define TOSTRING(x) _TOSTRING(x)
static const char *PyPgVersion = TOSTRING(PYGRESQL_VERSION);

#if SIZEOF_SIZE_T != SIZEOF_INT
#define Py_InitModule4 Py_InitModule4_64
#endif

/* Default values */
#define PG_ARRAYSIZE 1

/* Flags for object validity checks */
#define CHECK_OPEN   1
#define CHECK_CLOSE  2
#define CHECK_CNX    4
#define CHECK_RESULT 8
#define CHECK_DQL   16

/* Query result types */
#define RESULT_EMPTY 1
#define RESULT_DML   2
#define RESULT_DDL   3
#define RESULT_DQL   4

/* Flags for move methods */
#define QUERY_MOVEFIRST 1
#define QUERY_MOVELAST  2
#define QUERY_MOVENEXT  3
#define QUERY_MOVEPREV  4

#define MAX_BUFFER_SIZE 65536 /* maximum transaction size */
#define MAX_ARRAY_DEPTH 16    /* maximum allowed depth of an array */

/* MODULE GLOBAL VARIABLES */

#ifdef DEFAULT_VARS
static PyObject *pg_default_host;   /* default database host */
static PyObject *pg_default_base;   /* default database name */
static PyObject *pg_default_opt;    /* default connection options */
static PyObject *pg_default_port;   /* default connection port */
static PyObject *pg_default_user;   /* default username */
static PyObject *pg_default_passwd; /* default password */
#endif  /* DEFAULT_VARS */

static PyObject *decimal = NULL,    /* decimal type */
                *dictiter = NULL,   /* function for getting dict results */
                *namediter = NULL,  /* function for getting named results */
                *namednext = NULL,  /* function for getting one named result */
                *scalariter = NULL, /* function for getting scalar results */
                *jsondecode = NULL; /* function for decoding json strings */
static const char *date_format = NULL; /* date format that is always assumed */
static char decimal_point = '.';    /* decimal point used in money values */
static int bool_as_text = 0;   /* whether bool shall be returned as text */
static int array_as_text = 0;  /* whether arrays shall be returned as text */
static int bytea_escaped = 0;  /* whether bytea shall be returned escaped */

static int pg_encoding_utf8 = 0;
static int pg_encoding_latin1 = 0;
static int pg_encoding_ascii = 0;

/*
OBJECTS
=======

  Each object has a number of elements.  The naming scheme will be based on
  the object type.  Here are the elements using example object type "foo".
   - fooType: Type definition for object.
   - fooObject: A structure to hold local object information.
   - foo_methods: Methods declaration.
   - foo_method_name: Object methods.

  The objects that we need to create:
   - pg: The module itself.
   - conn: Connection object returned from pg.connect().
   - notice: Notice object returned from pg.notice().
   - large: Large object returned by pg.conn.locreate() and pg.conn.loimport().
   - query: Query object returned by pg.conn.query().
   - source: Source object returned by pg.conn.source().
*/

/* Forward declarations for types */
static PyTypeObject connType, sourceType, queryType, noticeType, largeType;

/* Forward static declarations */
static void notice_receiver(void *, const PGresult *);

/* Object declarations */

typedef struct
{
    PyObject_HEAD
    int        valid;             /* validity flag */
    PGconn     *cnx;              /* Postgres connection handle */
    const char *date_format;      /* date format derived from datestyle */
    PyObject   *cast_hook;        /* external typecast method */
    PyObject   *notice_receiver;  /* current notice receiver */
}   connObject;
#define is_connObject(v) (PyType(v) == &connType)

typedef struct
{
    PyObject_HEAD
    int        valid;        /* validity flag */
    connObject *pgcnx;       /* parent connection object */
    PGresult   *result;      /* result content */
    int        encoding;     /* client encoding */
    int        result_type;  /* result type (DDL/DML/DQL) */
    long       arraysize;    /* array size for fetch method */
    int        current_row;  /* currently selected row */
    int        max_row;      /* number of rows in the result */
    int        num_fields;   /* number of fields in each row */
}   sourceObject;
#define is_sourceObject(v) (PyType(v) == &sourceType)

typedef struct
{
    PyObject_HEAD
    connObject *pgcnx;    /* parent connection object */
    PGresult const *res;  /* an error or warning */
}   noticeObject;
#define is_noticeObject(v) (PyType(v) == &noticeType)

typedef struct
{
    PyObject_HEAD
    connObject *pgcnx;       /* parent connection object */
    PGresult   *result;      /* result content */
    int        async;        /* flag for asynchronous queries */
    int        encoding;     /* client encoding */
    int        current_row;  /* currently selected row */
    int        max_row;      /* number of rows in the result */
    int        num_fields;   /* number of fields in each row */
    int        *col_types;   /* PyGreSQL column types */
}   queryObject;
#define is_queryObject(v) (PyType(v) == &queryType)

#ifdef LARGE_OBJECTS
typedef struct
{
    PyObject_HEAD
    connObject *pgcnx;  /* parent connection object */
    Oid lo_oid;         /* large object oid */
    int lo_fd;          /* large object fd */
}   largeObject;
#define is_largeObject(v) (PyType(v) == &largeType)
#endif /* LARGE_OBJECTS */

/* Internal functions */
#include "pginternal.c"

/* Connection object */
#include "pgconn.c"

/* Query object */
#include "pgquery.c"

/* Source object */
#include "pgsource.c"

/* Notice object */
#include "pgnotice.c"

/* Large objects */
#ifdef LARGE_OBJECTS
#include "pglarge.c"
#endif

/* MODULE FUNCTIONS */

/* Connect to a database. */
static char pg_connect__doc__[] =
"connect(dbname, host, port, opt, user, passwd, wait) -- connect to a PostgreSQL database\n\n"
"The connection uses the specified parameters (optional, keywords aware).\n";

static PyObject *
pg_connect(PyObject *self, PyObject *args, PyObject *dict)
{
    static const char *kwlist[] =
    {
        "dbname", "host", "port", "opt", "user", "passwd", "nowait", NULL
    };

    char *pghost, *pgopt, *pgdbname, *pguser, *pgpasswd;
    int pgport = -1, nowait = 0, nkw = 0;
    char port_buffer[20];
    const char *keywords[sizeof(kwlist) / sizeof(*kwlist) + 1],
               *values[sizeof(kwlist) / sizeof(*kwlist) + 1];
    connObject *conn_obj;

    pghost = pgopt = pgdbname = pguser = pgpasswd = NULL;

    /*
     * parses standard arguments With the right compiler warnings, this
     * will issue a diagnostic. There is really no way around it.  If I
     * don't declare kwlist as const char *kwlist[] then it complains when
     * I try to assign all those constant strings to it.
     */
    if (!PyArg_ParseTupleAndKeywords(
        args, dict, "|zzizzzi", (char**)kwlist,
        &pgdbname, &pghost, &pgport, &pgopt, &pguser, &pgpasswd, &nowait))
    {
        return NULL;
    }

#ifdef DEFAULT_VARS
    /* handles defaults variables (for uninitialised vars) */
    if ((!pghost) && (pg_default_host != Py_None))
        pghost = PyBytes_AsString(pg_default_host);

    if ((pgport == -1) && (pg_default_port != Py_None))
        pgport = (int) PyInt_AsLong(pg_default_port);

    if ((!pgopt) && (pg_default_opt != Py_None))
        pgopt = PyBytes_AsString(pg_default_opt);

    if ((!pgdbname) && (pg_default_base != Py_None))
        pgdbname = PyBytes_AsString(pg_default_base);

    if ((!pguser) && (pg_default_user != Py_None))
        pguser = PyBytes_AsString(pg_default_user);

    if ((!pgpasswd) && (pg_default_passwd != Py_None))
        pgpasswd = PyBytes_AsString(pg_default_passwd);
#endif /* DEFAULT_VARS */

    if (!(conn_obj = PyObject_New(connObject, &connType))) {
        set_error_msg(InternalError, "Can't create new connection object");
        return NULL;
    }

    conn_obj->valid = 1;
    conn_obj->cnx = NULL;
    conn_obj->date_format = date_format;
    conn_obj->cast_hook = NULL;
    conn_obj->notice_receiver = NULL;

    if (pghost)
    {
        keywords[nkw] = "host";
        values[nkw++] = pghost;
    }
    if (pgopt)
    {
        keywords[nkw] = "options";
        values[nkw++] = pgopt;
    }
    if (pgdbname)
    {
        keywords[nkw] = "dbname";
        values[nkw++] = pgdbname;
    }
    if (pguser)
    {
        keywords[nkw] = "user";
        values[nkw++] = pguser;
    }
    if (pgpasswd)
    {
        keywords[nkw] = "password";
        values[nkw++] = pgpasswd;
    }
    if (pgport != -1)
    {
        memset(port_buffer, 0, sizeof(port_buffer));
        sprintf(port_buffer, "%d", pgport);

        keywords[nkw] = "port";
        values[nkw++] = port_buffer;
    }
    keywords[nkw] = values[nkw] = NULL;

    Py_BEGIN_ALLOW_THREADS
    conn_obj->cnx = nowait ? PQconnectStartParams(keywords, values, 1) :
        PQconnectdbParams(keywords, values, 1);
    Py_END_ALLOW_THREADS

    if (PQstatus(conn_obj->cnx) == CONNECTION_BAD) {
        set_error(InternalError, "Cannot connect", conn_obj->cnx, NULL);
        Py_XDECREF(conn_obj);
        return NULL;
    }

    return (PyObject *) conn_obj;
}

#ifdef PQLIB_INFO

/* Get version of libpq that is being used */
static char pg_get_pqlib_version__doc__[] =
"get_pqlib_version() -- get the version of libpq that is being used";

static PyObject *
pg_get_pqlib_version(PyObject *self, PyObject *noargs) {
    return PyLong_FromLong(PQlibVersion());
}

#endif /* PQLIB_INFO */

/* Escape string */
static char pg_escape_string__doc__[] =
"escape_string(string) -- escape a string for use within SQL";

static PyObject *
pg_escape_string(PyObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */
    int encoding = -1;         /* client encoding */

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &from, &from_length);
    }
    else if (PyUnicode_Check(string)) {
        encoding = pg_encoding_ascii;
        tmp_obj = get_encoded_string(string, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method escape_string() expects a string as argument");
        return NULL;
    }

    to_length = 2 * (size_t) from_length + 1;
    if ((Py_ssize_t ) to_length < from_length) { /* overflow */
        to_length = (size_t) from_length;
        from_length = (from_length - 1)/2;
    }
    to = (char *) PyMem_Malloc(to_length);
    to_length = (size_t) PQescapeString(to, from, (size_t) from_length);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t) to_length);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t) to_length, encoding);
    PyMem_Free(to);
    return to_obj;
}

/* Escape bytea */
static char pg_escape_bytea__doc__[] =
"escape_bytea(data) -- escape binary data for use within SQL as type bytea";

static PyObject *
pg_escape_bytea(PyObject *self, PyObject *data)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */
    int encoding = -1;         /* client encoding */

    if (PyBytes_Check(data)) {
        PyBytes_AsStringAndSize(data, &from, &from_length);
    }
    else if (PyUnicode_Check(data)) {
        encoding = pg_encoding_ascii;
        tmp_obj = get_encoded_string(data, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method escape_bytea() expects a string as argument");
        return NULL;
    }

    to = (char *) PQescapeBytea(
        (unsigned char*) from, (size_t) from_length, &to_length);

    Py_XDECREF(tmp_obj);

    if (encoding == -1)
        to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t) to_length - 1);
    else
        to_obj = get_decoded_string(to, (Py_ssize_t) to_length - 1, encoding);
    if (to)
        PQfreemem(to);
    return to_obj;
}

/* Unescape bytea */
static char pg_unescape_bytea__doc__[] =
"unescape_bytea(string) -- unescape bytea data retrieved as text";

static PyObject *
pg_unescape_bytea(PyObject *self, PyObject *data)
{
    PyObject *tmp_obj = NULL,  /* auxiliary string object */
             *to_obj;          /* string object to return */
    char *from,  /* our string argument as encoded string */
         *to;    /* the result as encoded string */
    Py_ssize_t from_length;    /* length of string */
    size_t to_length;          /* length of result */

    if (PyBytes_Check(data)) {
        PyBytes_AsStringAndSize(data, &from, &from_length);
    }
    else if (PyUnicode_Check(data)) {
        tmp_obj = get_encoded_string(data, pg_encoding_ascii);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &from, &from_length);
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Method unescape_bytea() expects a string as argument");
        return NULL;
    }

    to = (char *) PQunescapeBytea((unsigned char*) from, &to_length);

    Py_XDECREF(tmp_obj);

    if (!to) return PyErr_NoMemory();

    to_obj = PyBytes_FromStringAndSize(to, (Py_ssize_t) to_length);
    PQfreemem(to);

    return to_obj;
}

/* Set fixed datestyle. */
static char pg_set_datestyle__doc__[] =
"set_datestyle(style) -- set which style is assumed";

static PyObject *
pg_set_datestyle(PyObject *self, PyObject *args)
{
    const char *datestyle = NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &datestyle)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_datestyle() expects a string or None as argument");
        return NULL;
    }

    date_format = datestyle ? date_style_to_format(datestyle) : NULL;

    Py_INCREF(Py_None); return Py_None;
}

/* Get fixed datestyle. */
static char pg_get_datestyle__doc__[] =
"get_datestyle() -- get which date style is assumed";

static PyObject *
pg_get_datestyle(PyObject *self, PyObject *noargs)
{
    if (date_format) {
        return PyStr_FromString(date_format_to_style(date_format));
    }
    else {
        Py_INCREF(Py_None); return Py_None;
    }
}

/* Get decimal point. */
static char pg_get_decimal_point__doc__[] =
"get_decimal_point() -- get decimal point to be used for money values";

static PyObject *
pg_get_decimal_point(PyObject *self, PyObject *noargs)
{
    PyObject *ret;
    char s[2];

    if (decimal_point) {
        s[0] = decimal_point; s[1] = '\0';
        ret = PyStr_FromString(s);
    }
    else {
        Py_INCREF(Py_None); ret = Py_None;
    }

    return ret;
}

/* Set decimal point. */
static char pg_set_decimal_point__doc__[] =
"set_decimal_point(char) -- set decimal point to be used for money values";

static PyObject *
pg_set_decimal_point(PyObject *self, PyObject *args)
{
    PyObject *ret = NULL;
    char *s = NULL;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "z", &s)) {
        if (!s)
            s = "\0";
        else if (*s && (*(s+1) || !strchr(".,;: '*/_`|", *s)))
            s = NULL;
    }

    if (s) {
        decimal_point = *s;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_decimal_mark() expects"
                        " a decimal mark character as argument");
    }
    return ret;
}

/* Get decimal type. */
static char pg_get_decimal__doc__[] =
"get_decimal() -- get the decimal type to be used for numeric values";

static PyObject *
pg_get_decimal(PyObject *self, PyObject *noargs)
{
    PyObject *ret;

    ret = decimal ? decimal : Py_None;
    Py_INCREF(ret);

    return ret;
}

/* Set decimal type. */
static char pg_set_decimal__doc__[] =
"set_decimal(cls) -- set a decimal type to be used for numeric values";

static PyObject *
pg_set_decimal(PyObject *self, PyObject *cls)
{
    PyObject *ret = NULL;

    if (cls == Py_None) {
        Py_XDECREF(decimal); decimal = NULL;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else if (PyCallable_Check(cls)) {
        Py_XINCREF(cls); Py_XDECREF(decimal); decimal = cls;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_decimal() expects"
                        " a callable or None as argument");
    }

    return ret;
}

/* Get usage of bool values. */
static char pg_get_bool__doc__[] =
"get_bool() -- check whether boolean values are converted to bool";

static PyObject *
pg_get_bool(PyObject *self, PyObject *noargs)
{
    PyObject *ret;

    ret = bool_as_text ? Py_False : Py_True;
    Py_INCREF(ret);

    return ret;
}

/* Set usage of bool values. */
static char pg_set_bool__doc__[] =
"set_bool(on) -- set whether boolean values should be converted to bool";

static PyObject *
pg_set_bool(PyObject *self, PyObject *args)
{
    PyObject *ret = NULL;
    int i;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "i", &i)) {
        bool_as_text = i ? 0 : 1;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_bool() expects a boolean value as argument");
    }

    return ret;
}

/* Get conversion of arrays to lists. */
static char pg_get_array__doc__[] =
"get_array() -- check whether arrays are converted as lists";

static PyObject *
pg_get_array(PyObject *self, PyObject *noargs)
{
    PyObject *ret;

    ret = array_as_text ? Py_False : Py_True;
    Py_INCREF(ret);

    return ret;
}

/* Set conversion of arrays to lists. */
static char pg_set_array__doc__[] =
"set_array(on) -- set whether arrays should be converted to lists";

static PyObject *
pg_set_array(PyObject* self, PyObject* args)
{
    PyObject* ret = NULL;
    int i;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "i", &i)) {
        array_as_text = i ? 0 : 1;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_array() expects a boolean value as argument");
    }

    return ret;
}

/* Check whether bytea values are unescaped. */
static char pg_get_bytea_escaped__doc__[] =
"get_bytea_escaped() -- check whether bytea will be returned escaped";

static PyObject *
pg_get_bytea_escaped(PyObject *self, PyObject *noargs)
{
    PyObject *ret;

    ret = bytea_escaped ? Py_True : Py_False;
    Py_INCREF(ret);

    return ret;
}

/* Set usage of bool values. */
static char pg_set_bytea_escaped__doc__[] =
"set_bytea_escaped(on) -- set whether bytea will be returned escaped";

static PyObject *
pg_set_bytea_escaped(PyObject *self, PyObject *args)
{
    PyObject *ret = NULL;
    int i;

    /* gets arguments */
    if (PyArg_ParseTuple(args, "i", &i)) {
        bytea_escaped = i ? 1 : 0;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_bytea_escaped() expects"
                        " a boolean value as argument");
    }

    return ret;
}

/* set query helper functions (not part of public API) */

static char pg_set_query_helpers__doc__[] =
"set_query_helpers(*helpers) -- set internal query helper functions";

static PyObject *
pg_set_query_helpers(PyObject *self, PyObject *args)
{
    /* gets arguments */
    if (!PyArg_ParseTuple(args, "O!O!O!O!",
        &PyFunction_Type, &dictiter,
        &PyFunction_Type, &namediter,
        &PyFunction_Type, &namednext,
        &PyFunction_Type, &scalariter))
    {
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

/* Get json decode function. */
static char pg_get_jsondecode__doc__[] =
"get_jsondecode() -- get the function used for decoding json results";

static PyObject *
pg_get_jsondecode(PyObject *self, PyObject *noargs)
{
    PyObject *ret;

    ret = jsondecode;
    if (!ret)
        ret = Py_None;
    Py_INCREF(ret);

    return ret;
}

/* Set json decode function. */
static char pg_set_jsondecode__doc__[] =
"set_jsondecode(func) -- set a function to be used for decoding json results";

static PyObject *
pg_set_jsondecode(PyObject *self, PyObject *func)
{
    PyObject *ret = NULL;

    if (func == Py_None) {
        Py_XDECREF(jsondecode); jsondecode = NULL;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else if (PyCallable_Check(func)) {
        Py_XINCREF(func); Py_XDECREF(jsondecode); jsondecode = func;
        Py_INCREF(Py_None); ret = Py_None;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function jsondecode() expects"
                        " a callable or None as argument");
    }

    return ret;
}

#ifdef DEFAULT_VARS

/* Get default host. */
static char pg_get_defhost__doc__[] =
"get_defhost() -- return default database host";

static PyObject *
pg_get_defhost(PyObject *self, PyObject *noargs)
{
    Py_XINCREF(pg_default_host);
    return pg_default_host;
}

/* Set default host. */
static char pg_set_defhost__doc__[] =
"set_defhost(string) -- set default database host and return previous value";

static PyObject *
pg_set_defhost(PyObject *self, PyObject *args)
{
    char *tmp = NULL;
    PyObject *old;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &tmp)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_defhost() expects a string or None as argument");
        return NULL;
    }

    /* adjusts value */
    old = pg_default_host;

    if (tmp) {
        pg_default_host = PyStr_FromString(tmp);
    }
    else {
        Py_INCREF(Py_None);
        pg_default_host = Py_None;
    }

    return old;
}

/* Get default database. */
static char pg_get_defbase__doc__[] =
"get_defbase() -- return default database name";

static PyObject *
pg_get_defbase(PyObject *self, PyObject *noargs)
{
    Py_XINCREF(pg_default_base);
    return pg_default_base;
}

/* Set default database. */
static char pg_set_defbase__doc__[] =
"set_defbase(string) -- set default database name and return previous value";

static PyObject *
pg_set_defbase(PyObject *self, PyObject *args)
{
    char *tmp = NULL;
    PyObject *old;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &tmp)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_defbase() Argument a string or None as argument");
        return NULL;
    }

    /* adjusts value */
    old = pg_default_base;

    if (tmp) {
        pg_default_base = PyStr_FromString(tmp);
    }
    else {
        Py_INCREF(Py_None);
        pg_default_base = Py_None;
    }

    return old;
}

/* Get default options. */
static char pg_get_defopt__doc__[] =
"get_defopt() -- return default database options";

static PyObject *
pg_get_defopt(PyObject *self, PyObject *noargs)
{
    Py_XINCREF(pg_default_opt);
    return pg_default_opt;
}

/* Set default options. */
static char pg_set_defopt__doc__[] =
"set_defopt(string) -- set default options and return previous value";

static PyObject *
pg_setdefopt(PyObject *self, PyObject *args)
{
    char *tmp = NULL;
    PyObject *old;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &tmp)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_defopt() expects a string or None as argument");
        return NULL;
    }

    /* adjusts value */
    old = pg_default_opt;

    if (tmp) {
        pg_default_opt = PyStr_FromString(tmp);
    }
    else {
        Py_INCREF(Py_None);
        pg_default_opt = Py_None;
    }

    return old;
}

/* Get default username. */
static char pg_get_defuser__doc__[] =
"get_defuser() -- return default database username";

static PyObject *
pg_get_defuser(PyObject *self, PyObject *noargs)
{
    Py_XINCREF(pg_default_user);
    return pg_default_user;
}

/* Set default username. */

static char pg_set_defuser__doc__[] =
"set_defuser(name) -- set default username and return previous value";

static PyObject *
pg_set_defuser(PyObject *self, PyObject *args)
{
    char *tmp = NULL;
    PyObject *old;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &tmp)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_defuser() expects a string or None as argument");
        return NULL;
    }

    /* adjusts value */
    old = pg_default_user;

    if (tmp) {
        pg_default_user = PyStr_FromString(tmp);
    }
    else {
        Py_INCREF(Py_None);
        pg_default_user = Py_None;
    }

    return old;
}

/* Set default password. */
static char pg_set_defpasswd__doc__[] =
"set_defpasswd(password) -- set default database password";

static PyObject *
pg_set_defpasswd(PyObject *self, PyObject *args)
{
    char *tmp = NULL;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "z", &tmp)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function set_defpasswd() expects a string or None as argument");
        return NULL;
    }

    if (tmp) {
        pg_default_passwd = PyStr_FromString(tmp);
    }
    else {
        Py_INCREF(Py_None);
        pg_default_passwd = Py_None;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

/* Get default port. */
static char pg_get_defport__doc__[] =
"get_defport() -- return default database port";

static PyObject *
pg_get_defport(PyObject *self, PyObject *noargs)
{
    Py_XINCREF(pg_default_port);
    return pg_default_port;
}

/* Set default port. */
static char pg_set_defport__doc__[] =
"set_defport(port) -- set default port and return previous value";

static PyObject *
pg_set_defport(PyObject *self, PyObject *args)
{
    long int port = -2;
    PyObject *old;

    /* gets arguments */
    if ((!PyArg_ParseTuple(args, "l", &port)) || (port < -1)) {
        PyErr_SetString(PyExc_TypeError,
                        "Function set_deport expects"
                        " a positive integer or -1 as argument");
        return NULL;
    }

    /* adjusts value */
    old = pg_default_port;

    if (port != -1) {
        pg_default_port = PyInt_FromLong(port);
    }
    else {
        Py_INCREF(Py_None);
        pg_default_port = Py_None;
    }

    return old;
}
#endif /* DEFAULT_VARS */

/* Cast a string with a text representation of an array to a list. */
static char pg_cast_array__doc__[] =
"cast_array(string, cast=None, delim=',') -- cast a string as an array";

PyObject *
pg_cast_array(PyObject *self, PyObject *args, PyObject *dict)
{
    static const char *kwlist[] = {"string", "cast", "delim", NULL};
    PyObject *string_obj, *cast_obj = NULL, *ret;
    char *string, delim = ',';
    Py_ssize_t size;
    int encoding;

    if (!PyArg_ParseTupleAndKeywords(
        args, dict, "O|Oc",
        (char**) kwlist, &string_obj, &cast_obj, &delim))
    {
        return NULL;
    }

    if (PyBytes_Check(string_obj)) {
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        string_obj = NULL;
        encoding = pg_encoding_ascii;
    }
    else if (PyUnicode_Check(string_obj)) {
        string_obj = PyUnicode_AsUTF8String(string_obj);
        if (!string_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        encoding = pg_encoding_utf8;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_array() expects a string as first argument");
        return NULL;
    }

    if (cast_obj == Py_None) {
        cast_obj = NULL;
    }
    else if (cast_obj && !PyCallable_Check(cast_obj)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_array() expects a callable as second argument");
        return NULL;
    }

    ret = cast_array(string, size, encoding, 0, cast_obj, delim);

    Py_XDECREF(string_obj);

    return ret;
}

/* Cast a string with a text representation of a record to a tuple. */
static char pg_cast_record__doc__[] =
"cast_record(string, cast=None, delim=',') -- cast a string as a record";

PyObject *
pg_cast_record(PyObject *self, PyObject *args, PyObject *dict)
{
    static const char *kwlist[] = {"string", "cast", "delim", NULL};
    PyObject *string_obj, *cast_obj = NULL, *ret;
    char *string, delim = ',';
    Py_ssize_t size, len;
    int encoding;

    if (!PyArg_ParseTupleAndKeywords(
        args, dict, "O|Oc",
        (char**) kwlist, &string_obj, &cast_obj, &delim))
    {
        return NULL;
    }

    if (PyBytes_Check(string_obj)) {
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        string_obj = NULL;
        encoding = pg_encoding_ascii;
    }
    else if (PyUnicode_Check(string_obj)) {
        string_obj = PyUnicode_AsUTF8String(string_obj);
        if (!string_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(string_obj, &string, &size);
        encoding = pg_encoding_utf8;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_record() expects a string as first argument");
        return NULL;
    }

    if (!cast_obj || PyCallable_Check(cast_obj)) {
        len = 0;
    }
    else if (cast_obj == Py_None) {
        cast_obj = NULL; len = 0;
    }
    else if (PyTuple_Check(cast_obj) || PyList_Check(cast_obj)) {
        len = PySequence_Size(cast_obj);
        if (!len) {
            cast_obj = NULL;
        }
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Function cast_record() expects a callable"
                        " or tuple or list of callables as second argument");
        return NULL;
    }

    ret = cast_record(string, size, encoding, 0, cast_obj, len, delim);

    Py_XDECREF(string_obj);

    return ret;
}

/* Cast a string with a text representation of an hstore to a dict. */
static char pg_cast_hstore__doc__[] =
"cast_hstore(string) -- cast a string as an hstore";

PyObject *
pg_cast_hstore(PyObject *self, PyObject *string)
{
    PyObject *tmp_obj = NULL, *ret;
    char *s;
    Py_ssize_t size;
    int encoding;

    if (PyBytes_Check(string)) {
        PyBytes_AsStringAndSize(string, &s, &size);
        encoding = pg_encoding_ascii;
    }
    else if (PyUnicode_Check(string)) {
        tmp_obj = PyUnicode_AsUTF8String(string);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &s, &size);
        encoding = pg_encoding_utf8;
    }
    else {
        PyErr_SetString(
            PyExc_TypeError,
            "Function cast_hstore() expects a string as first argument");
        return NULL;
    }

    ret = cast_hstore(s, size, encoding);

    Py_XDECREF(tmp_obj);

    return ret;
}

/* The list of functions defined in the module */

static struct PyMethodDef pg_methods[] = {
    {"connect", (PyCFunction) pg_connect,
        METH_VARARGS|METH_KEYWORDS, pg_connect__doc__},
    {"escape_string", (PyCFunction) pg_escape_string,
        METH_O, pg_escape_string__doc__},
    {"escape_bytea", (PyCFunction) pg_escape_bytea,
        METH_O, pg_escape_bytea__doc__},
    {"unescape_bytea", (PyCFunction) pg_unescape_bytea,
        METH_O, pg_unescape_bytea__doc__},
    {"get_datestyle", (PyCFunction) pg_get_datestyle,
        METH_NOARGS, pg_get_datestyle__doc__},
    {"set_datestyle", (PyCFunction) pg_set_datestyle,
        METH_VARARGS, pg_set_datestyle__doc__},
    {"get_decimal_point", (PyCFunction) pg_get_decimal_point,
        METH_NOARGS, pg_get_decimal_point__doc__},
    {"set_decimal_point", (PyCFunction) pg_set_decimal_point,
        METH_VARARGS, pg_set_decimal_point__doc__},
    {"get_decimal", (PyCFunction) pg_get_decimal,
        METH_NOARGS, pg_get_decimal__doc__},
    {"set_decimal", (PyCFunction) pg_set_decimal,
        METH_O, pg_set_decimal__doc__},
    {"get_bool", (PyCFunction) pg_get_bool,
        METH_NOARGS, pg_get_bool__doc__},
    {"set_bool", (PyCFunction) pg_set_bool,
        METH_VARARGS, pg_set_bool__doc__},
    {"get_array", (PyCFunction) pg_get_array,
        METH_NOARGS, pg_get_array__doc__},
    {"set_array", (PyCFunction) pg_set_array,
        METH_VARARGS, pg_set_array__doc__},
    {"set_query_helpers", (PyCFunction) pg_set_query_helpers,
        METH_VARARGS, pg_set_query_helpers__doc__},
    {"get_bytea_escaped", (PyCFunction) pg_get_bytea_escaped,
        METH_NOARGS, pg_get_bytea_escaped__doc__},
    {"set_bytea_escaped", (PyCFunction) pg_set_bytea_escaped,
        METH_VARARGS, pg_set_bytea_escaped__doc__},
    {"get_jsondecode", (PyCFunction) pg_get_jsondecode,
        METH_NOARGS, pg_get_jsondecode__doc__},
    {"set_jsondecode", (PyCFunction) pg_set_jsondecode,
        METH_O, pg_set_jsondecode__doc__},
    {"cast_array", (PyCFunction) pg_cast_array,
        METH_VARARGS|METH_KEYWORDS, pg_cast_array__doc__},
    {"cast_record", (PyCFunction) pg_cast_record,
        METH_VARARGS|METH_KEYWORDS, pg_cast_record__doc__},
    {"cast_hstore", (PyCFunction) pg_cast_hstore,
        METH_O, pg_cast_hstore__doc__},
#ifdef DEFAULT_VARS
    {"get_defhost", pg_get_defhost, METH_NOARGS, pg_get_defhost__doc__},
    {"set_defhost", pg_set_defhost, METH_VARARGS, pg_set_defhost__doc__},
    {"get_defbase", pg_get_defbase, METH_NOARGS, pg_get_defbase__doc__},
    {"set_defbase", pg_set_defbase, METH_VARARGS, pg_set_defbase__doc__},
    {"get_defopt", pg_get_defopt, METH_NOARGS, pg_get_defopt__doc__},
    {"set_defopt", pg_setdefopt, METH_VARARGS, pg_set_defopt__doc__},
    {"get_defport", pg_get_defport, METH_NOARGS, pg_get_defport__doc__},
    {"set_defport", pg_set_defport, METH_VARARGS, pg_set_defport__doc__},
    {"get_defuser", pg_get_defuser, METH_NOARGS, pg_get_defuser__doc__},
    {"set_defuser", pg_set_defuser, METH_VARARGS, pg_set_defuser__doc__},
    {"set_defpasswd", pg_set_defpasswd, METH_VARARGS, pg_set_defpasswd__doc__},
#endif /* DEFAULT_VARS */
#ifdef PQLIB_INFO
    {"get_pqlib_version", (PyCFunction) pg_get_pqlib_version,
        METH_NOARGS, pg_get_pqlib_version__doc__},
#endif /* PQLIB_INFO */
    {NULL, NULL} /* sentinel */
};

static char pg__doc__[] = "Python interface to PostgreSQL DB";

static struct PyModuleDef moduleDef = {
    PyModuleDef_HEAD_INIT,
    "_pg",     /* m_name */
    pg__doc__, /* m_doc */
    -1,        /* m_size */
    pg_methods /* m_methods */
};

/* Initialization function for the module */
MODULE_INIT_FUNC(_pg)
{
    PyObject *mod, *dict, *s;

    /* Create the module and add the functions */

    mod = PyModule_Create(&moduleDef);

    /* Initialize here because some Windows platforms get confused otherwise */
#if IS_PY3
    connType.tp_base = noticeType.tp_base =
        queryType.tp_base = sourceType.tp_base = &PyBaseObject_Type;
#ifdef LARGE_OBJECTS
    largeType.tp_base = &PyBaseObject_Type;
#endif
#else
    connType.ob_type = noticeType.ob_type =
        queryType.ob_type = sourceType.ob_type = &PyType_Type;
#ifdef LARGE_OBJECTS
    largeType.ob_type = &PyType_Type;
#endif
#endif

    if (PyType_Ready(&connType)
        || PyType_Ready(&noticeType)
        || PyType_Ready(&queryType)
        || PyType_Ready(&sourceType)
#ifdef LARGE_OBJECTS
        || PyType_Ready(&largeType)
#endif
        )
    {
        return NULL;
    }

    dict = PyModule_GetDict(mod);

    /* Exceptions as defined by DB-API 2.0 */
    Error = PyErr_NewException("pg.Error", PyExc_StandardError, NULL);
    PyDict_SetItemString(dict, "Error", Error);

    Warning = PyErr_NewException("pg.Warning", PyExc_StandardError, NULL);
    PyDict_SetItemString(dict, "Warning", Warning);

    InterfaceError = PyErr_NewException(
        "pg.InterfaceError", Error, NULL);
    PyDict_SetItemString(dict, "InterfaceError", InterfaceError);

    DatabaseError = PyErr_NewException(
        "pg.DatabaseError", Error, NULL);
    PyDict_SetItemString(dict, "DatabaseError", DatabaseError);

    InternalError = PyErr_NewException(
        "pg.InternalError", DatabaseError, NULL);
    PyDict_SetItemString(dict, "InternalError", InternalError);

    OperationalError = PyErr_NewException(
        "pg.OperationalError", DatabaseError, NULL);
    PyDict_SetItemString(dict, "OperationalError", OperationalError);

    ProgrammingError = PyErr_NewException(
        "pg.ProgrammingError", DatabaseError, NULL);
    PyDict_SetItemString(dict, "ProgrammingError", ProgrammingError);

    IntegrityError = PyErr_NewException(
        "pg.IntegrityError", DatabaseError, NULL);
    PyDict_SetItemString(dict, "IntegrityError", IntegrityError);

    DataError = PyErr_NewException(
        "pg.DataError", DatabaseError, NULL);
    PyDict_SetItemString(dict, "DataError", DataError);

    NotSupportedError = PyErr_NewException(
        "pg.NotSupportedError", DatabaseError, NULL);
    PyDict_SetItemString(dict, "NotSupportedError", NotSupportedError);

    InvalidResultError = PyErr_NewException(
        "pg.InvalidResultError", DataError, NULL);
    PyDict_SetItemString(dict, "InvalidResultError", InvalidResultError);

    NoResultError = PyErr_NewException(
        "pg.NoResultError", InvalidResultError, NULL);
    PyDict_SetItemString(dict, "NoResultError", NoResultError);

    MultipleResultsError = PyErr_NewException(
        "pg.MultipleResultsError", InvalidResultError, NULL);
    PyDict_SetItemString(dict, "MultipleResultsError", MultipleResultsError);

    /* Make the version available */
    s = PyStr_FromString(PyPgVersion);
    PyDict_SetItemString(dict, "version", s);
    PyDict_SetItemString(dict, "__version__", s);
    Py_DECREF(s);

    /* Result types for queries */
    PyDict_SetItemString(dict, "RESULT_EMPTY", PyInt_FromLong(RESULT_EMPTY));
    PyDict_SetItemString(dict, "RESULT_DML", PyInt_FromLong(RESULT_DML));
    PyDict_SetItemString(dict, "RESULT_DDL", PyInt_FromLong(RESULT_DDL));
    PyDict_SetItemString(dict, "RESULT_DQL", PyInt_FromLong(RESULT_DQL));

    /* Transaction states */
    PyDict_SetItemString(dict, "TRANS_IDLE", PyInt_FromLong(PQTRANS_IDLE));
    PyDict_SetItemString(dict, "TRANS_ACTIVE", PyInt_FromLong(PQTRANS_ACTIVE));
    PyDict_SetItemString(dict, "TRANS_INTRANS", PyInt_FromLong(PQTRANS_INTRANS));
    PyDict_SetItemString(dict, "TRANS_INERROR", PyInt_FromLong(PQTRANS_INERROR));
    PyDict_SetItemString(dict, "TRANS_UNKNOWN", PyInt_FromLong(PQTRANS_UNKNOWN));

    /* Polling results */
    PyDict_SetItemString(dict, "POLLING_OK", PyInt_FromLong(PGRES_POLLING_OK));
    PyDict_SetItemString(dict, "POLLING_FAILED", PyInt_FromLong(PGRES_POLLING_FAILED));
    PyDict_SetItemString(dict, "POLLING_READING", PyInt_FromLong(PGRES_POLLING_READING));
    PyDict_SetItemString(dict, "POLLING_WRITING", PyInt_FromLong(PGRES_POLLING_WRITING));

#ifdef LARGE_OBJECTS
    /* Create mode for large objects */
    PyDict_SetItemString(dict, "INV_READ", PyInt_FromLong(INV_READ));
    PyDict_SetItemString(dict, "INV_WRITE", PyInt_FromLong(INV_WRITE));

    /* Position flags for lo_lseek */
    PyDict_SetItemString(dict, "SEEK_SET", PyInt_FromLong(SEEK_SET));
    PyDict_SetItemString(dict, "SEEK_CUR", PyInt_FromLong(SEEK_CUR));
    PyDict_SetItemString(dict, "SEEK_END", PyInt_FromLong(SEEK_END));
#endif /* LARGE_OBJECTS */

#ifdef DEFAULT_VARS
    /* Prepare default values */
    Py_INCREF(Py_None);
    pg_default_host = Py_None;
    Py_INCREF(Py_None);
    pg_default_base = Py_None;
    Py_INCREF(Py_None);
    pg_default_opt = Py_None;
    Py_INCREF(Py_None);
    pg_default_port = Py_None;
    Py_INCREF(Py_None);
    pg_default_user = Py_None;
    Py_INCREF(Py_None);
    pg_default_passwd = Py_None;
#endif /* DEFAULT_VARS */

    /* Store common pg encoding ids */

    pg_encoding_utf8 = pg_char_to_encoding("UTF8");
    pg_encoding_latin1 = pg_char_to_encoding("LATIN1");
    pg_encoding_ascii = pg_char_to_encoding("SQL_ASCII");

    /* Check for errors */
    if (PyErr_Occurred()) {
        return NULL;
    }

    return mod;
}

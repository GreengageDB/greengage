/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The source object - this file is part a of the C extension module.
 *
 * Copyright (c) 2023 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Deallocate source object. */
static void
source_dealloc(sourceObject *self)
{
    if (self->result)
        PQclear(self->result);

    Py_XDECREF(self->pgcnx);
    PyObject_Del(self);
}

/* Return source object as string in human readable form. */
static PyObject *
source_str(sourceObject *self)
{
    switch (self->result_type) {
        case RESULT_DQL:
            return format_result(self->result);
        case RESULT_DDL:
        case RESULT_DML:
            return PyStr_FromString(PQcmdStatus(self->result));
        case RESULT_EMPTY:
        default:
            return PyStr_FromString("(empty PostgreSQL source object)");
    }
}

/* Check source object validity. */
static int
_check_source_obj(sourceObject *self, int level)
{
    if (!self->valid) {
        set_error_msg(OperationalError, "Object has been closed");
        return 0;
    }

    if ((level & CHECK_RESULT) && !self->result) {
        set_error_msg(DatabaseError, "No result");
        return 0;
    }

    if ((level & CHECK_DQL) && self->result_type != RESULT_DQL) {
        set_error_msg(DatabaseError, "Last query did not return tuples");
        return 0;
    }

    if ((level & CHECK_CNX) && !_check_cnx_obj(self->pgcnx)) {
        return 0;
    }

    return 1;
}

/* Get source object attributes. */
static PyObject *
source_getattr(sourceObject *self, PyObject *nameobj)
{
    const char *name = PyStr_AsString(nameobj);

    /* pg connection object */
    if (!strcmp(name, "pgcnx")) {
        if (_check_source_obj(self, 0)) {
            Py_INCREF(self->pgcnx);
            return (PyObject *) (self->pgcnx);
        }
        Py_INCREF(Py_None);
        return Py_None;
    }

    /* arraysize */
    if (!strcmp(name, "arraysize"))
        return PyInt_FromLong(self->arraysize);

    /* resulttype */
    if (!strcmp(name, "resulttype"))
        return PyInt_FromLong(self->result_type);

    /* ntuples */
    if (!strcmp(name, "ntuples"))
        return PyInt_FromLong(self->max_row);

    /* nfields */
    if (!strcmp(name, "nfields"))
        return PyInt_FromLong(self->num_fields);

    /* seeks name in methods (fallback) */
    return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* Set source object attributes. */
static int
source_setattr(sourceObject *self, char *name, PyObject *v)
{
    /* arraysize */
    if (!strcmp(name, "arraysize")) {
        if (!PyInt_Check(v)) {
            PyErr_SetString(PyExc_TypeError, "arraysize must be integer");
            return -1;
        }

        self->arraysize = PyInt_AsLong(v);
        return 0;
    }

    /* unknown attribute */
    PyErr_SetString(PyExc_TypeError, "Not a writable attribute");
    return -1;
}

/* Close object. */
static char source_close__doc__[] =
"close() -- close query object without deleting it\n\n"
"All instances of the query object can no longer be used after this call.\n";

static PyObject *
source_close(sourceObject *self, PyObject *noargs)
{
    /* frees result if necessary and invalidates object */
    if (self->result) {
        PQclear(self->result);
        self->result_type = RESULT_EMPTY;
        self->result = NULL;
    }

    self->valid = 0;

    /* return None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* Database query. */
static char source_execute__doc__[] =
"execute(sql) -- execute a SQL statement (string)\n\n"
"On success, this call returns the number of affected rows, or None\n"
"for DQL (SELECT, ...) statements.  The fetch (fetch(), fetchone()\n"
"and fetchall()) methods can be used to get result rows.\n";

static PyObject *
source_execute(sourceObject *self, PyObject *sql)
{
    PyObject *tmp_obj = NULL;  /* auxiliary string object */
    char *query;
    int encoding;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX)) {
        return NULL;
    }

    encoding = PQclientEncoding(self->pgcnx->cnx);

    if (PyBytes_Check(sql)) {
        query = PyBytes_AsString(sql);
    }
    else if (PyUnicode_Check(sql)) {
        tmp_obj = get_encoded_string(sql, encoding);
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        query = PyBytes_AsString(tmp_obj);
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method execute() expects a string as argument");
        return NULL;
    }

    /* frees previous result */
    if (self->result) {
        PQclear(self->result);
        self->result = NULL;
    }
    self->max_row = 0;
    self->current_row = 0;
    self->num_fields = 0;
    self->encoding = encoding;

    /* gets result */
    Py_BEGIN_ALLOW_THREADS
    self->result = PQexec(self->pgcnx->cnx, query);
    Py_END_ALLOW_THREADS

    /* we don't need the auxiliary string any more */
    Py_XDECREF(tmp_obj);

    /* checks result validity */
    if (!self->result) {
        PyErr_SetString(PyExc_ValueError, PQerrorMessage(self->pgcnx->cnx));
        return NULL;
    }

    /* this may have changed the datestyle, so we reset the date format
       in order to force fetching it newly when next time requested */
    self->pgcnx->date_format = date_format; /* this is normally NULL */

    /* checks result status */
    switch (PQresultStatus(self->result)) {
        /* query succeeded */
        case PGRES_TUPLES_OK:   /* DQL: returns None (DB-SIG compliant) */
            self->result_type = RESULT_DQL;
            self->max_row = PQntuples(self->result);
            self->num_fields = PQnfields(self->result);
            Py_INCREF(Py_None);
            return Py_None;
        case PGRES_COMMAND_OK:  /* other requests */
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
            {
                long num_rows;
                char *tmp;

                tmp = PQcmdTuples(self->result);
                if (tmp[0]) {
                    self->result_type = RESULT_DML;
                    num_rows = atol(tmp);
                }
                else {
                    self->result_type = RESULT_DDL;
                    num_rows = -1;
                }
                return PyInt_FromLong(num_rows);
            }

        /* query failed */
        case PGRES_EMPTY_QUERY:
            PyErr_SetString(PyExc_ValueError, "Empty query");
            break;
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
        case PGRES_NONFATAL_ERROR:
            set_error(ProgrammingError, "Cannot execute command",
                self->pgcnx->cnx, self->result);
            break;
        default:
            set_error_msg(InternalError,
                          "Internal error: unknown result status");
    }

    /* frees result and returns error */
    PQclear(self->result);
    self->result = NULL;
    self->result_type = RESULT_EMPTY;
    return NULL;
}

/* Get oid status for last query (valid for INSERTs, 0 for other). */
static char source_oidstatus__doc__[] =
"oidstatus() -- return oid of last inserted row (if available)";

static PyObject *
source_oidstatus(sourceObject *self, PyObject *noargs)
{
    Oid oid;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT)) {
        return NULL;
    }

    /* retrieves oid status */
    if ((oid = PQoidValue(self->result)) == InvalidOid) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return PyInt_FromLong((long) oid);
}

/* Fetch rows from last result. */
static char source_fetch__doc__[] =
"fetch(num) -- return the next num rows from the last result in a list\n\n"
"If num parameter is omitted arraysize attribute value is used.\n"
"If size equals -1, all rows are fetched.\n";

static PyObject *
source_fetch(sourceObject *self, PyObject *args)
{
    PyObject *res_list;
    int i, k;
    long size;
#if IS_PY3
    int encoding;
#endif

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL | CHECK_CNX)) {
        return NULL;
    }

    /* checks args */
    size = self->arraysize;
    if (!PyArg_ParseTuple(args, "|l", &size)) {
        PyErr_SetString(PyExc_TypeError,
                        "fetch(num), with num (integer, optional)");
        return NULL;
    }

    /* seeks last line */
    /* limit size to be within the amount of data we actually have */
    if (size == -1 || (self->max_row - self->current_row) < size) {
        size = self->max_row - self->current_row;
    }

    /* allocate list for result */
    if (!(res_list = PyList_New(0))) return NULL;

#if IS_PY3
    encoding = self->encoding;
#endif

    /* builds result */
    for (i = 0, k = self->current_row; i < size; ++i, ++k) {
        PyObject *rowtuple;
        int j;

        if (!(rowtuple = PyTuple_New(self->num_fields))) {
            Py_DECREF(res_list); return NULL;
        }

        for (j = 0; j < self->num_fields; ++j) {
            PyObject *str;

            if (PQgetisnull(self->result, k, j)) {
                Py_INCREF(Py_None);
                str = Py_None;
            }
            else {
                char *s = PQgetvalue(self->result, k, j);
                Py_ssize_t size = PQgetlength(self->result, k, j);
#if IS_PY3
                if (PQfformat(self->result, j) == 0) { /* textual format */
                    str = get_decoded_string(s, size, encoding);
                    if (!str) /* cannot decode */
                        str = PyBytes_FromStringAndSize(s, size);
                }
                else
#endif
                str = PyBytes_FromStringAndSize(s, size);
            }
            PyTuple_SET_ITEM(rowtuple, j, str);
        }

        if (PyList_Append(res_list, rowtuple)) {
            Py_DECREF(rowtuple); Py_DECREF(res_list); return NULL;
        }
        Py_DECREF(rowtuple);
    }

    self->current_row = k;
    return res_list;
}

/* Change current row (internal wrapper for all "move" methods). */
static PyObject *
_source_move(sourceObject *self, int move)
{
    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL)) {
        return NULL;
    }

    /* changes the current row */
    switch (move) {
        case QUERY_MOVEFIRST:
            self->current_row = 0;
            break;
        case QUERY_MOVELAST:
            self->current_row = self->max_row - 1;
            break;
        case QUERY_MOVENEXT:
            if (self->current_row != self->max_row)
                ++self->current_row;
            break;
        case QUERY_MOVEPREV:
            if (self->current_row > 0)
                self->current_row--;
            break;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

/* Move to first result row. */
static char source_movefirst__doc__[] =
"movefirst() -- move to first result row";

static PyObject *
source_movefirst(sourceObject *self, PyObject *noargs)
{
    return _source_move(self, QUERY_MOVEFIRST);
}

/* Move to last result row. */
static char source_movelast__doc__[] =
"movelast() -- move to last valid result row";

static PyObject *
source_movelast(sourceObject *self, PyObject *noargs)
{
    return _source_move(self, QUERY_MOVELAST);
}

/* Move to next result row. */
static char source_movenext__doc__[] =
"movenext() -- move to next result row";

static PyObject *
source_movenext(sourceObject *self, PyObject *noargs)
{
    return _source_move(self, QUERY_MOVENEXT);
}

/* Move to previous result row. */
static char source_moveprev__doc__[] =
"moveprev() -- move to previous result row";

static PyObject *
source_moveprev(sourceObject *self, PyObject *noargs)
{
    return _source_move(self, QUERY_MOVEPREV);
}

/* Put copy data. */
static char source_putdata__doc__[] =
"putdata(buffer) -- send data to server during copy from stdin";

static PyObject *
source_putdata(sourceObject *self, PyObject *buffer)
{
    PyObject *tmp_obj = NULL;  /* an auxiliary object */
    char *buf;                 /* the buffer as encoded string */
    Py_ssize_t nbytes;         /* length of string */
    char *errormsg = NULL;     /* error message */
    int res;                   /* direct result of the operation */
    PyObject *ret;             /* return value */

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX)) {
        return NULL;
    }

    /* make sure that the connection object is valid */
    if (!self->pgcnx->cnx) {
        return NULL;
    }

    if (buffer == Py_None) {
        /* pass None for terminating the operation */
        buf = errormsg = NULL;
    }
    else if (PyBytes_Check(buffer)) {
        /* or pass a byte string */
        PyBytes_AsStringAndSize(buffer, &buf, &nbytes);
    }
    else if (PyUnicode_Check(buffer)) {
        /* or pass a unicode string */
        tmp_obj = get_encoded_string(
            buffer, PQclientEncoding(self->pgcnx->cnx));
        if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        PyBytes_AsStringAndSize(tmp_obj, &buf, &nbytes);
    }
    else if (PyErr_GivenExceptionMatches(buffer, PyExc_BaseException)) {
        /* or pass a Python exception for sending an error message */
        tmp_obj = PyObject_Str(buffer);
        if (PyUnicode_Check(tmp_obj)) {
            PyObject *obj = tmp_obj;

            tmp_obj = get_encoded_string(
                obj, PQclientEncoding(self->pgcnx->cnx));
            Py_DECREF(obj);
            if (!tmp_obj) return NULL; /* pass the UnicodeEncodeError */
        }
        errormsg = PyBytes_AsString(tmp_obj);
        buf = NULL;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "Method putdata() expects a buffer, None"
                        " or an exception as argument");
        return NULL;
    }

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX | CHECK_RESULT) ||
        PQresultStatus(self->result) != PGRES_COPY_IN)
    {
        PyErr_SetString(PyExc_IOError,
                        "Connection is invalid or not in copy_in state");
        Py_XDECREF(tmp_obj);
        return NULL;
    }

    if (buf) {
        res = nbytes ? PQputCopyData(self->pgcnx->cnx, buf, (int) nbytes) : 1;
    }
    else {
        res = PQputCopyEnd(self->pgcnx->cnx, errormsg);
    }

    Py_XDECREF(tmp_obj);

    if (res != 1) {
        PyErr_SetString(PyExc_IOError, PQerrorMessage(self->pgcnx->cnx));
        return NULL;
    }

    if (buf) { /* buffer has been sent */
        ret = Py_None;
        Py_INCREF(ret);
    }
    else { /* copy is done */
        PGresult *result; /* final result of the operation */

        Py_BEGIN_ALLOW_THREADS;
        result = PQgetResult(self->pgcnx->cnx);
        Py_END_ALLOW_THREADS;

        if (PQresultStatus(result) == PGRES_COMMAND_OK) {
            char *tmp;
            long num_rows;

            tmp = PQcmdTuples(result);
            num_rows = tmp[0] ? atol(tmp) : -1;
            ret = PyInt_FromLong(num_rows);
        }
        else {
            if (!errormsg) errormsg = PQerrorMessage(self->pgcnx->cnx);
            PyErr_SetString(PyExc_IOError, errormsg);
            ret = NULL;
        }

        PQclear(self->result);
        self->result = NULL;
        self->result_type = RESULT_EMPTY;
    }

    return ret; /* None or number of rows */
}

/* Get copy data. */
static char source_getdata__doc__[] =
"getdata(decode) -- receive data to server during copy to stdout";

static PyObject *
source_getdata(sourceObject *self, PyObject *args)
{
    int *decode = 0;    /* decode flag */
    char *buffer;       /* the copied buffer as encoded byte string */
    Py_ssize_t nbytes;  /* length of the byte string */
    PyObject *ret;      /* return value */

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX)) {
        return NULL;
    }

    /* make sure that the connection object is valid */
    if (!self->pgcnx->cnx) {
        return NULL;
    }

    if (!PyArg_ParseTuple(args, "|i", &decode)) {
        return NULL;
    }

    /* checks validity */
    if (!_check_source_obj(self, CHECK_CNX | CHECK_RESULT) ||
        PQresultStatus(self->result) != PGRES_COPY_OUT)
    {
        PyErr_SetString(PyExc_IOError,
                        "Connection is invalid or not in copy_out state");
        return NULL;
    }

    nbytes = PQgetCopyData(self->pgcnx->cnx, &buffer, 0);

    if (!nbytes || nbytes < -1) { /* an error occurred */
        PyErr_SetString(PyExc_IOError, PQerrorMessage(self->pgcnx->cnx));
        return NULL;
    }

    if (nbytes == -1) { /* copy is done */
        PGresult *result; /* final result of the operation */

        Py_BEGIN_ALLOW_THREADS;
        result = PQgetResult(self->pgcnx->cnx);
        Py_END_ALLOW_THREADS;

        if (PQresultStatus(result) == PGRES_COMMAND_OK) {
            char *tmp;
            long num_rows;

            tmp = PQcmdTuples(result);
            num_rows = tmp[0] ? atol(tmp) : -1;
            ret = PyInt_FromLong(num_rows);
        }
        else {
            PyErr_SetString(PyExc_IOError, PQerrorMessage(self->pgcnx->cnx));
            ret = NULL;
        }

        PQclear(self->result);
        self->result = NULL;
        self->result_type = RESULT_EMPTY;
    }
    else { /* a row has been returned */
        ret = decode ? get_decoded_string(
                buffer, nbytes, PQclientEncoding(self->pgcnx->cnx)) :
            PyBytes_FromStringAndSize(buffer, nbytes);
        PQfreemem(buffer);
    }

    return ret; /* buffer or number of rows */
}

/* Find field number from string/integer (internal use only). */
static int
_source_fieldindex(sourceObject *self, PyObject *param, const char *usage)
{
    int num;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL))
        return -1;

    /* gets field number */
    if (PyStr_Check(param)) {
        num = PQfnumber(self->result, PyBytes_AsString(param));
    }
    else if (PyInt_Check(param)) {
        num = (int) PyInt_AsLong(param);
    }
    else {
        PyErr_SetString(PyExc_TypeError, usage);
        return -1;
    }

    /* checks field validity */
    if (num < 0 || num >= self->num_fields) {
        PyErr_SetString(PyExc_ValueError, "Unknown field");
        return -1;
    }

    return num;
}

/* Build field information from position (internal use only). */
static PyObject *
_source_buildinfo(sourceObject *self, int num)
{
    PyObject *result;

    /* allocates tuple */
    result = PyTuple_New(5);
    if (!result) {
        return NULL;
    }

    /* affects field information */
    PyTuple_SET_ITEM(result, 0, PyInt_FromLong(num));
    PyTuple_SET_ITEM(result, 1,
        PyStr_FromString(PQfname(self->result, num)));
    PyTuple_SET_ITEM(result, 2,
        PyInt_FromLong((long) PQftype(self->result, num)));
    PyTuple_SET_ITEM(result, 3,
        PyInt_FromLong(PQfsize(self->result, num)));
    PyTuple_SET_ITEM(result, 4,
        PyInt_FromLong(PQfmod(self->result, num)));

    return result;
}

/* Lists fields info. */
static char source_listinfo__doc__[] =
"listinfo() -- get information for all fields (position, name, type oid)";

static PyObject *
source_listInfo(sourceObject *self, PyObject *noargs)
{
    PyObject *result, *info;
    int i;

    /* checks validity */
    if (!_check_source_obj(self, CHECK_RESULT | CHECK_DQL)) {
        return NULL;
    }

    /* builds result */
    if (!(result = PyTuple_New(self->num_fields))) {
        return NULL;
    }

    for (i = 0; i < self->num_fields; ++i) {
        info = _source_buildinfo(self, i);
        if (!info) {
            Py_DECREF(result);
            return NULL;
        }
        PyTuple_SET_ITEM(result, i, info);
    }

    /* returns result */
    return result;
}

/* List fields information for last result. */
static char source_fieldinfo__doc__[] =
"fieldinfo(desc) -- get specified field info (position, name, type oid)";

static PyObject *
source_fieldinfo(sourceObject *self, PyObject *desc)
{
    int num;

    /* checks args and validity */
    if ((num = _source_fieldindex(
        self, desc,
        "Method fieldinfo() needs a string or integer as argument")) == -1)
    {
        return NULL;
    }

    /* returns result */
    return _source_buildinfo(self, num);
}

/* Retrieve field value. */
static char source_field__doc__[] =
"field(desc) -- return specified field value";

static PyObject *
source_field(sourceObject *self, PyObject *desc)
{
    int num;

    /* checks args and validity */
    if ((num = _source_fieldindex(
        self, desc,
        "Method field() needs a string or integer as argument")) == -1)
    {
        return NULL;
    }

    return PyStr_FromString(
        PQgetvalue(self->result, self->current_row, num));
}

/* Get the list of source object attributes. */
static PyObject *
source_dir(connObject *self, PyObject *noargs)
{
    PyObject *attrs;

    attrs = PyObject_Dir(PyObject_Type((PyObject *) self));
    PyObject_CallMethod(
        attrs, "extend", "[sssss]",
        "pgcnx", "arraysize", "resulttype", "ntuples", "nfields");

    return attrs;
}

/* Source object methods */
static PyMethodDef source_methods[] = {
    {"__dir__", (PyCFunction) source_dir, METH_NOARGS, NULL},

    {"close", (PyCFunction) source_close,
        METH_NOARGS, source_close__doc__},
    {"execute", (PyCFunction) source_execute,
        METH_O, source_execute__doc__},
    {"oidstatus", (PyCFunction) source_oidstatus,
        METH_NOARGS, source_oidstatus__doc__},
    {"fetch", (PyCFunction) source_fetch,
        METH_VARARGS, source_fetch__doc__},
    {"movefirst", (PyCFunction) source_movefirst,
        METH_NOARGS, source_movefirst__doc__},
    {"movelast", (PyCFunction) source_movelast,
        METH_NOARGS, source_movelast__doc__},
    {"movenext", (PyCFunction) source_movenext,
        METH_NOARGS, source_movenext__doc__},
    {"moveprev", (PyCFunction) source_moveprev,
        METH_NOARGS, source_moveprev__doc__},
    {"putdata", (PyCFunction) source_putdata,
        METH_O, source_putdata__doc__},
    {"getdata", (PyCFunction) source_getdata,
        METH_VARARGS, source_getdata__doc__},
    {"field", (PyCFunction) source_field,
        METH_O, source_field__doc__},
    {"fieldinfo", (PyCFunction) source_fieldinfo,
        METH_O, source_fieldinfo__doc__},
    {"listinfo", (PyCFunction) source_listInfo,
        METH_NOARGS, source_listinfo__doc__},
    {NULL, NULL}
};

static char source__doc__[] = "PyGreSQL source object";

/* Source type definition */
static PyTypeObject sourceType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pgdb.Source",                  /* tp_name */
    sizeof(sourceObject),           /* tp_basicsize */
    0,                              /* tp_itemsize */
    /* methods */
    (destructor) source_dealloc,    /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    (setattrfunc) source_setattr,   /* tp_setattr */
    0,                              /* tp_compare */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    0,                              /* tp_call */
    (reprfunc) source_str,          /* tp_str */
    (getattrofunc) source_getattr,  /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    source__doc__,                  /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    source_methods,                 /* tp_methods */
};

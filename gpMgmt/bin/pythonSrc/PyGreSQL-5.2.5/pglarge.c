/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * Large object support - this file is part a of the C extension module.
 *
 * Copyright (c) 2023 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Deallocate large object. */
static void
large_dealloc(largeObject *self)
{
    /* Note: We do not try to close the large object here anymore,
       since the server automatically closes it at the end of the
       transaction in which it was created. So the object might already
       be closed, which will then cause error messages on the server.
       In other situations we might close the object too early here
       if the Python object falls out of scope but is still needed. */

    Py_XDECREF(self->pgcnx);
    PyObject_Del(self);
}

/* Return large object as string in human readable form. */
static PyObject *
large_str(largeObject *self)
{
    char str[80];
    sprintf(str, self->lo_fd >= 0 ?
            "Opened large object, oid %ld" :
            "Closed large object, oid %ld", (long) self->lo_oid);
    return PyStr_FromString(str);
}

/* Check validity of large object. */
static int
_check_lo_obj(largeObject *self, int level)
{
    if (!_check_cnx_obj(self->pgcnx))
        return 0;

    if (!self->lo_oid) {
        set_error_msg(IntegrityError, "Object is not valid (null oid)");
        return 0;
    }

    if (level & CHECK_OPEN) {
        if (self->lo_fd < 0) {
            PyErr_SetString(PyExc_IOError, "Object is not opened");
            return 0;
        }
    }

    if (level & CHECK_CLOSE) {
        if (self->lo_fd >= 0) {
            PyErr_SetString(PyExc_IOError, "Object is already opened");
            return 0;
        }
    }

    return 1;
}

/* Get large object attributes. */
static PyObject *
large_getattr(largeObject *self, PyObject *nameobj)
{
    const char *name = PyStr_AsString(nameobj);

    /* list postgreSQL large object fields */

    /* associated pg connection object */
    if (!strcmp(name, "pgcnx")) {
        if (_check_lo_obj(self, 0)) {
            Py_INCREF(self->pgcnx);
            return (PyObject *) (self->pgcnx);
        }
        PyErr_Clear();
        Py_INCREF(Py_None);
        return Py_None;
    }

    /* large object oid */
    if (!strcmp(name, "oid")) {
        if (_check_lo_obj(self, 0))
            return PyInt_FromLong((long) self->lo_oid);
        PyErr_Clear();
        Py_INCREF(Py_None);
        return Py_None;
    }

    /* error (status) message */
    if (!strcmp(name, "error"))
        return PyStr_FromString(PQerrorMessage(self->pgcnx->cnx));

    /* seeks name in methods (fallback) */
    return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* Get the list of large object attributes. */
static PyObject *
large_dir(largeObject *self, PyObject *noargs)
{
    PyObject *attrs;

    attrs = PyObject_Dir(PyObject_Type((PyObject *) self));
    PyObject_CallMethod(
        attrs, "extend", "[sss]", "oid", "pgcnx", "error");

    return attrs;
}

/* Open large object. */
static char large_open__doc__[] =
"open(mode) -- open access to large object with specified mode\n\n"
"The mode must be one of INV_READ, INV_WRITE (module level constants).\n";

static PyObject *
large_open(largeObject *self, PyObject *args)
{
    int mode, fd;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i", &mode)) {
        PyErr_SetString(PyExc_TypeError,
                        "The open() method takes an integer argument");
        return NULL;
    }

    /* check validity */
    if (!_check_lo_obj(self, CHECK_CLOSE)) {
        return NULL;
    }

    /* opens large object */
    if ((fd = lo_open(self->pgcnx->cnx, self->lo_oid, mode)) == -1) {
        PyErr_SetString(PyExc_IOError, "Can't open large object");
        return NULL;
    }
    self->lo_fd = fd;

    /* no error : returns Py_None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* Close large object. */
static char large_close__doc__[] =
"close() -- close access to large object data";

static PyObject *
large_close(largeObject *self, PyObject *noargs)
{
    /* checks validity */
    if (!_check_lo_obj(self, CHECK_OPEN)) {
        return NULL;
    }

    /* closes large object */
    if (lo_close(self->pgcnx->cnx, self->lo_fd)) {
        PyErr_SetString(PyExc_IOError, "Error while closing large object fd");
        return NULL;
    }
    self->lo_fd = -1;

    /* no error : returns Py_None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* Read from large object. */
static char large_read__doc__[] =
"read(size) -- read from large object to sized string\n\n"
"Object must be opened in read mode before calling this method.\n";

static PyObject *
large_read(largeObject *self, PyObject *args)
{
    int size;
    PyObject *buffer;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "i", &size)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method read() takes an integer argument");
        return NULL;
    }

    if (size <= 0) {
        PyErr_SetString(PyExc_ValueError,
                        "Method read() takes a positive integer as argument");
        return NULL;
    }

    /* checks validity */
    if (!_check_lo_obj(self, CHECK_OPEN)) {
        return NULL;
    }

    /* allocate buffer and runs read */
    buffer = PyBytes_FromStringAndSize((char *) NULL, size);

    if ((size = lo_read(self->pgcnx->cnx, self->lo_fd,
        PyBytes_AS_STRING((PyBytesObject *) (buffer)), (size_t) size)) == -1)
    {
        PyErr_SetString(PyExc_IOError, "Error while reading");
        Py_XDECREF(buffer);
        return NULL;
    }

    /* resize buffer and returns it */
    _PyBytes_Resize(&buffer, size);
    return buffer;
}

/* Write to large object. */
static char large_write__doc__[] =
"write(string) -- write sized string to large object\n\n"
"Object must be opened in read mode before calling this method.\n";

static PyObject *
large_write(largeObject *self, PyObject *args)
{
    char *buffer;
    int size;
    Py_ssize_t bufsize;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "s#", &buffer, &bufsize)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method write() expects a sized string as argument");
        return NULL;
    }

    /* checks validity */
    if (!_check_lo_obj(self, CHECK_OPEN)) {
        return NULL;
    }

    /* sends query */
    if ((size = lo_write(self->pgcnx->cnx, self->lo_fd, buffer,
                         (size_t) bufsize)) != bufsize)
    {
        PyErr_SetString(PyExc_IOError, "Buffer truncated during write");
        return NULL;
    }

    /* no error : returns Py_None */
    Py_INCREF(Py_None);
    return Py_None;
}

/* Go to position in large object. */
static char large_seek__doc__[] =
"seek(offset, whence) -- move to specified position\n\n"
"Object must be opened before calling this method. The whence option\n"
"can be SEEK_SET, SEEK_CUR or SEEK_END (module level constants).\n";

static PyObject *
large_seek(largeObject *self, PyObject *args)
{
    /* offset and whence are initialized to keep compiler happy */
    int ret, offset = 0, whence = 0;

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "ii", &offset, &whence)) {
        PyErr_SetString(PyExc_TypeError,
                        "Method lseek() expects two integer arguments");
        return NULL;
    }

    /* checks validity */
    if (!_check_lo_obj(self, CHECK_OPEN)) {
        return NULL;
    }

    /* sends query */
    if ((ret = lo_lseek(
        self->pgcnx->cnx, self->lo_fd, offset, whence)) == -1)
    {
        PyErr_SetString(PyExc_IOError, "Error while moving cursor");
        return NULL;
    }

    /* returns position */
    return PyInt_FromLong(ret);
}

/* Get large object size. */
static char large_size__doc__[] =
"size() -- return large object size\n\n"
"The object must be opened before calling this method.\n";

static PyObject *
large_size(largeObject *self, PyObject *noargs)
{
    int start, end;

    /* checks validity */
    if (!_check_lo_obj(self, CHECK_OPEN)) {
        return NULL;
    }

    /* gets current position */
    if ((start = lo_tell(self->pgcnx->cnx, self->lo_fd)) == -1) {
        PyErr_SetString(PyExc_IOError, "Error while getting current position");
        return NULL;
    }

    /* gets end position */
    if ((end = lo_lseek(self->pgcnx->cnx, self->lo_fd, 0, SEEK_END)) == -1) {
        PyErr_SetString(PyExc_IOError, "Error while getting end position");
        return NULL;
    }

    /* move back to start position */
    if ((start = lo_lseek(
        self->pgcnx->cnx, self->lo_fd, start, SEEK_SET)) == -1)
    {
        PyErr_SetString(PyExc_IOError,
                        "Error while moving back to first position");
        return NULL;
    }

    /* returns size */
    return PyInt_FromLong(end);
}

/* Get large object cursor position. */
static char large_tell__doc__[] =
"tell() -- give current position in large object\n\n"
"The object must be opened before calling this method.\n";

static PyObject *
large_tell(largeObject *self, PyObject *noargs)
{
    int start;

    /* checks validity */
    if (!_check_lo_obj(self, CHECK_OPEN)) {
        return NULL;
    }

    /* gets current position */
    if ((start = lo_tell(self->pgcnx->cnx, self->lo_fd)) == -1) {
        PyErr_SetString(PyExc_IOError, "Error while getting position");
        return NULL;
    }

    /* returns size */
    return PyInt_FromLong(start);
}

/* Export large object as unix file. */
static char large_export__doc__[] =
"export(filename) -- export large object data to specified file\n\n"
"The object must be closed when calling this method.\n";

static PyObject *
large_export(largeObject *self, PyObject *args)
{
    char *name;

    /* checks validity */
    if (!_check_lo_obj(self, CHECK_CLOSE)) {
        return NULL;
    }

    /* gets arguments */
    if (!PyArg_ParseTuple(args, "s", &name)) {
        PyErr_SetString(PyExc_TypeError,
                        "The method export() takes a filename as argument");
        return NULL;
    }

    /* runs command */
    if (lo_export(self->pgcnx->cnx, self->lo_oid, name) != 1) {
        PyErr_SetString(PyExc_IOError, "Error while exporting large object");
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

/* Delete a large object. */
static char large_unlink__doc__[] =
"unlink() -- destroy large object\n\n"
"The object must be closed when calling this method.\n";

static PyObject *
large_unlink(largeObject *self, PyObject *noargs)
{
    /* checks validity */
    if (!_check_lo_obj(self, CHECK_CLOSE)) {
        return NULL;
    }

    /* deletes the object, invalidate it on success */
    if (lo_unlink(self->pgcnx->cnx, self->lo_oid) != 1) {
        PyErr_SetString(PyExc_IOError, "Error while unlinking large object");
        return NULL;
    }
    self->lo_oid = 0;

    Py_INCREF(Py_None);
    return Py_None;
}

/* Large object methods */
static struct PyMethodDef large_methods[] = {
    {"__dir__", (PyCFunction) large_dir,  METH_NOARGS, NULL},
    {"open", (PyCFunction) large_open, METH_VARARGS, large_open__doc__},
    {"close", (PyCFunction) large_close, METH_NOARGS, large_close__doc__},
    {"read", (PyCFunction) large_read, METH_VARARGS, large_read__doc__},
    {"write", (PyCFunction) large_write, METH_VARARGS, large_write__doc__},
    {"seek", (PyCFunction) large_seek, METH_VARARGS, large_seek__doc__},
    {"size", (PyCFunction) large_size, METH_NOARGS, large_size__doc__},
    {"tell", (PyCFunction) large_tell, METH_NOARGS, large_tell__doc__},
    {"export",(PyCFunction) large_export, METH_VARARGS, large_export__doc__},
    {"unlink",(PyCFunction) large_unlink, METH_NOARGS, large_unlink__doc__},
    {NULL, NULL}
};

static char large__doc__[] = "PostgreSQL large object";

/* Large object type definition */
static PyTypeObject largeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pg.LargeObject",              /* tp_name */
    sizeof(largeObject),           /* tp_basicsize */
    0,                             /* tp_itemsize */

    /* methods */
    (destructor) large_dealloc,    /* tp_dealloc */
    0,                             /* tp_print */
    0,                             /* tp_getattr */
    0,                             /* tp_setattr */
    0,                             /* tp_compare */
    0,                             /* tp_repr */
    0,                             /* tp_as_number */
    0,                             /* tp_as_sequence */
    0,                             /* tp_as_mapping */
    0,                             /* tp_hash */
    0,                             /* tp_call */
    (reprfunc) large_str,          /* tp_str */
    (getattrofunc) large_getattr,  /* tp_getattro */
    0,                             /* tp_setattro */
    0,                             /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,            /* tp_flags */
    large__doc__,                  /* tp_doc */
    0,                             /* tp_traverse */
    0,                             /* tp_clear */
    0,                             /* tp_richcompare */
    0,                             /* tp_weaklistoffset */
    0,                             /* tp_iter */
    0,                             /* tp_iternext */
    large_methods,                 /* tp_methods */
};

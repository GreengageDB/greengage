/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * The notice object - this file is part a of the C extension module.
 *
 * Copyright (c) 2023 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* Get notice object attributes. */
static PyObject *
notice_getattr(noticeObject *self, PyObject *nameobj)
{
    PGresult const *res = self->res;
    const char *name = PyStr_AsString(nameobj);
    int fieldcode;

    if (!res) {
        PyErr_SetString(PyExc_TypeError, "Cannot get current notice");
        return NULL;
    }

    /* pg connection object */
    if (!strcmp(name, "pgcnx")) {
        if (self->pgcnx && _check_cnx_obj(self->pgcnx)) {
            Py_INCREF(self->pgcnx);
            return (PyObject *) self->pgcnx;
        }
        else {
            Py_INCREF(Py_None);
            return Py_None;
        }
    }

    /* full message */
    if (!strcmp(name, "message")) {
        return PyStr_FromString(PQresultErrorMessage(res));
    }

    /* other possible fields */
    fieldcode = 0;
    if (!strcmp(name, "severity"))
        fieldcode = PG_DIAG_SEVERITY;
    else if (!strcmp(name, "primary"))
        fieldcode = PG_DIAG_MESSAGE_PRIMARY;
    else if (!strcmp(name, "detail"))
        fieldcode = PG_DIAG_MESSAGE_DETAIL;
    else if (!strcmp(name, "hint"))
        fieldcode = PG_DIAG_MESSAGE_HINT;
    if (fieldcode) {
        char *s = PQresultErrorField(res, fieldcode);
        if (s) {
            return PyStr_FromString(s);
        }
        else {
            Py_INCREF(Py_None); return Py_None;
        }
    }

    return PyObject_GenericGetAttr((PyObject *) self, nameobj);
}

/* Get the list of notice attributes. */
static PyObject *
notice_dir(noticeObject *self, PyObject *noargs)
{
    PyObject *attrs;

    attrs = PyObject_Dir(PyObject_Type((PyObject *) self));
    PyObject_CallMethod(
        attrs, "extend", "[ssssss]",
        "pgcnx", "severity", "message", "primary", "detail", "hint");

    return attrs;
}

/* Return notice as string in human readable form. */
static PyObject *
notice_str(noticeObject *self)
{
    return notice_getattr(self, PyBytes_FromString("message"));
}

/* Notice object methods */
static struct PyMethodDef notice_methods[] = {
    {"__dir__", (PyCFunction) notice_dir,  METH_NOARGS, NULL},
    {NULL, NULL}
};

static char notice__doc__[] = "PostgreSQL notice object";

/* Notice type definition */
static PyTypeObject noticeType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pg.Notice",                    /* tp_name */
    sizeof(noticeObject),           /* tp_basicsize */
    0,                              /* tp_itemsize */
    /* methods */
    0,                              /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_compare */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    0,                              /* tp_call */
    (reprfunc) notice_str,          /* tp_str */
    (getattrofunc) notice_getattr,  /* tp_getattro */
    PyObject_GenericSetAttr,        /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    notice__doc__,                  /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    notice_methods,                 /* tp_methods */
};

/*
 * PyGreSQL - a Python interface for the PostgreSQL database.
 *
 * Internal functions - this file is part a of the C extension module.
 *
 * Copyright (c) 2023 by the PyGreSQL Development Team
 *
 * Please see the LICENSE.TXT file for specific restrictions.
 */

/* PyGreSQL internal types */

/* Simple types */
#define PYGRES_INT 1
#define PYGRES_LONG 2
#define PYGRES_FLOAT 3
#define PYGRES_DECIMAL 4
#define PYGRES_MONEY 5
#define PYGRES_BOOL 6
/* Text based types */
#define PYGRES_TEXT 8
#define PYGRES_BYTEA 9
#define PYGRES_JSON 10
#define PYGRES_OTHER 11
/* Array types */
#define PYGRES_ARRAY 16

/* Shared functions for encoding and decoding strings */

static PyObject *
get_decoded_string(const char *str, Py_ssize_t size, int encoding)
{
    if (encoding == pg_encoding_utf8)
        return PyUnicode_DecodeUTF8(str, size, "strict");
    if (encoding == pg_encoding_latin1)
        return PyUnicode_DecodeLatin1(str, size, "strict");
    if (encoding == pg_encoding_ascii)
        return PyUnicode_DecodeASCII(str, size, "strict");
    /* encoding name should be properly translated to Python here */
    return PyUnicode_Decode(str, size,
        pg_encoding_to_char(encoding), "strict");
}

static PyObject *
get_encoded_string(PyObject *unicode_obj, int encoding)
{
    if (encoding == pg_encoding_utf8)
        return PyUnicode_AsUTF8String(unicode_obj);
    if (encoding == pg_encoding_latin1)
        return PyUnicode_AsLatin1String(unicode_obj);
    if (encoding == pg_encoding_ascii)
        return PyUnicode_AsASCIIString(unicode_obj);
    /* encoding name should be properly translated to Python here */
    return PyUnicode_AsEncodedString(unicode_obj,
        pg_encoding_to_char(encoding), "strict");
}

/* Helper functions */

/* Get PyGreSQL internal types for a PostgreSQL type. */
static int
get_type(Oid pgtype)
{
    int t;

    switch (pgtype) {
        /* simple types */

        case INT2OID:
        case INT4OID:
        case CIDOID:
        case OIDOID:
        case XIDOID:
            t = PYGRES_INT;
            break;

        case INT8OID:
            t = PYGRES_LONG;
            break;

        case FLOAT4OID:
        case FLOAT8OID:
            t = PYGRES_FLOAT;
            break;

        case NUMERICOID:
            t = PYGRES_DECIMAL;
            break;

        case CASHOID:
            t = decimal_point ? PYGRES_MONEY : PYGRES_TEXT;
            break;

        case BOOLOID:
            t = PYGRES_BOOL;
            break;

        case BYTEAOID:
            t = bytea_escaped ? PYGRES_TEXT : PYGRES_BYTEA;
            break;

        case JSONOID:
        case JSONBOID:
            t = jsondecode ? PYGRES_JSON : PYGRES_TEXT;
            break;

        case BPCHAROID:
        case CHAROID:
        case TEXTOID:
        case VARCHAROID:
        case NAMEOID:
        case REGTYPEOID:
            t = PYGRES_TEXT;
            break;

        /* array types */

        case INT2ARRAYOID:
        case INT4ARRAYOID:
        case CIDARRAYOID:
        case OIDARRAYOID:
        case XIDARRAYOID:
            t = array_as_text ? PYGRES_TEXT : (PYGRES_INT | PYGRES_ARRAY);
            break;

        case INT8ARRAYOID:
            t = array_as_text ? PYGRES_TEXT : (PYGRES_LONG | PYGRES_ARRAY);
            break;

        case FLOAT4ARRAYOID:
        case FLOAT8ARRAYOID:
            t = array_as_text ? PYGRES_TEXT : (PYGRES_FLOAT | PYGRES_ARRAY);
            break;

        case NUMERICARRAYOID:
            t = array_as_text ? PYGRES_TEXT : (PYGRES_DECIMAL | PYGRES_ARRAY);
            break;

        case MONEYARRAYOID:
            t = array_as_text ? PYGRES_TEXT : ((decimal_point ?
                PYGRES_MONEY : PYGRES_TEXT) | PYGRES_ARRAY);
            break;

        case BOOLARRAYOID:
            t = array_as_text ? PYGRES_TEXT : (PYGRES_BOOL | PYGRES_ARRAY);
            break;

        case BYTEAARRAYOID:
            t = array_as_text ? PYGRES_TEXT : ((bytea_escaped ?
                PYGRES_TEXT : PYGRES_BYTEA) | PYGRES_ARRAY);
            break;

        case JSONARRAYOID:
        case JSONBARRAYOID:
            t = array_as_text ? PYGRES_TEXT : ((jsondecode ?
                PYGRES_JSON : PYGRES_TEXT) | PYGRES_ARRAY);
            break;

        case BPCHARARRAYOID:
        case CHARARRAYOID:
        case TEXTARRAYOID:
        case VARCHARARRAYOID:
        case NAMEARRAYOID:
        case REGTYPEARRAYOID:
            t = array_as_text ? PYGRES_TEXT : (PYGRES_TEXT | PYGRES_ARRAY);
            break;

        default:
            t = PYGRES_OTHER;
    }

    return t;
}

/* Get PyGreSQL column types for all result columns. */
static int *
get_col_types(PGresult *result, int nfields)
{
    int *types, *t, j;

    if (!(types = PyMem_Malloc(sizeof(int) * (size_t) nfields))) {
        return (int*) PyErr_NoMemory();
    }

    for (j = 0, t = types; j < nfields; ++j) {
        *t++ = get_type(PQftype(result, j));
    }

    return types;
}

/* Cast a bytea encoded text based type to a Python object.
   This assumes the text is null-terminated character string. */
static PyObject *
cast_bytea_text(char *s)
{
    PyObject *obj;
    char *tmp_str;
    size_t str_len;

    /* this function should not be called when bytea_escaped is set */
    tmp_str = (char *) PQunescapeBytea((unsigned char*) s, &str_len);
    obj = PyBytes_FromStringAndSize(tmp_str, (Py_ssize_t) str_len);
    if (tmp_str) {
        PQfreemem(tmp_str);
    }
    return obj;
}

/* Cast a text based type to a Python object.
   This needs the character string, size and encoding. */
static PyObject *
cast_sized_text(char *s, Py_ssize_t size, int encoding, int type)
{
    PyObject *obj, *tmp_obj;
    char *tmp_str;
    size_t str_len;

    switch (type) { /* this must be the PyGreSQL internal type */

        case PYGRES_BYTEA:
            /* this type should not be passed when bytea_escaped is set */
            /* we need to add a null byte */
            tmp_str = (char *) PyMem_Malloc((size_t) size + 1);
            if (!tmp_str) {
                return PyErr_NoMemory();
            }
            memcpy(tmp_str, s, (size_t) size);
            s = tmp_str; *(s + size) = '\0';
            tmp_str = (char *) PQunescapeBytea((unsigned char*) s, &str_len);
            PyMem_Free(s);
            if (!tmp_str) return PyErr_NoMemory();
            obj = PyBytes_FromStringAndSize(tmp_str, (Py_ssize_t) str_len);
            if (tmp_str) {
                PQfreemem(tmp_str);
            }
            break;

        case PYGRES_JSON:
            /* this type should only be passed when jsondecode is set */
            obj = get_decoded_string(s, size, encoding);
            if (obj && jsondecode) { /* was able to decode */
                tmp_obj = obj;
                obj = PyObject_CallFunction(jsondecode, "(O)", obj);
                Py_DECREF(tmp_obj);
            }
            break;

        default:  /* PYGRES_TEXT */
#if IS_PY3
            obj = get_decoded_string(s, size, encoding);
            if (!obj) /* cannot decode */
#endif
            obj = PyBytes_FromStringAndSize(s, size);
    }

    return obj;
}

/* Cast an arbitrary type to a Python object using a callback function.
   This needs the character string, size, encoding, the Postgres type
   and the external typecast function to be called. */
static PyObject *
cast_other(char *s, Py_ssize_t size, int encoding, Oid pgtype,
           PyObject *cast_hook)
{
    PyObject *obj;

    obj = cast_sized_text(s, size, encoding, PYGRES_TEXT);

    if (cast_hook) {
        PyObject *tmp_obj = obj;
        obj = PyObject_CallFunction(cast_hook, "(OI)", obj, pgtype);
        Py_DECREF(tmp_obj);
    }
    return obj;
}

/* Cast a simple type to a Python object.
   This needs a character string representation with a given size. */
static PyObject *
cast_sized_simple(char *s, Py_ssize_t size, int type)
{
    PyObject *obj, *tmp_obj;
    char buf[64], *t;
    int i, j, n;

    switch (type) { /* this must be the PyGreSQL internal type */

        case PYGRES_INT:
            n = sizeof(buf) / sizeof(buf[0]) - 1;
            if ((int) size < n) {
                n = (int) size;
            }
            for (i = 0, t = buf; i < n; ++i) {
                *t++ = *s++;
            }
            *t = '\0';
            obj = PyInt_FromString(buf, NULL, 10);
            break;

        case PYGRES_LONG:
            n = sizeof(buf) / sizeof(buf[0]) - 1;
            if ((int) size < n) {
                n = (int) size;
            }
            for (i = 0, t = buf; i < n; ++i) {
                *t++ = *s++;
            }
            *t = '\0';
            obj = PyLong_FromString(buf, NULL, 10);
            break;

        case PYGRES_FLOAT:
            tmp_obj = PyStr_FromStringAndSize(s, size);
            obj = PyFloat_FromString(tmp_obj);
            Py_DECREF(tmp_obj);
            break;

        case PYGRES_MONEY:
            /* this type should only be passed when decimal_point is set */
            n = sizeof(buf) / sizeof(buf[0]) - 1;
            for (i = 0, j = 0; i < size && j < n; ++i, ++s) {
                if (*s >= '0' && *s <= '9') {
                    buf[j++] = *s;
                }
                else if (*s == decimal_point) {
                    buf[j++] = '.';
                }
                else if (*s == '(' || *s == '-') {
                    buf[j++] = '-';
                }
            }
            if (decimal) {
                buf[j] = '\0';
                obj = PyObject_CallFunction(decimal, "(s)", buf);
            }
            else {
                tmp_obj = PyStr_FromString(buf);
                obj = PyFloat_FromString(tmp_obj);
                Py_DECREF(tmp_obj);

            }
            break;

        case PYGRES_DECIMAL:
            tmp_obj = PyStr_FromStringAndSize(s, size);
            obj = decimal ? PyObject_CallFunctionObjArgs(
                decimal, tmp_obj, NULL) : PyFloat_FromString(tmp_obj);
            Py_DECREF(tmp_obj);
            break;

        case PYGRES_BOOL:
            /* convert to bool only if bool_as_text is not set */
            if (bool_as_text) {
                obj = PyStr_FromString(*s == 't' ? "t" : "f");
            }
            else {
                obj = *s == 't' ? Py_True : Py_False;
                Py_INCREF(obj);
            }
            break;

        default:
            /* other types should never be passed, use cast_sized_text */
            obj = PyStr_FromStringAndSize(s, size);
    }

    return obj;
}

/* Cast a simple type to a Python object.
   This needs a null-terminated character string representation. */
static PyObject *
cast_unsized_simple(char *s, int type)
{
    PyObject *obj, *tmp_obj;
    char buf[64];
    int j, n;

    switch (type) { /* this must be the PyGreSQL internal type */

        case PYGRES_INT:
            obj = PyInt_FromString(s, NULL, 10);
            break;

        case PYGRES_LONG:
            obj = PyLong_FromString(s, NULL, 10);
            break;

        case PYGRES_FLOAT:
            tmp_obj = PyStr_FromString(s);
            obj = PyFloat_FromString(tmp_obj);
            Py_DECREF(tmp_obj);
            break;

        case PYGRES_MONEY:
            /* this type should only be passed when decimal_point is set */
            n = sizeof(buf) / sizeof(buf[0]) - 1;
            for (j = 0; *s && j < n; ++s) {
                if (*s >= '0' && *s <= '9') {
                    buf[j++] = *s;
                }
                else if (*s == decimal_point) {
                    buf[j++] = '.';
                }
                else if (*s == '(' || *s == '-') {
                    buf[j++] = '-';
                }
            }
            buf[j] = '\0'; s = buf;
            /* FALLTHROUGH */ /* no break here */

        case PYGRES_DECIMAL:
            if (decimal) {
                obj = PyObject_CallFunction(decimal, "(s)", s);
            }
            else {
                tmp_obj = PyStr_FromString(s);
                obj = PyFloat_FromString(tmp_obj);
                Py_DECREF(tmp_obj);
            }
            break;

        case PYGRES_BOOL:
            /* convert to bool only if bool_as_text is not set */
            if (bool_as_text) {
                obj = PyStr_FromString(*s == 't' ? "t" : "f");
            }
            else {
                obj = *s == 't' ? Py_True : Py_False;
                Py_INCREF(obj);
            }
            break;

        default:
            /* other types should never be passed, use cast_sized_text */
            obj = PyStr_FromString(s);
    }

    return obj;
}

/* Quick case insensitive check if given sized string is null. */
#define STR_IS_NULL(s, n) (n == 4 && \
    (s[0] == 'n' || s[0] == 'N') && \
    (s[1] == 'u' || s[1] == 'U') && \
    (s[2] == 'l' || s[2] == 'L') && \
    (s[3] == 'l' || s[3] == 'L'))

/* Cast string s with size and encoding to a Python list,
   using the input and output syntax for arrays.
   Use internal type or cast function to cast elements.
   The parameter delim specifies the delimiter for the elements,
   since some types do not use the default delimiter of a comma. */
static PyObject *
cast_array(char *s, Py_ssize_t size, int encoding,
     int type, PyObject *cast, char delim)
{
    PyObject *result, *stack[MAX_ARRAY_DEPTH];
    char *end = s + size, *t;
    int depth, ranges = 0, level = 0;

    if (type) {
        type &= ~PYGRES_ARRAY; /* get the base type */
        if (!type) type = PYGRES_TEXT;
    }
    if (!delim) {
        delim = ',';
    }
    else if (delim == '{' || delim =='}' || delim=='\\') {
        PyErr_SetString(PyExc_ValueError, "Invalid array delimiter");
        return NULL;
    }

    /* strip blanks at the beginning */
    while (s != end && *s == ' ') ++s;
    if (*s == '[') { /* dimension ranges */
        int valid;

        for (valid = 0; !valid;) {
            if (s == end || *s++ != '[') break;
            while (s != end && *s == ' ') ++s;
            if (s != end && (*s == '+' || *s == '-')) ++s;
            if (s == end || *s < '0' || *s > '9') break;
            while (s != end && *s >= '0' && *s <= '9') ++s;
            if (s == end || *s++ != ':') break;
            if (s != end && (*s == '+' || *s == '-')) ++s;
            if (s == end || *s < '0' || *s > '9') break;
            while (s != end && *s >= '0' && *s <= '9') ++s;
            if (s == end || *s++ != ']') break;
            while (s != end && *s == ' ') ++s;
            ++ranges;
            if (s != end && *s == '=') {
                do ++s; while (s != end && *s == ' ');
                valid = 1;
            }
        }
        if (!valid) {
            PyErr_SetString(PyExc_ValueError, "Invalid array dimensions");
            return NULL;
        }
    }
    for (t = s, depth = 0; t != end && (*t == '{' || *t == ' '); ++t) {
        if (*t == '{') ++depth;
    }
    if (!depth) {
        PyErr_SetString(PyExc_ValueError,
                        "Array must start with a left brace");
        return NULL;
    }
    if (ranges && depth != ranges) {
        PyErr_SetString(PyExc_ValueError,
                        "Array dimensions do not match content");
        return NULL;
    }
    if (depth > MAX_ARRAY_DEPTH) {
        PyErr_SetString(PyExc_ValueError, "Array is too deeply nested");
        return NULL;
    }
    depth--; /* next level of parsing */
    result = PyList_New(0);
    if (!result) return NULL;
    do ++s; while (s != end && *s == ' ');
    /* everything is set up, start parsing the array */
    while (s != end) {
        if (*s == '}') {
            PyObject *subresult;

            if (!level) break; /* top level array ended */
            do ++s; while (s != end && *s == ' ');
            if (s == end) break; /* error */
            if (*s == delim) {
                do ++s; while (s != end && *s == ' ');
                if (s == end) break; /* error */
                if (*s != '{') {
                    PyErr_SetString(PyExc_ValueError,
                                    "Subarray expected but not found");
                    Py_DECREF(result); return NULL;
                }
            }
            else if (*s != '}') break; /* error */
            subresult = result;
            result = stack[--level];
            if (PyList_Append(result, subresult)) {
                Py_DECREF(result); return NULL;
            }
        }
        else if (level == depth) { /* we expect elements at this level */
            PyObject *element;
            char *estr;
            Py_ssize_t esize;
            int escaped = 0;

            if (*s == '{') {
                PyErr_SetString(PyExc_ValueError,
                                "Subarray found where not expected");
                Py_DECREF(result); return NULL;
            }
            if (*s == '"') { /* quoted element */
                estr = ++s;
                while (s != end && *s != '"') {
                    if (*s == '\\') {
                        ++s; if (s == end) break;
                        escaped = 1;
                    }
                    ++s;
                }
                esize = s - estr;
                do ++s; while (s != end && *s == ' ');
            }
            else { /* unquoted element */
                estr = s;
                /* can contain blanks inside */
                while (s != end && *s != '"' &&
                       *s != '{' && *s != '}' && *s != delim)
                {
                    if (*s == '\\') {
                        ++s; if (s == end) break;
                        escaped = 1;
                    }
                    ++s;
                }
                t = s; while (t > estr && *(t - 1) == ' ') --t;
                if (!(esize = t - estr)) {
                    s = end; break; /* error */
                }
                if (STR_IS_NULL(estr, esize)) /* NULL gives None */
                    estr = NULL;
            }
            if (s == end) break; /* error */
            if (estr) {
                if (escaped) {
                    char *r;
                    Py_ssize_t i;

                    /* create unescaped string */
                    t = estr;
                    estr = (char *) PyMem_Malloc((size_t) esize);
                    if (!estr) {
                        Py_DECREF(result); return PyErr_NoMemory();
                    }
                    for (i = 0, r = estr; i < esize; ++i) {
                        if (*t == '\\') ++t, ++i;
                        *r++ = *t++;
                    }
                    esize = r - estr;
                }
                if (type) { /* internal casting of base type */
                    if (type & PYGRES_TEXT)
                        element = cast_sized_text(estr, esize, encoding, type);
                    else
                        element = cast_sized_simple(estr, esize, type);
                }
                else { /* external casting of base type */
#if IS_PY3
                    element = encoding == pg_encoding_ascii ? NULL :
                        get_decoded_string(estr, esize, encoding);
                    if (!element) /* no decoding necessary or possible */
#endif
                    element = PyBytes_FromStringAndSize(estr, esize);
                    if (element && cast) {
                        PyObject *tmp = element;
                        element = PyObject_CallFunctionObjArgs(
                            cast, element, NULL);
                        Py_DECREF(tmp);
                    }
                }
                if (escaped) PyMem_Free(estr);
                if (!element) {
                    Py_DECREF(result); return NULL;
                }
            }
            else {
                Py_INCREF(Py_None); element = Py_None;
            }
            if (PyList_Append(result, element)) {
                Py_DECREF(element); Py_DECREF(result); return NULL;
            }
            Py_DECREF(element);
            if (*s == delim) {
                do ++s; while (s != end && *s == ' ');
                if (s == end) break; /* error */
            }
            else if (*s != '}') break; /* error */
        }
        else { /* we expect arrays at this level */
            if (*s != '{') {
                PyErr_SetString(PyExc_ValueError,
                                "Subarray must start with a left brace");
                Py_DECREF(result); return NULL;
            }
            do ++s; while (s != end && *s == ' ');
            if (s == end) break; /* error */
            stack[level++] = result;
            if (!(result = PyList_New(0))) return NULL;
        }
    }
    if (s == end || *s != '}') {
        PyErr_SetString(PyExc_ValueError,
                        "Unexpected end of array");
        Py_DECREF(result); return NULL;
    }
    do ++s; while (s != end && *s == ' ');
    if (s != end) {
        PyErr_SetString(PyExc_ValueError,
                        "Unexpected characters after end of array");
        Py_DECREF(result); return NULL;
    }
    return result;
}

/* Cast string s with size and encoding to a Python tuple.
   using the input and output syntax for composite types.
   Use array of internal types or cast function or sequence of cast
   functions to cast elements. The parameter len is the record size.
   The parameter delim can specify a delimiter for the elements,
   although composite types always use a comma as delimiter. */
static PyObject *
cast_record(char *s, Py_ssize_t size, int encoding,
     int *type, PyObject *cast, Py_ssize_t len, char delim)
{
    PyObject *result, *ret;
    char *end = s + size, *t;
    Py_ssize_t i;

    if (!delim) {
        delim = ',';
    }
    else if (delim == '(' || delim ==')' || delim=='\\') {
        PyErr_SetString(PyExc_ValueError, "Invalid record delimiter");
        return NULL;
    }

    /* strip blanks at the beginning */
    while (s != end && *s == ' ') ++s;
    if (s == end || *s != '(') {
        PyErr_SetString(PyExc_ValueError,
                        "Record must start with a left parenthesis");
        return NULL;
    }
    result = PyList_New(0);
    if (!result) return NULL;
    i = 0;
    /* everything is set up, start parsing the record */
    while (++s != end) {
        PyObject *element;

        if (*s == ')' || *s == delim) {
            Py_INCREF(Py_None); element = Py_None;
        }
        else {
            char *estr;
            Py_ssize_t esize;
            int quoted = 0, escaped = 0;

            estr = s;
            quoted = *s == '"';
            if (quoted) ++s;
            esize = 0;
            while (s != end) {
                if (!quoted && (*s == ')' || *s == delim))
                    break;
                if (*s == '"') {
                    ++s; if (s == end) break;
                    if (!(quoted && *s == '"')) {
                        quoted = !quoted; continue;
                    }
                }
                if (*s == '\\') {
                    ++s; if (s == end) break;
                }
                ++s, ++esize;
            }
            if (s == end) break; /* error */
            if (estr + esize != s) {
                char *r;

                escaped = 1;
                /* create unescaped string */
                t = estr;
                estr = (char *) PyMem_Malloc((size_t) esize);
                if (!estr) {
                    Py_DECREF(result); return PyErr_NoMemory();
                }
                quoted = 0;
                r = estr;
                while (t != s) {
                    if (*t == '"') {
                        ++t;
                        if (!(quoted && *t == '"')) {
                            quoted = !quoted; continue;
                        }
                    }
                    if (*t == '\\') ++t;
                    *r++ = *t++;
                }
            }
            if (type) { /* internal casting of element type */
                int etype = type[i];

                if (etype & PYGRES_ARRAY)
                    element = cast_array(
                        estr, esize, encoding, etype, NULL, 0);
                else if (etype & PYGRES_TEXT)
                    element = cast_sized_text(estr, esize, encoding, etype);
                else
                    element = cast_sized_simple(estr, esize, etype);
            }
            else { /* external casting of base type */
#if IS_PY3
                element = encoding == pg_encoding_ascii ? NULL :
                    get_decoded_string(estr, esize, encoding);
                if (!element) /* no decoding necessary or possible */
#endif
                element = PyBytes_FromStringAndSize(estr, esize);
                if (element && cast) {
                    if (len) {
                        PyObject *ecast = PySequence_GetItem(cast, i);

                        if (ecast) {
                            if (ecast != Py_None) {
                                PyObject *tmp = element;
                                element = PyObject_CallFunctionObjArgs(
                                    ecast, element, NULL);
                                Py_DECREF(tmp);
                            }
                        }
                        else {
                            Py_DECREF(element); element = NULL;
                        }
                    }
                    else {
                        PyObject *tmp = element;
                        element = PyObject_CallFunctionObjArgs(
                            cast, element, NULL);
                        Py_DECREF(tmp);
                    }
                }
            }
            if (escaped) PyMem_Free(estr);
            if (!element) {
                Py_DECREF(result); return NULL;
            }
        }
        if (PyList_Append(result, element)) {
            Py_DECREF(element); Py_DECREF(result); return NULL;
        }
        Py_DECREF(element);
        if (len) ++i;
        if (*s != delim) break; /* no next record */
        if (len && i >= len) {
            PyErr_SetString(PyExc_ValueError, "Too many columns");
            Py_DECREF(result); return NULL;
        }
    }
    if (s == end || *s != ')') {
        PyErr_SetString(PyExc_ValueError, "Unexpected end of record");
        Py_DECREF(result); return NULL;
    }
    do ++s; while (s != end && *s == ' ');
    if (s != end) {
        PyErr_SetString(PyExc_ValueError,
                        "Unexpected characters after end of record");
        Py_DECREF(result); return NULL;
    }
    if (len && i < len) {
        PyErr_SetString(PyExc_ValueError, "Too few columns");
        Py_DECREF(result); return NULL;
    }

    ret = PyList_AsTuple(result);
    Py_DECREF(result);
    return ret;
}

/* Cast string s with size and encoding to a Python dictionary.
   using the input and output syntax for hstore values. */
static PyObject *
cast_hstore(char *s, Py_ssize_t size, int encoding)
{
    PyObject *result;
    char *end = s + size;

    result = PyDict_New();

    /* everything is set up, start parsing the record */
    while (s != end) {
        char *key, *val;
        PyObject *key_obj, *val_obj;
        Py_ssize_t key_esc = 0, val_esc = 0, size;
        int quoted;

        while (s != end && *s == ' ') ++s;
        if (s == end) break;
        quoted = *s == '"';
        if (quoted) {
            key = ++s;
            while (s != end) {
                if (*s == '"') break;
                if (*s == '\\') {
                    if (++s == end) break;
                    ++key_esc;
                }
                ++s;
            }
            if (s == end) {
                PyErr_SetString(PyExc_ValueError, "Unterminated quote");
                Py_DECREF(result); return NULL;
            }
        }
        else {
            key = s;
            while (s != end) {
                if (*s == '=' || *s == ' ') break;
                if (*s == '\\') {
                    if (++s == end) break;
                    ++key_esc;
                }
                ++s;
            }
            if (s == key) {
                PyErr_SetString(PyExc_ValueError, "Missing key");
                Py_DECREF(result); return NULL;
            }
        }
        size = s - key - key_esc;
        if (key_esc) {
            char *r = key, *t;
            key = (char *) PyMem_Malloc((size_t) size);
            if (!key) {
                Py_DECREF(result); return PyErr_NoMemory();
            }
            t = key;
            while (r != s) {
                if (*r == '\\') {
                    ++r; if (r == s) break;
                }
                *t++ = *r++;
            }
        }
        key_obj = cast_sized_text(key, size, encoding, PYGRES_TEXT);
        if (key_esc) PyMem_Free(key);
        if (!key_obj) {
            Py_DECREF(result); return NULL;
        }
        if (quoted) ++s;
        while (s != end && *s == ' ') ++s;
        if (s == end || *s++ != '=' || s == end || *s++ != '>') {
            PyErr_SetString(PyExc_ValueError, "Invalid characters after key");
            Py_DECREF(key_obj); Py_DECREF(result); return NULL;
        }
        while (s != end && *s == ' ') ++s;
        quoted = *s == '"';
        if (quoted) {
            val = ++s;
            while (s != end) {
                if (*s == '"') break;
                if (*s == '\\') {
                    if (++s == end) break;
                    ++val_esc;
                }
                ++s;
            }
            if (s == end) {
                PyErr_SetString(PyExc_ValueError, "Unterminated quote");
                Py_DECREF(result); return NULL;
            }
        }
        else {
            val = s;
            while (s != end) {
                if (*s == ',' || *s == ' ') break;
                if (*s == '\\') {
                    if (++s == end) break;
                    ++val_esc;
                }
                ++s;
            }
            if (s == val) {
                PyErr_SetString(PyExc_ValueError, "Missing value");
                Py_DECREF(key_obj); Py_DECREF(result); return NULL;
            }
            if (STR_IS_NULL(val, s - val))
                val = NULL;
        }
        if (val) {
            size = s - val - val_esc;
            if (val_esc) {
                char *r = val, *t;
                val = (char *) PyMem_Malloc((size_t) size);
                if (!val) {
                    Py_DECREF(key_obj); Py_DECREF(result);
                    return PyErr_NoMemory();
                }
                t = val;
                while (r != s) {
                    if (*r == '\\') {
                        ++r; if (r == s) break;
                    }
                    *t++ = *r++;
                }
            }
            val_obj = cast_sized_text(val, size, encoding, PYGRES_TEXT);
            if (val_esc) PyMem_Free(val);
            if (!val_obj) {
                Py_DECREF(key_obj); Py_DECREF(result); return NULL;
            }
        }
        else {
            Py_INCREF(Py_None); val_obj = Py_None;
        }
        if (quoted) ++s;
        while (s != end && *s == ' ') ++s;
        if (s != end) {
            if (*s++ != ',') {
                PyErr_SetString(PyExc_ValueError,
                                "Invalid characters after val");
                Py_DECREF(key_obj); Py_DECREF(val_obj);
                Py_DECREF(result); return NULL;
            }
            while (s != end && *s == ' ') ++s;
            if (s == end) {
                PyErr_SetString(PyExc_ValueError, "Missing entry");
                Py_DECREF(key_obj); Py_DECREF(val_obj);
                Py_DECREF(result); return NULL;
            }
        }
        PyDict_SetItem(result, key_obj, val_obj);
        Py_DECREF(key_obj); Py_DECREF(val_obj);
    }
    return result;
}

/* Get appropriate error type from sqlstate. */
static PyObject *
get_error_type(const char *sqlstate)
{
    switch (sqlstate[0]) {
        case '0':
            switch (sqlstate[1]) {
                case 'A':
                    return NotSupportedError;
            }
            break;
        case '2':
            switch (sqlstate[1]) {
                case '0':
                case '1':
                    return ProgrammingError;
                case '2':
                    return DataError;
                case '3':
                    return IntegrityError;
                case '4':
                case '5':
                    return InternalError;
                case '6':
                case '7':
                case '8':
                    return OperationalError;
                case 'B':
                case 'D':
                case 'F':
                    return InternalError;
            }
            break;
        case '3':
            switch (sqlstate[1]) {
                case '4':
                    return OperationalError;
                case '8':
                case '9':
                case 'B':
                    return InternalError;
                case 'D':
                case 'F':
                    return ProgrammingError;
            }
            break;
        case '4':
            switch (sqlstate[1]) {
                case '0':
                    return OperationalError;
                case '2':
                case '4':
                    return ProgrammingError;
            }
            break;
        case '5':
        case 'H':
            return OperationalError;
        case 'F':
        case 'P':
        case 'X':
            return InternalError;
    }
    return DatabaseError;
}

/* Set database error message and sqlstate attribute. */
static void
set_error_msg_and_state(PyObject *type,
    const char *msg, int encoding, const char *sqlstate)
{
    PyObject *err_obj, *msg_obj, *sql_obj = NULL;

#if IS_PY3
    if (encoding == -1) /* unknown */
        msg_obj = PyUnicode_DecodeLocale(msg, NULL);
    else
        msg_obj = get_decoded_string(msg, (Py_ssize_t) strlen(msg), encoding);
    if (!msg_obj) /* cannot decode */
#endif
    msg_obj = PyBytes_FromString(msg);

    if (sqlstate) {
        sql_obj = PyStr_FromStringAndSize(sqlstate, 5);
    }
    else {
        Py_INCREF(Py_None); sql_obj = Py_None;
    }

    err_obj = PyObject_CallFunctionObjArgs(type, msg_obj, NULL);
    if (err_obj) {
        Py_DECREF(msg_obj);
        PyObject_SetAttrString(err_obj, "sqlstate", sql_obj);
        Py_DECREF(sql_obj);
        PyErr_SetObject(type, err_obj);
        Py_DECREF(err_obj);
    }
    else {
        PyErr_SetString(type, msg);
    }
}

/* Set given database error message. */
static void
set_error_msg(PyObject *type, const char *msg)
{
    set_error_msg_and_state(type, msg, pg_encoding_ascii, NULL);
}

/* Set database error from connection and/or result. */
static void
set_error(PyObject *type, const char * msg, PGconn *cnx, PGresult *result)
{
    char *sqlstate = NULL;
    int encoding = pg_encoding_ascii;

    if (cnx) {
        char *err_msg = PQerrorMessage(cnx);
        if (err_msg) {
            msg = err_msg;
            encoding = PQclientEncoding(cnx);
        }
    }
    if (result) {
        sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
        if (sqlstate) type = get_error_type(sqlstate);
    }

    set_error_msg_and_state(type, msg, encoding, sqlstate);
}

#ifdef SSL_INFO

/* Get SSL attributes and values as a dictionary. */
static PyObject *
get_ssl_attributes(PGconn *cnx) {
    PyObject *attr_dict = NULL;
    const char * const *s;

    if (!(attr_dict = PyDict_New())) {
        return NULL;
    }

    for (s = PQsslAttributeNames(cnx); *s; ++s) {
        const char *val = PQsslAttribute(cnx, *s);

        if (val) {
            PyObject * val_obj = PyStr_FromString(val);

            PyDict_SetItemString(attr_dict, *s, val_obj);
            Py_DECREF(val_obj);
        }
        else {
            PyDict_SetItemString(attr_dict, *s, Py_None);
        }
    }

    return attr_dict;
}

#endif /* SSL_INFO */

/* Format result (mostly useful for debugging).
   Note: This is similar to the Postgres function PQprint().
   PQprint() is not used because handing over a stream from Python to
   PostgreSQL can be problematic if they use different libs for streams
   and because using PQprint() and tp_print is not recommended any more. */
static PyObject *
format_result(const PGresult *res)
{
    const int n = PQnfields(res);

    if (n > 0) {
        char * const aligns = (char *) PyMem_Malloc(
            (unsigned int) n * sizeof(char));
        size_t * const sizes = (size_t *) PyMem_Malloc(
            (unsigned int) n * sizeof(size_t));

        if (aligns && sizes) {
            const int m = PQntuples(res);
            int i, j;
            size_t size;
            char *buffer;

            /* calculate sizes and alignments */
            for (j = 0; j < n; ++j) {
                const char * const s = PQfname(res, j);
                const int format = PQfformat(res, j);

                sizes[j] = s ? strlen(s) : 0;
                if (format) {
                    aligns[j] = '\0';
                    if (m && sizes[j] < 8)
                        /* "<binary>" must fit */
                        sizes[j] = 8;
                }
                else {
                    const Oid ftype = PQftype(res, j);

                    switch (ftype) {
                        case INT2OID:
                        case INT4OID:
                        case INT8OID:
                        case FLOAT4OID:
                        case FLOAT8OID:
                        case NUMERICOID:
                        case OIDOID:
                        case XIDOID:
                        case CIDOID:
                        case CASHOID:
                            aligns[j] = 'r';
                            break;
                        default:
                            aligns[j] = 'l';
                    }
                }
            }
            for (i = 0; i < m; ++i) {
                for (j = 0; j < n; ++j) {
                    if (aligns[j]) {
                        const int k = PQgetlength(res, i, j);

                        if (sizes[j] < (size_t) k)
                            /* value must fit */
                            sizes[j] = (size_t) k;
                    }
                }
            }
            size = 0;
            /* size of one row */
            for (j = 0; j < n; ++j) size += sizes[j] + 1;
            /* times number of rows incl. heading */
            size *= (size_t) m + 2;
            /* plus size of footer */
            size += 40;
            /* is the buffer size that needs to be allocated */
            buffer = (char *) PyMem_Malloc(size);
            if (buffer) {
                char *p = buffer;
                PyObject *result;

                /* create the header */
                for (j = 0; j < n; ++j) {
                    const char * const s = PQfname(res, j);
                    const size_t k = sizes[j];
                    const size_t h = (k - (size_t) strlen(s)) / 2;

                    sprintf(p, "%*s", (int) h, "");
                    sprintf(p + h, "%-*s", (int) (k - h), s);
                    p += k;
                    if (j + 1 < n)
                        *p++ = '|';
                }
                *p++ = '\n';
                for (j = 0; j < n; ++j) {
                    size_t k = sizes[j];

                    while (k--)
                        *p++ = '-';
                    if (j + 1 < n)
                        *p++ = '+';
                }
                *p++ = '\n';
                /* create the body */
                for (i = 0; i < m; ++i) {
                    for (j = 0; j < n; ++j) {
                        const char align = aligns[j];
                        const size_t k = sizes[j];

                        if (align) {
                            sprintf(p, align == 'r' ? "%*s" : "%-*s", (int) k,
                                    PQgetvalue(res, i, j));
                        }
                        else {
                            sprintf(p, "%-*s", (int) k,
                                    PQgetisnull(res, i, j) ? "" : "<binary>");
                        }
                        p += k;
                        if (j + 1 < n)
                            *p++ = '|';
                    }
                    *p++ = '\n';
                }
                /* free memory */
                PyMem_Free(aligns); PyMem_Free(sizes);
                /* create the footer */
                sprintf(p, "(%d row%s)", m, m == 1 ? "" : "s");
                /* return the result */
                result = PyStr_FromString(buffer);
                PyMem_Free(buffer);
                return result;
            }
            else {
                PyMem_Free(aligns); PyMem_Free(sizes); return PyErr_NoMemory();
            }
        }
        else {
            PyMem_Free(aligns); PyMem_Free(sizes); return PyErr_NoMemory();
        }
    }
    else
        return PyStr_FromString("(nothing selected)");
}

/* Internal function converting a Postgres datestyles to date formats. */
static const char *
date_style_to_format(const char *s)
{
    static const char *formats[] =
    {
        "%Y-%m-%d",  /* 0 = ISO */
        "%m-%d-%Y",  /* 1 = Postgres, MDY */
        "%d-%m-%Y",  /* 2 = Postgres, DMY */
        "%m/%d/%Y",  /* 3 = SQL, MDY */
        "%d/%m/%Y",  /* 4 = SQL, DMY */
        "%d.%m.%Y"   /* 5 = German */
    };

    switch (s ? *s : 'I') {
        case 'P': /* Postgres */
            s = strchr(s + 1, ',');
            if (s) do ++s; while (*s && *s == ' ');
            return formats[s && *s == 'D' ? 2 : 1];
        case 'S': /* SQL */
            s = strchr(s + 1, ',');
            if (s) do ++s; while (*s && *s == ' ');
            return formats[s && *s == 'D' ? 4 : 3];
        case 'G': /* German */
            return formats[5];
        default: /* ISO */
            return formats[0]; /* ISO is the default */
    }
}

/* Internal function converting a date format to a Postgres datestyle. */
static const char *
date_format_to_style(const char *s)
{
    static const char *datestyle[] =
    {
        "ISO, YMD",         /* 0 = %Y-%m-%d */
        "Postgres, MDY",    /* 1 = %m-%d-%Y */
        "Postgres, DMY",    /* 2 = %d-%m-%Y */
        "SQL, MDY",         /* 3 = %m/%d/%Y */
        "SQL, DMY",         /* 4 = %d/%m/%Y */
        "German, DMY"       /* 5 = %d.%m.%Y */
    };

    switch (s ? s[1] : 'Y') {
        case 'm':
            switch (s[2]) {
                case '/':
                    return datestyle[3]; /* SQL, MDY */
                default:
                    return datestyle[1]; /* Postgres, MDY */
            }
        case 'd':
            switch (s[2]) {
                case '/':
                    return datestyle[4]; /* SQL, DMY */
                case '.':
                    return datestyle[5]; /* German */
                default:
                    return datestyle[2]; /* Postgres, DMY */
            }
        default:
            return datestyle[0]; /* ISO */
    }
}

/* Internal wrapper for the notice receiver callback. */
static void
notice_receiver(void *arg, const PGresult *res)
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    connObject *self = (connObject*) arg;
    PyObject *func = self->notice_receiver;

    if (func) {
        noticeObject *notice = PyObject_New(noticeObject, &noticeType);
        PyObject *ret;
        if (notice) {
            notice->pgcnx = arg;
            notice->res = res;
        }
        else {
            Py_INCREF(Py_None);
            notice = (noticeObject *)(void *) Py_None;
        }
        ret = PyObject_CallFunction(func, "(O)", notice);
        Py_XDECREF(ret);
    }
    PyGILState_Release(gstate);
}

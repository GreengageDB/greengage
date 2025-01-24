#
# Autoconf macros for configuring the build of Python extension modules
#
# config/python.m4
#

# PGAC_PATH_PYTHON
# ----------------
# Look for Python and set the output variable 'PYTHON' if found,
# fail otherwise.
#
# As the Python 3 transition happens and PEP 394 isn't updated, we
# need to cater to systems that don't have unversioned "python" by
# default.  Some systems ship with "python3" by default and perhaps
# have "python" in an optional package.  Some systems only have
# "python2" and "python3", in which case it's reasonable to prefer the
# newer version.
AC_DEFUN([PGAC_PATH_PYTHON],
[PGAC_PATH_PROGS(PYTHON, [python python2])
if test x"$PYTHON" = x""; then
  AC_MSG_ERROR([Python not found])
fi
])


# _PGAC_CHECK_PYTHON_DIRS
# -----------------------
# Determine the name of various directories of a given Python installation,
# as well as the Python version.
AC_DEFUN([_PGAC_CHECK_PYTHON_DIRS],
[AC_REQUIRE([PGAC_PATH_PYTHON])
python_fullversion=`${PYTHON} -c "import sys; print(sys.version)" | sed q`
AC_MSG_NOTICE([using python $python_fullversion])
# python_fullversion is typically n.n.n plus some trailing junk
python_majorversion=`echo "$python_fullversion" | sed '[s/^\([0-9]*\).*/\1/]'`
python_minorversion=`echo "$python_fullversion" | sed '[s/^[0-9]*\.\([0-9]*\).*/\1/]'`
python_version=`echo "$python_fullversion" | sed '[s/^\([0-9]*\.[0-9]*\).*/\1/]'`

AC_MSG_CHECKING([for Python distutils module])
if "${PYTHON}" -c 'import distutils' 2>&AS_MESSAGE_LOG_FD
then
    AC_MSG_RESULT(yes)
else
    AC_MSG_RESULT(no)
    AC_MSG_ERROR([distutils module not found])
fi
AC_MSG_CHECKING([Python configuration directory])
python_configdir=`${PYTHON} -c "import distutils.sysconfig; print(' '.join(filter(None,distutils.sysconfig.get_config_vars('LIBPL'))))"`
AC_MSG_RESULT([$python_configdir])

AC_MSG_CHECKING([Python include directories])
python_includespec=`${PYTHON} -c "import distutils.sysconfig; print('-I'+distutils.sysconfig.get_python_inc())"`
AC_MSG_RESULT([$python_includespec])

AC_SUBST(python_majorversion)[]dnl
AC_SUBST(python_version)[]dnl
AC_SUBST(python_includespec)[]dnl
])# _PGAC_CHECK_PYTHON_DIRS


# PGAC_CHECK_PYTHON_EMBED_SETUP
# -----------------------------
#
# Note: selecting libpython from python_configdir works in all Python
# releases, but it generally finds a non-shared library, which means
# that we are binding the python interpreter right into libplpython.so.
# In Python 2.3 and up there should be a shared library available in
# the main library location.
AC_DEFUN([PGAC_CHECK_PYTHON_EMBED_SETUP],
[AC_REQUIRE([_PGAC_CHECK_PYTHON_DIRS])
AC_MSG_CHECKING([how to link an embedded Python application])

python_libdir=`${PYTHON} -c "import distutils.sysconfig; print(' '.join(filter(None,distutils.sysconfig.get_config_vars('LIBDIR'))))"`
python_ldlibrary=`${PYTHON} -c "import distutils.sysconfig; print(' '.join(filter(None,distutils.sysconfig.get_config_vars('LDLIBRARY'))))"`
ldlibrary=`echo "${python_ldlibrary}" | sed -e 's/\.so$//' -e 's/\.dll$//' -e 's/\.dylib$//' -e 's/\.sl$//'`
python_enable_shared=`${PYTHON} -c "import distutils.sysconfig; print(distutils.sysconfig.get_config_vars().get('Py_ENABLE_SHARED',0))"`

if test x"${python_libdir}" != x"" -a x"${python_ldlibrary}" != x"" -a x"${python_ldlibrary}" != x"${ldlibrary}"
then
	# New way: use the official shared library
	ldlibrary=`echo "${ldlibrary}" | sed "s/^lib//"`
	# special for greengage... python was built in /opt/, but resides in the ext directory
	if test ! -d "${python_libdir}"
	then
		python_libdir=`echo "${python_configdir}" | sed "s/\/python2.7\/config//"`
	fi
	python_libspec="-L${python_libdir} -l${ldlibrary}"
else
	# Old way: use libpython from python_configdir
	python_libdir="${python_configdir}"
	# LDVERSION was introduced in Python 3.2.
	python_ldversion=`${PYTHON} -c "import distutils.sysconfig; print(' '.join(filter(None,distutils.sysconfig.get_config_vars('LDVERSION'))))"`
	if test x"${python_ldversion}" = x""; then
		python_ldversion=$python_version
	fi
	python_libspec="-L${python_libdir} -lpython${python_ldversion}"
fi

python_additional_libs=`${PYTHON} -c "import distutils.sysconfig; print(' '.join(filter(None,distutils.sysconfig.get_config_vars('LIBS','LIBC','LIBM','BASEMODLIBS'))))"`

AC_MSG_RESULT([${python_libspec} ${python_additional_libs}])

AC_SUBST(python_libdir)[]dnl
AC_SUBST(python_libspec)[]dnl
AC_SUBST(python_additional_libs)[]dnl
AC_SUBST(python_enable_shared)[]dnl

])# PGAC_CHECK_PYTHON_EMBED_SETUP

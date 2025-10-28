Installation
============

General
-------

You must first install Python and PostgreSQL on your system.
If you want to access remote databases only, you don't need to install
the full PostgreSQL server, but only the libpq C-interface library.
On Windows, this library is called ``libpq.dll`` and is for instance contained
in the PostgreSQL ODBC driver (search for "psqlodbc"). On Linux, it is called
``libpq.so`` and usually provided in a package called "libpq" or "libpq5".
On Windows, you also need to make sure that the directory that contains
``libpq.dll`` is part of your ``PATH`` environment variable.

The current version of PyGreSQL has been tested with Python versions
2.7 and 3.5 to 3.11, and PostgreSQL versions 9.0 to 9.6 and 10 to 15.

PyGreSQL will be installed as three modules, a shared library called
``_pg.so`` (on Linux) or a DLL called ``_pg.pyd`` (on Windows), and two pure
Python wrapper modules called ``pg.py`` and ``pgdb.py``.
All three files will be installed directly into the Python site-packages
directory. To uninstall PyGreSQL, simply remove these three files.


Installing with Pip
-------------------

This is the most easy way to install PyGreSQL if you have "pip" installed.
Just run the following command in your terminal::

  pip install PyGreSQL

This will automatically try to find and download a distribution on the
`Python Package Index <https://pypi.python.org/>`_ that matches your operating
system and Python version and install it.

Note that you still need to have the libpq interface installed on your system
(see the general remarks above).


Installing from a Binary Distribution
-------------------------------------

If you don't want to use "pip", or "pip" doesn't find an appropriate
distribution for your computer, you can also try to manually download
and install a distribution.

When you download the source distribution, you will need to compile the
C extension, for which you need a C compiler installed.
If you don't want to install a C compiler or avoid possible problems
with the compilation, you can search for a pre-compiled binary distribution
of PyGreSQL on the Python Package Index or the PyGreSQL homepage.

You can currently download PyGreSQL as Linux RPM, NetBSD package and Windows
installer. Make sure the required Python version of the binary package matches
the Python version you have installed.

Install the package as usual on your system.

Note that the documentation is currently only included in the source package.


Installing from Source
----------------------

If you want to install PyGreSQL from Source, or there is no binary
package available for your platform, follow these instructions.

Make sure the Python header files and PostgreSQL client and server header
files are installed. These come usually with the "devel" packages on Unix
systems and the installer executables on Windows systems.

If you are using a precompiled PostgreSQL, you will also need the pg_config
tool. This is usually also part of the "devel" package on Unix, and will be
installed as part of the database server feature on Windows systems.

Building and installing with Distutils
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can build and install PyGreSQL using
`Distutils <http://docs.python.org/install/>`_.

Download and unpack the PyGreSQL source tarball if you haven't already done so.

Type the following commands to build and install PyGreSQL::

    python setup.py install

Now you should be ready to use PyGreSQL.

You can also run the build step separately if you want to create a distribution
to be installed on a different system or explicitly enable or disable certain
features. For instance, in order to build PyGreSQL without support for the SSL
info functions, run::

    python setup.py build_ext --no-ssl-info

By default, PyGreSQL is compiled with support for all features available in the
installed PostgreSQL version, and you will get warnings for the features that
are not supported in this version. You can also explicitly require a feature in
order to get an error if it is not available, for instance:

    python setup.py build_ext --ssl-info

You can find out all possible build options with::

    python setup.py build_ext --help

Alternatively, you can also use the corresponding C preprocessor macros like
``SSL_INFO`` directly (see the next section).

Note that if you build PyGreSQL with support for newer features that are not
available in the PQLib installed on the runtime system, you may get an error
when importing PyGreSQL, since these features are missing in the shared library
which will prevent Python from loading it.

Compiling Manually
~~~~~~~~~~~~~~~~~~

The source file for compiling the C extension module is pgmodule.c.
You have two options. You can compile PyGreSQL as a stand-alone module
or you can build it into the Python interpreter.

Stand-Alone
^^^^^^^^^^^

* In the directory containing ``pgmodule.c``, run the following command::

    cc -fpic -shared -o _pg.so -I$PYINC -I$PGINC -I$PSINC -L$PGLIB -lpq pgmodule.c

  where you have to set::

    PYINC = path to the Python include files
            (usually something like /usr/include/python)
    PGINC = path to the PostgreSQL client include files
            (something like /usr/include/pgsql or /usr/include/postgresql)
    PSINC = path to the PostgreSQL server include files
            (like /usr/include/pgsql/server or /usr/include/postgresql/server)
    PGLIB = path to the PostgreSQL object code libraries (usually /usr/lib)

  If you are not sure about the above paths, try something like::

    PYINC=`find /usr -name Python.h`
    PGINC=`find /usr -name libpq-fe.h`
    PSINC=`find /usr -name postgres.h`
    PGLIB=`find /usr -name libpq.so`

  If you have the ``pg_config`` tool installed, you can set::

    PGINC=`pg_config --includedir`
    PSINC=`pg_config --includedir-server`
    PGLIB=`pg_config --libdir`

  Some options may be added to this line::

    -DDEFAULT_VARS      default variables support
    -DDIRECT_ACCESS     direct access methods
    -DLARGE_OBJECTS     large object support
    -DESCAPING_FUNCS    support for newer escaping functions
    -DPQLIB_INFO        support PQLib information
    -DSSL_INFO          support SSL information
    -DMEMORY_SIZE       support memory size function

  On some systems you may need to include ``-lcrypt`` in the list of libraries
  to make it compile.

* Test the new module. Something like the following should work::

    $ python

    >>> import _pg
    >>> db = _pg.connect('thilo','localhost')
    >>> db.query("INSERT INTO test VALUES ('ping','pong')")
    18304
    >>> db.query("SELECT * FROM test")
    eins|zwei
    ----+----
    ping|pong
    (1 row)

* Finally, move the ``_pg.so``, ``pg.py``, and ``pgdb.py`` to a directory in
  your ``PYTHONPATH``. A good place would be ``/usr/lib/python/site-packages``
  if your Python modules are in ``/usr/lib/python``.

Built-in to Python interpreter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Find the directory where your ``Setup`` file lives (usually in the ``Modules``
  subdirectory) in the Python source hierarchy and copy or symlink the
  ``pgmodule.c`` file there.

* Add the following line to your 'Setup' file::

    _pg  pgmodule.c -I$PGINC -I$PSINC -L$PGLIB -lpq

  where::

    PGINC = path to the PostgreSQL client include files (see above)
    PSINC = path to the PostgreSQL server include files (see above)
    PGLIB = path to the PostgreSQL object code libraries (see above)

  Some options may be added to this line::

    -DDEFAULT_VARS      default variables support
    -DDIRECT_ACCESS     direct access methods
    -DLARGE_OBJECTS     large object support
    -DESCAPING_FUNCS    support for newer escaping functions
    -DPQLIB_INFO        support PQLib information
    -DSSL_INFO          support SSL information
    -DMEMORY_SIZE       support memory size function

  On some systems you may need to include ``-lcrypt`` in the list of libraries
  to make it compile.

* If you want a shared module, make sure that the ``shared`` keyword is
  uncommented and add the above line below it. You used to need to install
  your shared modules with ``make sharedinstall`` but this no longer seems
  to be true.

* Copy ``pg.py`` to the lib directory where the rest of your modules are.
  For example, that's ``/usr/local/lib/Python`` on my system.

* Rebuild Python from the root directory of the Python source hierarchy by
  running ``make -f Makefile.pre.in boot`` and ``make && make install``.

* For more details read the documentation at the top of ``Makefile.pre.in``.

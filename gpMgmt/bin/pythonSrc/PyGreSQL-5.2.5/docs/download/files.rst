Distribution files
------------------

============== =

pgmodule.c     the main source file for the C extension module (_pg)
pgconn.c       the connection object
pginternal.c   internal functions
pglarge.c      large object support
pgnotice.c     the notice object
pgquery.c      the query object
pgsource.c     the source object

pgtypes.h      PostgreSQL type definitions
py3c.h         Python 2/3 compatibility layer for the C extension

pg.py          the "classic" PyGreSQL module
pgdb.py        a DB-SIG DB-API 2.0 compliant API wrapper for PyGreSQL

setup.py       the Python setup script

               To install PyGreSQL, you can run "python setup.py install".

setup.cfg      the Python setup configuration

docs/          documentation directory

               The documentation has been created with Sphinx.
               All text files are in ReST format; a HTML version of
               the documentation can be created with "make html".

tests/         a suite of unit tests for PyGreSQL

============== =

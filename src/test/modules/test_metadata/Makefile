# src/test/modules/test_metadata/Makefile

MODULE_big = test_metadata
PGFILEDESC = "test_metadata - test extension for custom protocol metadata"

EXTENSION = test_metadata
DATA = test_metadata--1.0.sql
CONTROL_FILE = test_metadata.control

OBJS = test_metadata.o $(WIN32RES)
REGRESS = test_metadata

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_metadata
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


installcheck: install
test: clean all install
	psql postgres -f sql/test_metadata.sql 2>&1

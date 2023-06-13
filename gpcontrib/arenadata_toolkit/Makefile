# gpcontrib/arenadata_toolkit/Makefile

MODULES = arenadata_toolkit

EXTENSION = arenadata_toolkit
DATA = arenadata_toolkit--1.0.sql

REGRESS = arenadata_toolkit_test arenadata_toolkit_skew_test adb_get_relfilenodes_test
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/arenadata_toolkit
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

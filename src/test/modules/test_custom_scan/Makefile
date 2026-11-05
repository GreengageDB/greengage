# src/test/modules/test_custom_scan/Makefile

MODULE_big = test_custom_scan
OBJS = test_custom_scan.o $(WIN32RES)
PGFILEDESC = "test_custom_scan - a custom scan provider for tests"

# setup loads the provider in every new backend of the cluster, teardown
# undoes it.
REGRESS = setup test_custom_scan teardown
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_custom_scan
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

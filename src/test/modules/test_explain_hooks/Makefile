# src/test/modules/test_explain_hooks/Makefile

MODULE_big = test_explain_hooks
OBJS = test_explain_hooks.o $(WIN32RES)
PGFILEDESC = "test_explain_hooks - an extension of EXPLAIN for tests"

# setup loads the module in every new backend of the test database,
# teardown takes it out again.
REGRESS = setup test_explain_hooks teardown
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_explain_hooks
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

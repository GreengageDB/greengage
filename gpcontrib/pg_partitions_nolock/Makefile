EXTENSION = pg_partitions_nolock
MODULES = pg_partitions_nolock

EXTENSION_VERSION = 1.0


DATA = pg_partitions_nolock--1.0.sql

OBJS = pg_get_expr_nolock.o

MODULE_big = pg_partitions_nolock


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gp_inject_fault
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

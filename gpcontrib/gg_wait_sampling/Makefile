# gpcontrib/gg_wait_sampling/Makefile

MODULE_big = gg_wait_sampling
OBJS = pg_wait_sampling.o collector.o

EXTENSION = gg_wait_sampling
DATA = gg_wait_sampling--1.1.sql

REGRESS = setup load queries gg_queries teardown

with_llvm = no
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gg_wait_sampling
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

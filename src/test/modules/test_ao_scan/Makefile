# src/test/modules/test_ao_scan/Makefile

MODULE_big = test_ao_scan
OBJS = test_ao_scan.o $(WIN32RES)
PGFILEDESC = "test_ao_scan - read append-only tables by blocks from an extension"

EXTENSION = test_ao_scan
DATA = test_ao_scan--1.0.sql

REGRESS = test_ao_scan

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_ao_scan
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# gpcontrib/gg_tables_tracking/Makefile

MODULES = gg_tables_tracking

EXTENSION = gg_tables_tracking
EXTENSION_VERSION = 1.0

DATA= gg_tables_tracking--1.0.sql

MODULE_big = gg_tables_tracking
OBJS = \
	src/gg_tables_tracking_guc.o \
	src/bloom.o \
	src/bloom_set.o \
	src/drops_track.o \
	src/file_hook.o \
	src/tf_shmem.o \
	src/gg_tables_tracking.o \
	src/gg_tables_tracking_worker.o \
	src/track_files.o \
	src/track_dbsize.o \
	
PG_CFLAGS = -I$(libpq_srcdir) -I$(CURDIR)/src/include

REGRESS = setup gg_tables_tracking_guc gg_tables_tracking teardown
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

with_llvm = no
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

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

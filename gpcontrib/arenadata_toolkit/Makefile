# gpcontrib/arenadata_toolkit/Makefile

MODULES = arenadata_toolkit

EXTENSION = arenadata_toolkit
EXTENSION_VERSION = 1.7
DATA = \
       arenadata_toolkit--1.0.sql \
       arenadata_toolkit--1.0--1.1.sql \
       arenadata_toolkit--1.1--1.2.sql \
       arenadata_toolkit--1.2--1.3.sql \
       arenadata_toolkit--1.3--1.4.sql \
       arenadata_toolkit--1.4--1.5.sql \
       arenadata_toolkit--1.5--1.6.sql \
       arenadata_toolkit--1.6--1.7.sql \

DATA_built = $(EXTENSION)--$(EXTENSION_VERSION).sql

MODULE_big = arenadata_toolkit
OBJS = \
	src/arenadata_toolkit_guc.o \
	src/bloom.o \
	src/bloom_set.o \
	src/drops_track.o \
	src/file_hook.o \
	src/tf_shmem.o \
	src/arenadata_toolkit.o \
	src/arenadata_toolkit_worker.o \
	src/track_files.o \
	src/dbsize.o \
	
PG_CFLAGS = -I$(libpq_srcdir) -I$(CURDIR)/src/include

REGRESS = arenadata_toolkit_test arenadata_toolkit_skew_test adb_get_relfilenodes_test \
          adb_collect_table_stats_test adb_vacuum_strategy_test adb_relation_storage_size_test \
          tablespace_location upgrade_test adb_hba_file_rules_view_test \
          arenadata_toolkit_guc arenadata_toolkit_tracking
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

$(DATA_built): $(DATA)
	cat $(DATA) > $(DATA_built)

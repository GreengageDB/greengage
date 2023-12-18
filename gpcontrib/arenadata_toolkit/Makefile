# gpcontrib/arenadata_toolkit/Makefile

MODULES = arenadata_toolkit

EXTENSION = arenadata_toolkit
EXTENSION_VERSION = 1.3
DATA = \
       arenadata_toolkit--1.0.sql \
       arenadata_toolkit--1.0--1.1.sql \
       arenadata_toolkit--1.1--1.2.sql \
       arenadata_toolkit--1.2--1.3.sql

DATA_built = $(EXTENSION)--$(EXTENSION_VERSION).sql

$(DATA_built): $(DATA)
	cat $(DATA) > $(DATA_built)

REGRESS = arenadata_toolkit_test arenadata_toolkit_skew_test adb_get_relfilenodes_test \
          adb_collect_table_stats_test adb_vacuum_strategy_test adb_relation_storage_size_test
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

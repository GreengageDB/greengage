# gpcontrib/temp_tables_stat/Makefile

MODULE_big = temp_tables_stat
OBJS = $(MODULE_big).o

EXTENSION = $(MODULE_big)
PGFILEDESC = "Collect and show statistics on temporary tables"

DATA = $(MODULE_big)--0.1.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/temp_tables_stat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

installcheck:
	$(pg_isolation2_regress_installcheck) --dbname=temp_tables_stat_isolation temp_tables_stat

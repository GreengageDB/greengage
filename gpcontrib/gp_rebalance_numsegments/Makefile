MODULE_big = gp_rebalance_numsegments

EXTENSION = gp_rebalance_numsegments
DATA = gp_rebalance_numsegments--1.0.sql

OBJS       = gp_rebalance_numsegments.o
PG_CPPFLAGS = -I$(libpq_srcdir)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/gp_rebalance_numsegments
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

installcheck:
	$(pg_isolation2_regress_installcheck) --dbname=gp_rebalance_numsegments_isolation rebalance_numsegments

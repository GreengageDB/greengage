EXTENSION = credcheck
EXTVERSION = $(shell grep default_version $(srcdir)/$(EXTENSION).control | \
	       sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

# Uncomment the following two lines to enable cracklib support, adapt the path
# to the cracklib dictionary following your distribution
#PG_CPPFLAGS = -DUSE_CRACKLIB '-DCRACKLIB_DICTPATH="/usr/lib/cracklib_dict"'
#SHLIB_LINK = -lcrack

PG_CPPFLAGS += -Wno-ignored-attributes -flto

MODULE_big = credcheck
OBJS = credcheck.o $(WIN32RES)
PGFILEDESC = "credcheck - postgresql credential checker"

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql

REGRESS_OPTS  = --inputdir=$(srcdir)/test --load-extension=credcheck \
	--init-file=$(top_srcdir)/src/test/regress/init_file --init-file=$(srcdir)/test/init_file
TESTS = setup 01_username 02_password 03_rename 04_alter_pwd \
	05_reuse_history 06_reuse_interval 07_valid_until \
	08_first_login 09_plpgsql 10_no_password_logging 11_coordinator_only \
	12_segment_password_logging \
	teardown

REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/credcheck
top_builddir = ../..
# Build without LTO, as the rest of the tree
PG_CPPFLAGS := $(filter-out -flto,$(PG_CPPFLAGS))
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

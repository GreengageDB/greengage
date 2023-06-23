EXTENSION = credcheck
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
	       sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

# Uncomment the following two lines to enable cracklib support, adapt the path
# to the cracklib dictionary following your distribution
#PG_CPPFLAGS = -DUSE_CRACKLIB '-DCRACKLIB_DICTPATH="/usr/lib/cracklib_dict"'
#SHLIB_LINK = -lcrack

MODULE_big = credcheck
OBJS = credcheck.o $(WIN32RES)
PGFILEDESC = "credcheck - postgresql credential checker"

DATA = $(wildcard updates/*--*.sql) $(EXTENSION)--$(EXTVERSION).sql

REGRESS_OPTS  = --inputdir=test --load-extension=credcheck
TESTS = 01_username 02_password 03_rename 04_alter_pwd

REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifeq ($(MAJORVERSION),$(filter $(MAJORVERSION), 14 15 16))
	REGRESS += 05_reuse_history
	REGRESS += 06_reuse_interval
else
	REGRESS += 05_pg13_reuse_history
	REGRESS += 06_pg13_reuse_interval
endif

REGRESS += 07_valid_until

ifdef $PG_CPPFLAGS

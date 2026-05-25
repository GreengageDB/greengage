EXTENSION = credcheck
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
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

REGRESS_OPTS  = --inputdir=test --load-extension=credcheck
TESTS = 01_username 02_password 03_rename 04_alter_pwd \
	05_reuse_history 06_reuse_interval 07_valid_until \
	08_first_login 09_plpgsql

REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))

PG_CONFIG = pg_config

# Extract the major version number
PG_MAJORVERSION := $(shell $(PG_CONFIG) --version | sed -e 's/^[a-zA-Z ]*//' -e 's/\..*//')

# Check if major version is greater than or equal to 15 to run tap test
ifeq ($(shell test $(PG_MAJORVERSION) -ge 15; echo $$?),0)
    TAP_TESTS = 1
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

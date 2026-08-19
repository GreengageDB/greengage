# src/test/modules/pg_cron/Makefile

EXTENSION = pg_cron

DATA_built = $(EXTENSION)--1.0.sql $(EXTENSION)--1.4.sql $(EXTENSION)--1.6.sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)

REGRESS_OPTS =--temp-config=./pg_cron.conf
REGRESS = pg_cron-test 

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
ifeq ($(CC),gcc)
    PG_CPPFLAGS = -std=gnu11 -Wall -Wextra -Werror -Wno-unused-parameter -Wno-uninitialized -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
else
    PG_CPPFLAGS = -std=gnu11 -Wall -Wextra -Werror -Wno-unused-parameter -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
endif
ifeq ($(shell uname -s),SunOS)
    PG_CPPFLAGS += -Wno-sign-compare -D__EXTENSIONS__
endif
SHLIB_LINK = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gp_error_handling
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@

$(EXTENSION)--1.4.sql: $(EXTENSION)--1.0.sql \
                       $(EXTENSION)--1.0--1.1.sql \
                       $(EXTENSION)--1.1--1.2.sql \
                       $(EXTENSION)--1.2--1.3.sql \
                       $(EXTENSION)--1.3--1.4.sql \
                       $(EXTENSION)--1.4--1.4-1.sql
	cat $^ > $@

$(EXTENSION)--1.6.sql: $(EXTENSION)--1.4.sql \
                       $(EXTENSION)--1.4-1--1.5.sql \
                       $(EXTENSION)--1.5--1.6.sql
	cat $^ > $@

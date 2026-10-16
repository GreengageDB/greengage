# gpcontrib/pg_cron/Makefile

EXTENSION = pg_cron

DATA_built = $(EXTENSION)--1.0.sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)

REGRESS = setup pg_cron-test teardown
REGRESS_OPTS = --init-file=$(top_builddir)/src/test/regress/init_file

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
SHLIB_LINK_INTERNAL = $(libpq)
EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/pg_cron
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@

subdir=src/bin/pg_upgrade
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=tablespace

override CPPFLAGS+= -I../ -I$(top_srcdir)/src/interfaces/libpq 
override LDFLAGS+= -Wl,--wrap=pg_fatal,--wrap=fopen,--wrap=fgets,--wrap=ferror,--wrap=fclose

include $(top_srcdir)/src/Makefile.mock

SRCS := $(wildcard ../*.c)
SRCS+= $(wildcard ../greengage/*.c)
SRCS := $(filter-out ../pg_upgrade.c, $(SRCS))
SRCS := $(filter-out ../tablespace.c, $(SRCS))
OBJS := $(SRCS:.c=.o)

tablespace.t: tablespace_test.o $(OBJS) $(CMOCKERY_OBJS)
	$(CC) $^ $(libpq_pgport) $(LDFLAGS) $(LIBS) $(libpq) -o $@

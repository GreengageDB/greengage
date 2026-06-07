MODULE_big = arrowflight
OBJS = \
	src/arrowflight.o \
	src/arrowflight_reader.o \
	src/arrowflight_fdw.o \
	src/arrowflight_common.o \
	src/arrowflight_fdw_write.o \
	src/arrowflight_fdw_writer.o

EXTENSION = arrowflight
DATA = arrowflight--1.0.sql

REGRESS = setup options write_planning teardown

SHLIB_LINK += -lstdc++
PG_CPPFLAGS += -I$(CURDIR)/src/include
PG_CXXFLAGS += -std=c++20

ifneq ($(filter 1 yes,$(USE_ARROW_FLIGHT) $(with_arrow_flight)),)
PG_CPPFLAGS += -DUSE_ARROW_FLIGHT $(shell pkg-config --cflags arrow arrow-flight)
SHLIB_LINK += $(shell pkg-config --libs arrow arrow-flight)
endif

# Avoid building LLVM Bitcode for this C++ extension module.
with_llvm = no

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/arrowflight
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

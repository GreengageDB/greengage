MODULE_big = arrowflight
.DEFAULT_GOAL := all

OBJS = \
	src/arrowflight.o \
	src/flightsql_deparse.o \
	src/flightsql_reader.o \
	src/flightsql_fdw.o \
	src/flightsql_mpp.o \
	src/arrowflight_common.o \
	src/arrowflight_resource.o \
	src/flightsql_writer.o

EXTENSION = arrowflight
DATA = arrowflight--1.0.sql
MPP_PROTO = src/proto/flightsql_mpp.proto
EXTRA_DIST = $(MPP_PROTO)

REGRESS = setup options write_planning teardown

SHLIB_LINK += -lstdc++
PG_CPPFLAGS += -I$(CURDIR)/src/include
PG_CXXFLAGS += -std=c++20

ifneq ($(filter 1 yes,$(USE_ARROW_FLIGHT) $(with_arrow_flight)),)
MPP_PROTO_DIR = src/generated
MPP_PROTO_CC = $(MPP_PROTO_DIR)/flightsql_mpp.pb.cc
MPP_PROTO_H = $(MPP_PROTO_DIR)/flightsql_mpp.pb.h
MPP_PROTO_STAMP = $(MPP_PROTO_DIR)/.flightsql_mpp.stamp

OBJS += src/generated/flightsql_mpp.pb.o
PG_CPPFLAGS += -DUSE_ARROW_FLIGHT \
	-I$(CURDIR)/$(MPP_PROTO_DIR) \
	$(shell pkg-config --cflags arrow arrow-flight arrow-flight-sql protobuf)
SHLIB_LINK += $(shell pkg-config --libs arrow arrow-flight arrow-flight-sql protobuf)

$(MPP_PROTO_STAMP): $(MPP_PROTO)
	$(MKDIR_P) $(MPP_PROTO_DIR)
	protoc --proto_path=$(dir $(MPP_PROTO)) \
		--cpp_out=$(MPP_PROTO_DIR) $(MPP_PROTO)
	touch $@

$(MPP_PROTO_CC) $(MPP_PROTO_H): $(MPP_PROTO_STAMP)

src/flightsql_mpp.o: $(MPP_PROTO_H)
src/generated/flightsql_mpp.pb.o: $(MPP_PROTO_CC) $(MPP_PROTO_H)

EXTRA_CLEAN += $(MPP_PROTO_CC) $(MPP_PROTO_H) $(MPP_PROTO_STAMP)
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

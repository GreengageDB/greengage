#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/fmgr
#
# IDENTIFICATION
#    src/backend/utils/fmgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/fmgr
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	deprecated.o \
	dfmgr.o \
	fmgr.o \
	funcapi.o

override CPPFLAGS += -DDLSUFFIX=\"$(DLSUFFIX)\"

include $(top_srcdir)/src/backend/common.mk

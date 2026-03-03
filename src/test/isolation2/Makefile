#
# Makefile for isolation2 tests
#

subdir = src/test/isolation2
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

NAME = isolation2_regress
OBJS = isolation2_regress.o workfile_mgr_test.o
include $(top_srcdir)/src/Makefile.shlib

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

ifeq ($(PORTNAME), win32)
LDLIBS += -lws2_32
endif

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) -I$(srcdir)/../regress $(CPPFLAGS)
override LDLIBS := $(libpq_pgport) $(LDLIBS)

all: pg_isolation2_regress$(X) all-lib data scan_flaky_fault_injectors test_parallel_retrieve_cursor_extended_query test_parallel_retrieve_cursor_extended_query_error

test_python:
	python helpers_test.py

test_parallel_retrieve_cursor_extended_query: test_parallel_retrieve_cursor_extended_query.c
	$(CC) $(CPPFLAGS) -I$(top_builddir)/src/interfaces/libpq -L$(GPHOME)/lib -L$(top_builddir)/src/interfaces/libpq  -o $@ $< -lpq

test_parallel_retrieve_cursor_extended_query_error: test_parallel_retrieve_cursor_extended_query_error.c
	$(CC) $(CPPFLAGS) -I$(top_builddir)/src/interfaces/libpq -L$(GPHOME)/lib -L$(top_builddir)/src/interfaces/libpq  -o $@ $< -lpq

pg_regress.o:
	$(MAKE) -C $(top_builddir)/src/test/regress pg_regress.o
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/pg_regress.o .

data:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/data

isolation2_regress.o: submake-libpgport

.PHONY: scan_flaky_fault_injectors
scan_flaky_fault_injectors:
	$(top_builddir)/src/test/regress/scan_flaky_fault_injectors.sh

pg_isolation2_regress$(X): isolation2_main.o pg_regress.o submake-libpgport submake-libpq
	$(CC) $(CFLAGS) $(filter %.o,$^) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

clean distclean:
	rm -f pg_isolation2_regress$(X) $(OBJS) isolation2_main.o isolation2_regress.so
	rm -f pg_regress.o test_parallel_retrieve_cursor_extended_query test_parallel_retrieve_cursor_extended_query_error
	rm -f data
	rm -rf $(pg_regress_clean_files)

install: all
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'
	$(INSTALL_PROGRAM) pg_isolation2_regress$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_isolation2_regress$(X)'
	$(INSTALL_PROGRAM) sql_isolation_testcase.py '$(DESTDIR)$(pgxsdir)/$(subdir)/sql_isolation_testcase.py'
	$(LN_S) -f '$(DESTDIR)$(pgxsdir)/src/test/regress/gpdiff.pl' '$(DESTDIR)$(pgxsdir)/$(subdir)/gpdiff.pl'
	$(LN_S) -f '$(DESTDIR)$(pgxsdir)/src/test/regress/gpstringsubs.pl' '$(DESTDIR)$(pgxsdir)/$(subdir)/gpstringsubs.pl'
	$(LN_S) -f '$(DESTDIR)$(pgxsdir)/src/test/regress/GPTest.pm' '$(DESTDIR)$(pgxsdir)/$(subdir)/GPTest.pm'
	$(LN_S) -f '$(DESTDIR)$(pgxsdir)/src/test/regress/atmsort.pm' '$(DESTDIR)$(pgxsdir)/$(subdir)/atmsort.pm'
	$(LN_S) -f '$(DESTDIR)$(pgxsdir)/src/test/regress/explain.pm' '$(DESTDIR)$(pgxsdir)/$(subdir)/explain.pm'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_isolation2_regress$(X)'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/sql_isolation_testcase.py'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/gpdiff.pl'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/gpstringsubs.pl'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/GPTest.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/atmsort.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/explain.pm'

installcheck: test_python installcheck-parallel-retrieve-cursor installcheck-ic-tcp installcheck-ic-proxy
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --schedule=$(srcdir)/isolation2_schedule

installcheck-resgroup: all-lib data
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_resgroup --dbname=isolation2resgrouptest --outputdir=resgroup --schedule=$(srcdir)/isolation2_resgroup_schedule

installcheck-parallel-retrieve-cursor: all-lib data test_parallel_retrieve_cursor_extended_query test_parallel_retrieve_cursor_extended_query_error
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_parallel_retrieve_cursor --dbname=isolation2parallelretrcursor --outputdir=parallelretrcursor --load-extension=gp_inject_fault --schedule=$(srcdir)/parallel_retrieve_cursor_schedule

installcheck-mirrorless: all-lib data
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --schedule=$(srcdir)/mirrorless_schedule --outputdir=mirrorless --dbname=isolation2-mirrorless --load-extension=gp_inject_fault

installcheck-ic-tcp: all-lib data
ifeq ($(findstring gp_interconnect_type=tcp,$(PGOPTIONS)),gp_interconnect_type=tcp)
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --outputdir=ic_tcp --load-extension=gp_inject_fault --schedule=$(srcdir)/isolation2_ic_tcp_schedule
endif

installcheck-ic-proxy: all-lib data
ifeq ($(findstring gp_interconnect_type=proxy,$(PGOPTIONS)),gp_interconnect_type=proxy)
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --outputdir=ic_proxy --load-extension=gp_inject_fault --schedule=$(srcdir)/isolation2_ic_proxy_schedule
endif

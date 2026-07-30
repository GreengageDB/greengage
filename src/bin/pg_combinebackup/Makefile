#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_combinebackup
#
# Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/pg_combinebackup/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_combinebackup - combine incremental backups"
PGAPPICON=win32

subdir = src/bin/pg_combinebackup
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils

OBJS = \
	$(WIN32RES) \
	pg_combinebackup.o \
	backup_label.o \
	copy_file.o \
	load_manifest.o \
	reconstruct.o \
	write_manifest.o

all: pg_combinebackup

pg_combinebackup: $(OBJS) | submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_combinebackup$(X) '$(DESTDIR)$(bindir)/pg_combinebackup$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_combinebackup$(X)'

clean distclean maintainer-clean:
	rm -f pg_combinebackup$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

# GPDB: the remaining test files exercise physical replication / standby
# promotion / backup / auth behavior GPDB reimplemented (see
# pg184_tap_runtime_triage); restrict installcheck to the GPDB-compatible files.
installcheck: PROVE_TESTS = t/001_basic.pl
installcheck:
	$(prove_installcheck)

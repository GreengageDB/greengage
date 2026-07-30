#-------------------------------------------------------------------------
#
# Makefile for src/test
#
# Copyright (c) 1994, Regents of the University of California
#
# src/test/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

# GPDB_12_MERGE_FEATURE_NOT_SUPPORTED Since logical replication doesn't work for GPDB,
# removed "subscription" test from below list.
SUBDIRS = perl postmaster regress isolation modules authentication recovery \
	  locale

SUBDIRS += fsync walrep heap_checksum isolation2 fdw

ifeq ($(with_icu),yes)
SUBDIRS += icu
endif
ifeq ($(with_gssapi),yes)
SUBDIRS += kerberos
endif
ifeq ($(with_ldap),yes)
SUBDIRS += ldap
endif
ifeq ($(with_ssl),openssl)
SUBDIRS += ssl
endif

# We don't build or execute these by default, but we do want "make
# clean" etc to recurse into them.  (We must filter out those that we
# have conditionally included into SUBDIRS above, else there will be
# make confusion.)
ALWAYS_SUBDIRS = $(filter-out $(SUBDIRS),examples kerberos icu ldap ssl)

# We want to recurse to all subdirs for all standard targets, except that
# installcheck and install should not recurse into the subdirectory "modules".

recurse_alldirs_targets := $(filter-out installcheck install, $(standard_targets))
installable_dirs := $(filter-out modules, $(SUBDIRS))

# GPDB: exclude upstream single-node TAP suites from installcheck-world. They
# exercise behavior GPDB deliberately reimplemented and have no meaningful GPDB
# coverage: standalone hot-standby promotion (recovery -- GPDB uses
# gpactivatestandby, and pg_ctl promote hits "sinval slot ... already in use"),
# MPP connection reservations vs. GPDB background workers (postmaster),
# single-node ICU collation defaults (icu), and external-server auth
# (authentication, kerberos, ssl). See pg184_tap_runtime_triage.
installcheck_dirs := $(filter-out postmaster recovery authentication icu kerberos ssl, $(installable_dirs))

$(call recurse,$(recurse_alldirs_targets))
$(call recurse,installcheck, $(installcheck_dirs))
$(call recurse,install, $(installable_dirs))

$(recurse_always)

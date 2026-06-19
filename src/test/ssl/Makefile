#-------------------------------------------------------------------------
#
# Makefile for src/test/ssl
#
# Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/ssl/Makefile
#
#-------------------------------------------------------------------------

EXTRA_INSTALL = contrib/sslinfo

subdir = src/test/ssl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

export with_ssl

# The sslfiles targets are separated into their own file due to interactions
# with settings in Makefile.global.
.PHONY: sslfiles sslfiles-clean
sslfiles sslfiles-clean:
	$(MAKE) -f $(srcdir)/sslfiles.mk $@

clean distclean maintainer-clean:
	rm -rf tmp_check
	$(MAKE) -f $(srcdir)/sslfiles.mk $@

# Doesn't depend on sslfiles because we don't rebuild them by default
check:
ifeq ($(with_ssl),openssl)
	$(prove_check)
else
	@echo "SSL is disabled for this build"
endif

install:
	./configure_ssl.sh

HOSTNAME=$(shell hostname)
installcheck: install
	$(prove_installcheck)
	# Always run clear_ssl.sh, even when pg_regress fails: leaving the
	# cluster SSL-enabled with cert-only authentication breaks every
	# suite that runs after this one.
	rc=0; $(top_builddir)/src/test/regress/pg_regress $(EXTRA_REGRESS_OPTS) --init-file=./init_file_ssl_connection --inputdir=$(srcdir) --dbname=test_sslconnection --schedule=$(srcdir)/ssl_connection_schedule --user=ssltestuser --host=$(HOSTNAME) --sslmode=verify-full || rc=$$?; ./clear_ssl.sh; exit $$rc

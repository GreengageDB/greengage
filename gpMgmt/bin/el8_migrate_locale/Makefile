# gpMgmt/bin/el8_migrate_locale/Makefile

top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)/el8_migrate_locale'

install: installdirs
	$(INSTALL_SCRIPT) el8_migrate_locale.py '$(DESTDIR)$(bindir)/el8_migrate_locale/';
	$(INSTALL_SCRIPT) README.md '$(DESTDIR)$(bindir)/el8_migrate_locale/';

uninstall:
	rm -rf '$(DESTDIR)$(bindir)/el8_migrate_locale/';

clean distclean:
	rm -f *.pyc

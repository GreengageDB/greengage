# gpMgmt/bin/ggrebalance_modules/Makefile

top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PROGRAMS= __init__.py shrink.py \
planner.py \
rebalance_commons.py \
rebalance_schema.py \
solver.py ggrebalance_sm.py \
ggrebalance_main_sm.py rebalance_step.py

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)/ggrebalance_modules'

install: installdirs
	for file in $(PROGRAMS); do \
		$(INSTALL_SCRIPT) $$file '$(DESTDIR)$(bindir)/ggrebalance_modules/'$$file ; \
	done

uninstall:
	for file in $(PROGRAMS); do \
		rm -f '$(DESTDIR)$(bindir)/ggrebalance_modules/'$$file ; \
	done

clean distclean:
	rm -f *.pyc

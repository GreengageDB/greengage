top_builddir = ..
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/gpMgmt/Makefile.behave

SUBDIRS= sbin bin doc

$(recurse)

generate_greengage_path_file:
	mkdir -p $(DESTDIR)$(prefix)
	unset LIBPATH; \
	bin/generate-greengage-path.sh > $(DESTDIR)$(prefix)/greengage_path.sh

install: generate_greengage_path_file
	mkdir -p $(DESTDIR)$(prefix)/lib/python

	# Setup /lib/python contents
	if [ -e bin/ext/__init__.py ]; then \
	    cp -rp bin/ext/__init__.py $(DESTDIR)$(prefix)/lib/python ; \
	fi
	if [ -e bin/ext/psutil ]; then \
	    cp -rp bin/ext/psutil $(DESTDIR)$(prefix)/lib/python ; \
	fi
	# Move _pg extension to sys.path root for Python 3 import (from _pg import *)
	if [ -e bin/ext/pygresql ]; then \
	    cp -rp bin/ext/pygresql $(DESTDIR)$(prefix)/lib/python ; \
	    sofile=`find "$(DESTDIR)$(prefix)/lib/python/pygresql" -maxdepth 1 -type f -name "_pg*.so" | head -n 1`; \
	    if [ -n "$$sofile" ]; then \
	        mv -f "$$sofile" "$(DESTDIR)$(prefix)/lib/python/"; \
	    fi; \
	fi
	if [ -e bin/ext/yaml ]; then \
	    cp -rp bin/ext/yaml $(DESTDIR)$(prefix)/lib/python ; \
	fi
	if [ -e bin/ext/subprocess32.py ]; then \
	    cp -p bin/ext/subprocess32.py $(DESTDIR)$(prefix)/lib/python ; \
	    cp -p bin/ext/subprocess32-ChangeLog $(DESTDIR)$(prefix)/lib/python ; \
	    cp -p bin/ext/_posixsubprocess.so $(DESTDIR)$(prefix)/lib/python ; \
	fi

coverage-setup:
	@echo "Setting up coverage instrumentation..."
	echo "import coverage; coverage.process_startup()" > \
	    $(DESTDIR)$(prefix)/lib/python/sitecustomize.py
	@if [ -z "$$COVERAGE_PROCESS_START" ]; then \
		echo "WARNING: COVERAGE_PROCESS_START is not set."; \
		echo "Coverage will NOT be collected from subprocesses."; \
		echo "Set it manually, e.g.:"; \
		echo "export COVERAGE_PROCESS_START=~/gpdb_src/gpMgmt/test/coveragerc"; \
	else \
		echo "COVERAGE_PROCESS_START=$$COVERAGE_PROCESS_START"; \
	fi

clean distclean: 
	$(MAKE) -C bin $@
	rm -f $(DESTDIR)$(prefix)/lib/python/sitecustomize.py
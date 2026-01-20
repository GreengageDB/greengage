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
	if [ -e bin/ext/pygresql ]; then \
		cp -rp bin/ext/pygresql $(DESTDIR)$(prefix)/lib/python ; \
		# Create top-level _pg symlink for Python 3 import (_pg must be on sys.path root) \
		sofile=`find "$(DESTDIR)$(prefix)/lib/python/pygresql" -maxdepth 1 -type f -name "_pg*.so" | head -n 1`; \
		if [ -n "$$sofile" ]; then \
			ln -sf "pygresql/$$(basename $$sofile)" \
					"$(DESTDIR)$(prefix)/lib/python/$$(basename $$sofile)"; \
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

clean distclean:
	$(MAKE) -C bin $@

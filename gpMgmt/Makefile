top_builddir = ..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/gpMgmt/Makefile.behave

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

clean distclean:
	$(MAKE) -C bin $@

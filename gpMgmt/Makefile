top_builddir = ..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/gpMgmt/Makefile.behave

SUBDIRS = sbin bin doc
PIP ?= python3 -m pip
PYLIB_TARGET_DIR = $(DESTDIR)$(prefix)/lib/python
PIP_TARGET = $(PYLIB_TARGET_DIR)/vendor

.ext-pip-deps: requirements.txt
	mkdir -p $(PIP_TARGET)
	$(PIP) install \
	--no-compile \
	--target $(PIP_TARGET) \
	-r $<
	touch $@

$(recurse)

generate_greengage_path_file:
	mkdir -p $(DESTDIR)$(prefix)
	unset LIBPATH; \
	bin/generate-greengage-path.sh > $(DESTDIR)$(prefix)/greengage_path.sh

install: generate_greengage_path_file .ext-pip-deps
	mkdir -p $(PYLIB_TARGET_DIR)

	# Setup /lib/python contents
	if [ -e bin/ext/__init__.py ]; then \
	    cp -rp bin/ext/__init__.py $(PYLIB_TARGET_DIR) ; \
	fi

clean distclean:
	rm -f .ext-pip-deps
	rm -rf $(PIP_TARGET)
	$(MAKE) -C bin $@

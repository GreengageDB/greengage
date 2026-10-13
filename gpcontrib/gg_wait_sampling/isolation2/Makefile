top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# The isolation2 harness sources this helper from the current directory; the
# retrieve-mode sessions of retrieve_tmid need its endpoint functions.
global_sh_executor.sh: $(top_builddir)/src/test/isolation2/global_sh_executor.sh
	cp $< $@

installcheck: install global_sh_executor.sh
	$(pg_isolation2_regress_installcheck) \
	--dbname=waitsampling --schedule=sampling_schedule

installcheck-resgroup: install global_sh_executor.sh
	$(pg_isolation2_regress_installcheck) \
	--dbname=waitsamplingresgroup --schedule=resgroup_schedule

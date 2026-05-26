top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

installcheck: install
	$(pg_isolation2_regress_installcheck) \
	--dbname=waitsampling --schedule=sampling_schedule

installcheck-resgroup:
	$(pg_isolation2_regress_installcheck) \
	--dbname=waitsamplingresgroup --schedule=resgroup_schedule

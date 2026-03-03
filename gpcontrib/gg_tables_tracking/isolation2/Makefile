top_builddir = ../../../
include $(top_builddir)/src/Makefile.global

installcheck:
	$(pg_isolation2_regress_installcheck) \
	--dbname=trackingisolation --schedule=tracking_schedule \
	--load-extension=gp_inject_fault --load-extension=plpython3u

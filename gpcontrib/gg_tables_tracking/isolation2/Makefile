top_builddir = ../../../

ISOLATION2_ROOT = $(top_builddir)src/test/isolation2

installcheck:
	cd $(ISOLATION2_ROOT) && ./pg_isolation2_regress $(EXTRA_REGRESS_OPTS) \
	--dbname=trackingisolation --schedule=${CURDIR}/tracking_schedule --inputdir=${CURDIR} --outputdir=${CURDIR} \
	--load-extension=gp_inject_fault --load-extension=plpython3u

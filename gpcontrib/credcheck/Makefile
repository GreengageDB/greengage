EXTENSION = credcheck 
MODULE_big = credcheck
OBJS = credcheck.o $(WIN32RES)
PGFILEDESC = "credcheck - postgresql credential checker"
DATA = credcheck--0.1.0.sql

REGRESS_OPTS  = --inputdir=test --outputdir=test --load-extension=credcheck --dbname=postgres
REGRESS = credcheck_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

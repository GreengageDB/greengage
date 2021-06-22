EXTENSION = credcheck 
MODULE_big = credcheck
OBJS = credcheck.o $(WIN32RES)
PGFILEDESC = "credcheck - postgresql credential checker"
DATA = credcheck--0.1.0.sql

REGRESS_OPTS  = --inputdir=test --load-extension=credcheck
TESTS = 01_username 02_password 03_rename 04_alter_pwd

REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

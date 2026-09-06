EXTENSION = gp_local_test
DATA = gp_local_test--1.0.sql \
       gp_local_test--1.1.sql \
       gp_local_test--1.0--1.1.sql \
       gp_local_test--1.1--1.0.sql
PGFILEDESC = "gp_local_test - sample extension for testing local (coordinator-only) extensions"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


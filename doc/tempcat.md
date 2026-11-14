# In-memory temporary table catalog (tempcat)

`gp_enable_temp_memory_catalog` stores the metadata of temporary tables in
backend-private memory instead of the on-disk catalogs, so heavy create/drop
workloads do not bloat `pg_class`, `pg_attribute` and friends.

## Known limitations

**Catalog visibility.**  While the setting is on, plain queries on
`pg_catalog` and `information_schema` (including `\d` in psql and
SPI-based built-ins such as `pg_get_viewdef` or `col_description`) do not
see in-memory temporary tables: they bypass the internal catalog scan path
that merges in-memory rows.  The tables work normally for DML and DDL.
To introspect them, set `gp_enable_temp_memory_catalog = off` in the
session (allowed once the in-memory catalog is empty, i.e. after the
temporary tables are dropped).

**REPEATABLE READ.**  Reader gangs of a REPEATABLE READ (or serializable)
transaction do not pick up in-memory catalog rows published after the
transaction started, so a temporary table created inside such a
transaction would be invisible to the transaction's own later statements.
Creating temporary tables inside REPEATABLE READ transactions is
therefore rejected with an error while the setting is on; create them
outside the transaction (or at READ COMMITTED) instead.

**Parallel retrieve cursors.**  The retrieve connection does not carry the
in-memory catalog, so a cursor whose output type is a temporary composite
type cannot output its columns.  Use a permanent composite type for such
cursors.

**`CLUSTER`.**  Clustering a temporary table is rejected while the setting
is on.  The command itself is a rewrite of the tuple order and could be
supported by copying through the in-memory storage; for now the generic
rebuild path reads the on-disk heap, which is empty for an in-memory table,
so running it would silently lose the data.  Set the setting off or
recreate the table instead.

**`gp_partition_template`.**  Partition template rows of an in-memory
temporary partitioned table are written to disk and name in-memory
relation OIDs; other sessions see them as orphans referencing missing
relations.  A rework of partition template storage is planned upstream.

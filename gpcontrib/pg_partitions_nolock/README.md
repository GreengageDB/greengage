# `pg_partitions_nolock`: Modify `pg_catalog.pg_partitions` so that it does not require `AccessShareLock`

`pg_catalog.pg_partitions` uses `pg_get_expr()`, which holds an `AccessShareLock` on every relation whose OID is provided to it as its second argument. However, this lock is actually useless, as the values passed to `pg_get_expr()` by `pg_catalog.pg_partitions` do not contain `Var` nodes. The presence of the mentioned lock slows down queries significantly when accesses to `pg_catalog.pg_partitions` are combined with `ALTER PARTITION` queries. This is an exotic workload, but it turns out to be possible.

The `pg_partitions_nolock` extension fixes this using the following approach:
1. `pg_catalog.pg_partitions` is renamed to `pg_catalog.pg_partitions_lock`
2. A new `VIEW` with the name `pg_catalog.pg_partitions` is created, which uses a different version of `pg_get_expr()`, not requiring locks.


## Install
```shell script
make install
```
```sql
CREATE EXTENSION pg_partitions_nolock;
```


## Uninstall
PostgreSQL extensions do not provide a capability to set a custom `DROP EXTENSION` script. As a result, it is the uninstalling user's responsibility to rename `pg_catalog.pg_partitions_lock` back to `pg_catalog.pg_partitions`.

```sql
DROP EXTENSION pg_partitions_nolock;
SET allow_system_table_mods = true;
ALTER VIEW pg_catalog.pg_partitions_lock RENAME TO pg_partitions;
```

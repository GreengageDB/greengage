`gg_wait_sampling` – sampling based statistics of wait events for Greengage
===========================================================================

Introduction
------------

Greengage provides information about current wait event of particular
process.  However, in order to gather descriptive statistics of server
behavior user have to sample current wait event multiple times.
`gg_wait_sampling` is an Greengage adaptation of `pg_wait_sampling`
extension for collecting sampling statistics of wait events. This adaptation
is based on upstream extension by Postgres Professional and follows the
PostgreSQL license.

The module must be loaded by adding `gg_wait_sampling` to
`shared_preload_libraries` in postgresql.conf, because it requires additional
shared memory and launches background worker.  This means that a server restart
is needed to add or remove the module.

When used with `pg_stat_statements` it is recommended to put `pg_stat_statements`
before `gg_wait_sampling` in `shared_preload_libraries` so queryIds of
utility statements are not rewritten by the former.

When `gg_wait_sampling` is enabled, it collects two kinds of statistics.

 * History of waits events at each segment.  It's implemented as in-memory
   ring buffer where samples of each process wait events are written with
   given (configurable) period.  Therefore, for each running process user
   can see some number of recent samples depending on history size
   (configurable).  Assuming there is a client who periodically read this
   history and dump it somewhere, user can have continuous history.
 * Waits profile at each segment.  It's implemented as in-memory hash table
   where count of samples are accumulated per each process and each wait event
   (and each query with `pg_stat_statements`).  This hash
   table can be reset by user request.  Assuming there is a client who
   periodically dumps profile and resets it, user can have statistics of
   intensivity of wait events among time.

In combination with `pg_stat_statements` this extension can also provide
per query statistics.

`gg_wait_sampling` launches special background worker for gathering the
statistics above.

In Greengage, the extension also exposes cluster-wide views built as
union of EXECUTE ON SEGMENTS/COORDINATOR versions of original functions.
You can inspect both coordinator-local an segment-local wait activity.

Manual build
------------

`gg_wait_sampling` is Greengage 7 extension which is based on PostgreSQL 12.
Before build and install you should ensure following:

 * GPHOME variable points to binaries directory.
 * Your PATH variable is configured so that `pg_config` command available, or
   set PG_CONFIG variable.

Typical installation procedure may look like this (at each segment):

    $ cd gpcontrib/gg_wait_sampling
    $ source /usr/local/greengage-db-devel/greengage_path.sh
    $ make install

Then run
    $ gpconfig -c shared_preload_libraries -v 'gg_wait_sampling'
And restart the cluster.

To test your installation:

    $ make installcheck
    $ cd isolation2 && make installcheck

To create the extension in the target database:

    CREATE EXTENSION gg_wait_sampling;

Usage
-----

This adaptation differs from upstream pg_wait_sampling in several ways:
 - Main functions are renamed with 'gg_' prefix.
 Some of them are declared as EXECUTE ON SEGMENTS (COORDINATOR).
 - Cluster-wide wait events information is available through the same views as in original extension.
 - Query identity includes Greengage-specific fields:
    - queryid
    - mppsessionid
    - command_id
    - tmid
    - segid
 - Cluster-wide profile reset is implemented through view which calls helper functions
 function on all segments.

All objects live in the gg_wait_sampling schema. You may either schema-qualify all references or add gg_wait_sampling to your search_path.

#### Current waits

`gg_wait_sampling.gg_wait_sampling_current` view – information about current wait events for
all processed including background workers on coordinator and all segments.

| Column name  | Column type |      Description        |
| -----------  | ----------- | ----------------------- |
| pid          | int4        | Id of process           |
| event_type   | text        | Name of wait event type |
| event        | text        | Name of wait event      |
| queryid      | int8        | Id of query             |
| mppsessionid | int4        | Greengage session id    |
| command_id   | int4        | Greengage command id    |
| tmid         | int4        | Transaction time        |
| segid        | int4        | Segment id              |

`gg_wait_sampling.gg_wait_sampling_get_current(pid int4)` returns the same table for single given
process on local node.

#### Wait history

`gg_wait_sampling.gg_wait_sampling_history` view – history of wait events obtained by sampling into
in-memory ring buffer on coordinator and all segments.

| Column name  | Column type |      Description        |
| -----------  | ----------- | ----------------------- |
| pid          | int4        | Id of process           |
| ts           | timestamptz | Sample timestamp        |
| event_type   | text        | Name of wait event type |
| event        | text        | Name of wait event      |
| queryid      | int8        | Id of query             |
| mppsessionid | int4        | Greengage session id    |
| command_id   | int4        | Greengage command id    |
| tmid         | int4        | Transaction time        |
| segid        | int4        | Segment id              |

#### Wait profile

`gg_wait_sampling.gg_wait_sampling_profile` view – profile of wait events obtained by sampling into
in-memory hash table on coordinator and all segments.

| Column name  | Column type |      Description        |
| -----------  | ----------- | ----------------------- |
| pid          | int4        | Id of process           |
| event_type   | text        | Name of wait event type |
| event        | text        | Name of wait event      |
| queryid      | int8        | Id of query             |
| count        | text        | Count of samples        |
| mppsessionid | int4        | Greengage session id    |
| command_id   | int4        | Greengage command id    |
| tmid         | int4        | Transaction time        |
| segid        | int4        | Segment id              |

#### Resetting the profile
Profile reset requires superuser privilege. In version 1.1, reset is implemented as views rather than callable functions, which enables clean cluster-wide reset.

Reset the profile across the entire cluster (coordinator and all segments):
```sql
SELECT * FROM gg_wait_sampling.gg_wait_sampling_reset_profile;
```

Reset view is revoked from PUBLIC and require superuser access.

#### Configuration

The work of wait event statistics collector worker is controlled by following
GUCs.

| Parameter name                   | Data type | Description                                 | Default value |
|----------------------------------| --------- |---------------------------------------------|--------------:|
| gg_wait_sampling.history_size    | int4      | Size of history in-memory ring buffer       |          5000 |
| gg_wait_sampling.history_period  | int4      | Period for history sampling in milliseconds |            10 |
| gg_wait_sampling.profile_period  | int4      | Period for profile sampling in milliseconds |            10 |
| gg_wait_sampling.profile_pid     | bool      | Whether profile should be per pid           |          true |
| gg_wait_sampling.profile_queries | enum      | Whether profile should be per query         |           top |
| gg_wait_sampling.sample_cpu      | bool      | Whether on CPU backends should be sampled   |          true |

If `gg_wait_sampling.profile_pid` is set to false, sampling profile wouldn't be
collected in per-process manner.  In this case the value of pid could would
be always zero and corresponding row contain samples among all the processes.

If `gg_wait_sampling.profile_queries` is set to `none`, `queryid` field in
views will be zero. If it is set to `top`, queryIds only of top level statements
are recorded. If it is set to `all`, queryIds of nested statements are recorded.

If `gg_wait_sampling.sample_cpu` is set to true then processes that are not
waiting on anything are also sampled. The wait event columns for such processes
will be NULL.

Values of these GUC variables can be changed via `gpconfig` utility followed
by `gpstop -u`.

pg_wait_sampling authors
-------

 * Alexander Korotkov <a.korotkov@postgrespro.ru>, Postgres Professional,
   Moscow, Russia
 * Ildus Kurbangaliev <i.kurbangaliev@gmail.com>, Postgres Professional,
   Moscow, Russia


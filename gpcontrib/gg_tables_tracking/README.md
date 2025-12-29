## gg_tables_tracking - database objects tracking extension for GPDB

`gg_tables_tracking` represents a GPDB extension that efficiently tracks file system changes (extend, truncate, create, unlink operations) using space-efficient Bloom filters stored in shared memory. This extension is particularly useful for monitoring and maintaining database files sizes across a distributed environment.

The main purpose of this code is achieving fast database size calculation and tracking file changes at relation
level. The extension implements a probabilistic tracking system using Bloom filters to monitor file changes across Greenplum segments. It utilizes shared memory for state management and employs background workers to maintain consistency. 

### Architecture Overview

The extension implements a probabilistic tracking system using:

* Bloom filters for space-efficient change detection
* Background workers to maintain consistency across coordinator and segments
* File operation hooks to capture CREATE, EXTEND, TRUNCATE, and UNLINK events
* Version control to ensure transactional consistency during snapshot acquisition

### Key Features

* Incremental tracking: Only modified relations are reported
* Transactional semantics: Track acquisition is transaction-safe
* Automatic initialization: Background worker handles cluster setup
* Segment failure recovery: Automatic re-initialization of failed/promoted segments

## Configuring GG and extension usage
* Since extension uses shared memory, configuration on all GPDB segments must be changed by setting
```shell script
gpconfig -c shared_preload_libraries -v 'gg_tables_tracking'
```
Restart is required.

* Configure Background Worker (Optional).
```shell script
gpconfig -c gg_tables_tracking.tracking_worker_naptime_sec -v '10'
gpstop -u
```
|GUC|Requires restart|Range|
--|--|--
| gg_tables_tracking.tracking_db_track_count | No (SIGHUP) |Possible values [1, 3600]; Default 60|

* Extension may track restricted number of databases. The maximum number of them is defined by GUC

||||
--|--|--
| gg_tables_tracking.tracking_worker_naptime_sec | Need restart |Possible values [1, 1000]; Default 5|

* For each tracked database there allocated a Bloom filter in shared memory. The size of each filter is controlled via

||||
--|--|--
| gg_tables_tracking.tracking_bloom_size | Need restart |Possible values (bytes) [64, 128000000] Default 1048576|

### Create extension
```sql script
CREATE EXTENSION gg_tables_tracking;
```
### Usage
```sql script
-- Register current database
SELECT gg_tables_tracking.tracking_register_db();

-- Register specific database by OID
SELECT gg_tables_tracking.tracking_register_db(16384)
```
What happens:
* Database is bound to an available Bloom filter slot
* Initial snapshot mode is set based on tracking_snapshot_on_recovery
* Configuration is persisted in pg_db_role_setting
* Change tracking begins immediately for all file operations

### Check if database is tracked
```sql script
SHOW gg_tables_tracking.tracking_is_db_tracked;  -- Should return 'on'
```

### Configuring Tracking files
1. Track specific schemas
```sql script
-- Register a schema
SELECT gg_tables_tracking.tracking_register_schema('my_schema');

-- Unregister a schema  
SELECT gg_tables_tracking.tracking_unregister_schema('public');
```

Default schemas: public, gg_tables_tracking, pg_catalog, pg_toast, pg_aoseg, information_schema

1. Track specific relkinds
```sql script
--- Track only tables and indexes
SELECT gg_tables_tracking.tracking_set_relkinds('r,i');
```
Valid relkinds: 
* r - ordinary table
* i - index
* S - sequence
* t - TOAST table
* v - view
* c - composite type
* f - foreign table
* m - materialized view
* o - AO segments file
* b - AO block directory
* M - AO visimap
* p - partitioned table
* I - partitioned index

Default: r,i,t,m,o,b,M

3. Track Specific Access Methods
```sql script
-- Track only heap and AO tables
SELECT gg_tables_tracking.tracking_set_relams('heap,ao_row');
```
Default: heap,ao_row,ao_column,btree,hash,gist,gin,spgist,brin,bitmap

### Acquire Tracking Snapshots

Incremental Snapshot (Default)
Returns only relations modified since last snapshot

```sql script
select * from gg_tables_tracking.tables_track;
```
|Column|Type|Description|
--|--|--
| relid | OID |Relation OID (NULL for dropped relations)|
| relname | NAME |Relation name (NULL for dropped)|
| relfilenode | OID |Physical file identifier|
| size | BIGINT |Total size in bytes across all forks|
| state | CHAR |'a' = active, 'd' = dropped, 'i' = initial snapshot|
| segid | INT |Segment ID (-1 for coordinator)|
| relnamespace | OID |Schema OID|
| relkind | CHAR |relkind OID|
| relam | OID |Access method OID|
| parent_relid | OID |Parent relation OID|

State Meanings:
* 'a' - Active relation that was modified (created, extended, truncated)
* 'd' - Dropped relation (only relfilenode and state are populated)
* 'i' - Initial snapshot entry (all relations returned after trigger)

### Full Snapshot (One-Time)
```sql script
-- Trigger full snapshot
SELECT gg_tables_tracking.tracking_trigger_initial_snapshot();

-- Check if full snapshot is active across cluster
SELECT * FROM gg_tables_tracking.is_initial_snapshot_triggered;

-- Acquire full snapshot
SELECT * FROM gg_tables_tracking.tables_track;
```

***Attention***:  Acquiring size track from parallel sessions is not recommended, since there is the only
instance of Bloom filter for a database. I.e. track acquisition can return whole accumulated relation set
in one session, and empty set for acquisition from the second session (the first session acquired data earlier). 

### Snapshot on Recovery (Automatic)

Configure database to return full snapshot after cluster restart:
```sql script
-- Enable
SELECT gg_tables_tracking.tracking_set_snapshot_on_recovery(true);

-- Disable
SELECT gg_tables_tracking.tracking_set_snapshot_on_recovery(false);
```

### Unregister a Database
```sql script
SELECT gg_tables_tracking.tracking_unregister_db();
```
This will:
* Clear the Bloom filter
* Unbind the database from its filter slot
* Remove tracking configuration from pg_db_role_setting
* Stop tracking file operations

#### Choosing optimal Bloom size

Choosing the optimal Bloom filter size is crucial for balancing memory usage and accuracy.
First of all, when choosing the filter size, you should take into account your system resources, because bloom filters are allocated in shared memory for each segment, and too wide structures (tracking_db_track_count * tracking_bloom_size) could decrease overall performance.

Next, choose the filter size satisfying your performance goals:
- Define false positive tolerance, p. Since Bloom filter is probabilistic data structure there is a probability to calculate the size of relation, which has not been modified. And the smaller filter is, the more often this occurs.
- Memory constraints 
- Query patterns, if queries are mostly reading then huge sizes are unnecessary.

If you will estimate number of objects in your database, you can calculate theoretical size:
$$m = -\frac{n \ln p}{(\ln 2)^2}$$
- n = estimated number of elements
- p = target false positive rate
- m = filter size in bits

Quick Reference Table

 Deployment Size | Files      | Target FPR | Recommended Size, bytes|
|----------------|------------|------------|------------------------|
| Small          | < 100K     | 1%         | 1048576                |
| Medium         | 100K - 1M  | 1%         | 8388608                |
| Large          | > 1M       | 1%         | 33554432               |
| Enterprise     | > 10M      | 1%         | 134217728              |

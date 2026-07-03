# GGDB vs vanilla PostgreSQL — the differences map

What GGDB adds/changes relative to upstream PostgreSQL, where each piece lives,
and what typically breaks in an upstream merge. Use it to orient before touching
an area and to predict merge fallout.

## Process roles: coordinator (QD) vs segments (QE)

One coordinator ("query dispatcher", QD) plans queries and dispatches them to N
segment databases ("query executors", QE), each a full postgres instance owning
a data slice. `Gp_role` = GP_ROLE_DISPATCH / GP_ROLE_EXECUTE / GP_ROLE_UTILITY
(src/include/cdb/cdbvars.h); `gp_session_id` ties a QD session to its QE
backends. Merge hazard: upstream assumes one process does everything — new
upstream logic often must run QD-only, QE-only, or be dispatched explicitly.

## Gangs & dispatch (cdbdisp)

The QD allocates a **gang** of QE backends per slice and sends the serialized
plan + params + GUCs over libpq: src/backend/cdb/dispatcher/ (cdbdisp*.c,
cdbgang*.c, cdbconn.c, cdbpq.c). Merge hazard: upstream libpq protocol changes
(PG14 command queue broke `PQsendGpQuery_shared`; a dropped `maxlen==0` guard
in `pq_getmessage` broke COPY and nextval cluster-wide). Errors like "failed to
acquire resources on one or more segments" or "Segments are in reset/recovery
mode" are gang-creation failures, not query bugs.

## Plan serialization: outfast.c / readfast.c

Dispatch uses a BINARY node serializer, src/backend/nodes/outfast.c +
readfast.c, parallel to the text outfuncs.c/readfuncs.c (pre-PG16 they
#included the text files under COMPILING_BINARY_FUNCS; since PG16's generated
*funcs.c they are standalone, maintained by hand). Merge hazard: a new node field
added to the text pair but not the binary pair desyncs the wire — "could not
deserialize unrecognized node type N" with a garbage N means a stream slip;
a real small N means a missing reader case. Audit field lists pairwise.

## Motion nodes & slices

`Motion` is the data-movement plan node (GATHER / HASH / BROADCAST / EXPLICIT
/ GATHER_SINGLE / OUTER_QUERY, MOTIONTYPE_* in src/include/nodes/plannodes.h), executed
by src/backend/executor/nodeMotion.c. A plan is cut at Motions into **slices**
(`ExecSlice`/`SliceTable`, src/include/executor/execdesc.h) built by
src/backend/cdb/cdbllize.c; each slice runs in its own gang. Merge hazard: new
plan-node types unknown to GGDB's plan walkers (walkers.c) or to cdbllize; a
Motion may not sit directly on another Motion.

## Interconnect

Tuple transport between slices: src/backend/cdb/motion/ — ic_udpifc.c (default,
`gp_interconnect_type=udpifc`), ic_tcp.c, ic_proxy_* (requires
`--enable-ic-proxy` + libuv), tuple (de)serialization in tupser.c. Merge
hazard: core-API churn in rarely-built files, e.g. PG13's array-based List made
the old `cell = lnext(...); list_delete_ptr(...)` idiom in ic_tcp.c dangle —
crashes only under gp_interconnect_type=tcp/proxy, which default CI never runs.

## Locus & distribution policy

Every table has a distribution policy: hash (distkey columns), random, or
replicated, stored in the `gp_distribution_policy` catalog (GpPolicy, incl.
`numsegments`). The planner tracks data placement per path as a **locus**
(CdbLocusType_Entry/SingleQE/General/SegmentGeneral/Replicated/Hashed/HashedOJ/
Strewn — src/include/cdb/cdbpathlocus.h; logic in cdbpathlocus.c, cdbpullup.c,
cdbmutate.c). Merge hazard: planner data-structure churn breaks
distribution-key matching against targetlists/equivalence classes (see the
Aggref aggno class in [SKILL.md](SKILL.md)).

## Two optimizers

ORCA (optimizer=on, the default/primary): C++ optimizer in src/backend/gporca,
glued via the translator in src/backend/gpopt (translate/ = Query→DXL→PlannedStmt).
Postgres planner (optimizer=off) with GGDB extensions (cdbgroupingpaths.c
multi-stage aggregation, cdbllize.c parallelization). ORCA silently FALLS BACK
to the planner for unsupported features — an "ORCA test" may be exercising the
planner. Merge hazards: the translator must learn every changed node/field
(missed fields ⇒ wrong results, not errors); ORCA is not re-entrant
(see [SKILL.md](SKILL.md)); plan-shape answer-file drift is expected.

## Distributed transactions (DTX)

The QD coordinates two-phase commit across segments: src/backend/cdb/cdbtm.c
(DTX state machine, gid = distributed xid), cdbdtxrecovery.c (in-doubt
resolution), cdbdistributedsnapshot.c (distributed snapshots layered over local
ones), dispatcher/cdbdisp_dtx.c. GGDB extends the checkpoint WAL record with a
TMGXACT_CHECKPOINT payload replayed via `redoDtxCheckPoint()` (cdbtm.h). Merge
hazard: xlog/xact refactors drop the extended-checkpoint read/replay or the
delay-checkpoint interlocks — symptom: distributed commits abort on segments
after crash recovery ("could not open relation with OID N").

## FTS (fault tolerance service) & mirrors

Each primary segment has a WAL-streamed mirror; the FTS background worker on
the QD (src/backend/fts/: fts.c, ftsprobe.c) probes segments and promotes
mirrors, updating `gp_segment_configuration`. Segments answer probes in
ftsmessagehandler.c — OUTSIDE any transaction, via a special startup-packet
path (`am_ftshandler`). Mirrors run `hot_standby=off` (stay in PM_RECOVERY;
connection acceptance needs the CAC_MIRROR_READY path). `gprecoverseg`
(gpMgmt/bin) repairs a failed segment: incremental = pg_rewind, full =
pg_basebackup. Merge hazards: any new transaction-requiring check in FTS-reached
paths; postmaster state-machine reorderings; pg_rewind/pg_basebackup graft
drops (next section). A cluster where the catalog says role=p but the segment
still has standby.signal means promotion never reached the segment.

## Segment identity & the pg_basebackup/pg_rewind grafts

Each datadir carries its identity in `internal.auto.conf` (`gp_dbid`;
GP_INTERNAL_AUTO_CONF_FILE_NAME in src/include/catalog/catalog.h). GGDB extends
both tools: pg_basebackup `--target-gp-dbid` (writes internal.auto.conf, places
tablespaces under per-dbid dirs), `--force-overwrite`, `-E/--exclude`;
pg_rewind `--slot` and a rule to NEVER copy internal.auto.conf from the source.
Merge hazard (recurring every version): upstream rewrites of basebackup/
pg_rewind (PG15 bbsink/bbstreamer) silently drop these grafts; the failure
surfaces much later as FTS rejecting probes ("PROBE received dbid:N doesn't
match") after a promotion, or gprecoverseg full recovery failing on EEXIST.
Audit every GGDB option end-to-end after touching these tools.

## Append-optimized (AO) tables

Two extra table AMs: AO row-oriented (src/backend/access/appendonly) and AOCO
column-oriented (src/backend/access/aocs); AM names `ao_row`/`ao_column`.
Each AO table owns auxiliary HEAP relations — segfile map (pg_aoseg.*), block
directory (pg_aoblkdir.*), visibility map (pg_aovisimap.*) — created by
src/backend/catalog/{aoseg,aoblkdir,aovisimap,aocatalog}.c with relkinds
'o'/'b'/'M' (pg_class.h). Merge hazards: every new `RELKIND_HAS_*`-keyed path
misses the aux relkinds (see [SKILL.md](SKILL.md)); AO segfiles are not
BLCKSZ-paged, so page-checksum verification (e.g. basebackup) must skip them.

## Resource queues & resource groups

Two admission-control systems. Resource queues (legacy, default):
src/backend/utils/resscheduler (resqueue.c), catalogs pg_resqueue*. Resource
groups: src/backend/utils/resgroup + utils/resource_manager, cgroup-v1 only
(cgroup-ops-linux-v1.c) — need `gp_resource_manager=group` and a writable
cgroup v1 hierarchy; do not enable group mode on a cgroup-v2 host (init fails
and wedges the cluster). Merge hazard: lock/wait-queue and pgstat refactors
touch resqueue waiting; resgroup shmem is wired in core ipci.c.

## External tables & gpfdist

Web/file external tables: src/backend/access/external (external.c, url_curl.c
...), surfaced through the gp_exttable_fdw foreign-data wrapper
(gpcontrib/gp_exttable_fdw); `gpfdist` is the external file server
(src/bin/gpfdist). Tests involving gpfdist can hang rather than fail — treat a
"hanging external-table test" as a harness issue first.

## GGDB catalogs

Extra system catalogs (src/include/catalog/): `gp_segment_configuration`
(dbid, content, role, preferred_role, mode, status, port, hostname, datadir —
THE cluster-topology table), `gp_distribution_policy`, `gp_id`,
`gp_fastsequence`, `pg_appendonly`, `pg_resqueue*`/`pg_resgroup*`,
`gp_partition_template`, pg_stat_last_operation, etc. They go through genbki
like upstream catalogs. Merge hazards: OID collisions with new upstream
objects; genbki/initdb tightening (see
[greengage-pg-merge](../greengage-pg-merge/SKILL.md)). Handy idiom:
`select gp_segment_id, ... from gp_dist_random('pg_class')` reads a catalog
from every segment (implemented in parser/parse_clause.c).

## Fault injection (gp_inject_fault)

Backend fault-point framework src/backend/utils/misc/faultinjector.c + SQL
interface extension gpcontrib/gp_inject_fault: `gp_inject_fault(name, type,
dbid)` with types like suspend/skip/error/panic/infinite_loop/reset, and
`gp_wait_until_triggered_fault(...)`. Used heavily by isolation2 tests. Merge
hazard: upstream refactors move/remove the code path containing a fault point
— the fault "never fires" and the test HANGS forever instead of failing (a
dominant post-merge isolation2 failure mode). Killing such a test wedges the
cluster — see [greengage-cluster-ops](../greengage-cluster-ops/SKILL.md).

## Utility mode

`PGOPTIONS='-c gp_role=utility' psql -p <segment port>` connects directly to a
single segment (or the QD) bypassing all MPP machinery — no dispatch, no DTX.
Used by gpMgmt tools, pg_upgrade, and isolation2's `-1U:`-style sessions.
Useful for inspecting per-segment catalogs; DDL in utility mode desyncs the
cluster — read-only inspection only.

## GUC dispatch lists

The QD keeps QE session GUCs consistent: every GUC is classified in
`sync_guc_names_array` (dispatched to QEs) or `unsync_guc_names_array` in
src/backend/utils/misc/guc_gp.c (GGDB GUC definitions also live there, not in
guc.c). Merge hazard: every new upstream GUC must be added to one of the lists
— the `sync_guc` isolation2 test enforces coverage.

## gpexpand catalog protection

Online cluster expansion bumps a shared version; a session holding a gang from
the OLD segment set gets `FATAL: cluster is expanded from version N to M` on
catalog writes (`gp_expand_protect_catalog_changes()`,
src/backend/utils/misc/gpexpand.c, called from heapam QD-side). The FATAL is
intentional (an ERROR would let the stale gang keep computing wrong results on
the smaller segment set) — do not "fix" it to an ERROR; note old-libpq clients
may only see "server closed the connection unexpectedly".

## Management utilities (gpMgmt)

Python tooling in gpMgmt/bin: gpinitsystem, gpstart/gpstop, gprecoverseg,
gpconfig (cluster-wide GUC editing), gpinitstandby, gpexpand, plus gppylib.
They parse server output and call pg_ctl/pg_rewind/pg_basebackup — merge
hazard: upstream output-format/option changes break the parsers; covered by
the behave suite, not by regress.

## Extra test harnesses

- **isolation2** (src/test/isolation2): multi-session spec tests with
  `1:`/`2:` session prefixes, utility-mode (`1U:`) and mirror connections;
  most fault-injection and HA tests live here. Needs
  `--load-extension=gp_inject_fault` and gpdemo env. See
  [greengage-regress-tests](../greengage-regress-tests/SKILL.md).
- **behave** (gpMgmt/test/behave): Gherkin integration tests for the
  management utilities; many scenarios need a multi-host docker-compose
  harness.
- **gpdiff/atmsort/init_file** (src/test/regress/gpdiff.pl, atmsort.pm,
  init_file): GGDB's diff layer — sorts unordered result sets, applies
  matchignore/matchsubs masks, honors `--start_ignore`/`GP_IGNORE:`. Answer
  files are compared through it, never raw diff. See
  [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md).

## The demo cluster

Development runs against **gpdemo** (gpAux/gpdemo: demo_cluster.sh,
gpdemo-env.sh) — typically 3 primary/mirror pairs + standby coordinator on one
host, ports 7000+. `source gpAux/gpdemo/gpdemo-env.sh` sets
COORDINATOR_DATA_DIRECTORY/PGPORT for all tooling. Operations, health checks,
and recovery: [greengage-cluster-ops](../greengage-cluster-ops/SKILL.md).

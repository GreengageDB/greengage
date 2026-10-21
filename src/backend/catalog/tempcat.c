/*-------------------------------------------------------------------------
 *
 * tempcat.c
 *	  In-memory catalog rows for temporary objects.
 *
 * Every temporary table leaves dead tuples in pg_class, pg_attribute,
 * pg_type, pg_depend and friends, on the coordinator and on every segment.
 * Applications that create and drop many temporary tables therefore bloat
 * the catalogs and keep autovacuum busy.  This module keeps the catalog rows
 * of temporary objects out of the on-disk catalogs, in memory private to
 * the session.
 *
 * Which rows are kept in memory is decided by the data, not by the code
 * path that writes them:
 *
 * - When gp_enable_temp_memory_catalog is on at the moment the session's
 *   temporary namespace is created, the namespace gets an OID from the
 *   reserved range FirstTempcatObjectId..LastTempcatObjectId.  Relations,
 *   types, functions and operators created in such a namespace, and the
 *   attribute defaults, constraints, rules, triggers and policies of such
 *   relations, get OIDs from the same range (see tempcat_want_temp_oid(),
 *   called from oid_dispatch.c).  The coordinator assigns the OIDs and
 *   dispatches them to the segments as usual, so all nodes agree on them.
 *
 * - The range is split into slices of TEMPCAT_SLICE_SIZE OIDs.  A session
 *   takes its OIDs from slices of its own, which its coordinator backend
 *   takes (the first when it creates the temporary namespace), so OIDs of
 *   concurrent sessions never collide and lock tags stay unique.  On every
 *   node, the process keeping the session's objects (the coordinator
 *   backend, or the writer QE on a segment) holds each slice for as long as
 *   any of them has an OID of it (see tempcat_claim_free_slice(),
 *   tempcat_claim_slice() and tempcat_release_unused_slices()).
 *
 * - A session does not give an OID again soon: it goes through a slice in
 *   ascending order, then takes another one, one it used before only when
 *   no other is free (after it went through all slices free then, 64K OIDs
 *   each, which are few when other sessions hold most).  Backend-local caches
 *   keyed by OID (the type cache, cached plans, PL/pgSQL casts, ...) assume
 *   that an OID does not come back soon, which upstream's OID counter,
 *   wrapping around after 2^32 OIDs, makes true; a slice going round in a
 *   long session would not.  When OIDs of a slice do come back, the
 *   session's processes forget what they cached first
 *   (tempcat_forget_cached_oids()).  Another session may take a slice that
 *   was let go of: its processes are new, and so are their caches.
 *
 * - A row inserted into one of the catalogs listed in tempcat_catalogs[] is
 *   kept in memory if its owner column (pg_attribute.attrelid,
 *   pg_index.indrelid, ...) holds an OID that this session assigned to a
 *   temporary object.  Ownership is tracked per (catalog, OID) in a
 *   backend-local set, filled when the OID is allocated (coordinator) or
 *   received from the coordinator (segments), so ordinary objects that
 *   happen to have OIDs in the reserved range (created before the range was
 *   reserved, or carried over by pg_upgrade) are never affected.
 *
 * - Dependencies of temporary objects on ordinary objects (pg_depend and
 *   pg_shdepend rows: types of their columns, functions of their defaults,
 *   their owners, ...) are kept in memory too.  Other sessions cannot see
 *   them, so a session counts itself among the users of each ordinary
 *   object its temporary objects use in shared memory instead (see
 *   tempcat_use_object()), which DROP and the ALTERs that check for
 *   dependent objects fail on, as they would on the dependent objects
 *   themselves (tempcat_lock_out_users()).  The counts go with the session,
 *   also when it crashes (shared memory is reset then): nothing of a
 *   temporary object kept in memory is ever on disk, and there is nothing to
 *   clean up after it.  Ordinary objects cannot depend on temporary objects
 *   in memory at all (tempcat_check_dependency()).
 *
 * The rows live in a dynamic shared memory area owned by the session's
 * writer process.  Its handle is published in the writer's
 * SharedSnapshotSlot, so reader gangs and entry-db readers of the same
 * session attach to it and see the same rows, including uncommitted ones,
 * without any serialization.  The area grows up to
 * gp_temp_memory_catalog_max_size; a session that needs more gets an error.
 *
 * Every row carries xmin/xmax/cmin/cmax and is checked with the usual MVCC
 * rules, so subtransactions, aborts and two-phase commit behave like on-disk
 * rows: an aborted insert simply never becomes visible.  Rows whose fate is
 * known are cleaned up by the writer when it goes idle, or at the start of
 * its next transaction.  Frozen inserts and in-place updates
 * (gp_fastsequence, relpages, ...) stay non-transactional, like on disk.
 *
 * Lookups go through hash tables keyed by up to two columns per catalog
 * (typically the owner OID and the object's own OID or name), so the cost
 * does not grow with the number of temporary tables in the session.
 *
 * Portions Copyright (c) 2026, Greengage Development Group
 *
 * IDENTIFICATION
 *	  src/backend/catalog/tempcat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/twophase.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_distribution_policy.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/gp_partition_template.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_index.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_init_privs.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_callback.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_stat_last_operation.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_encoding.h"
#include "catalog/tempcat.h"
#include "cdb/cdbendpoint.h"
#include "commands/tablespace.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "storage/dsm.h"
#include "storage/dsm_impl.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/dsa.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/gpexpand.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sharedsnapshot.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/vmem_tracker.h"

/* GUCs */
bool		gp_enable_temp_memory_catalog = false;
int			gp_temp_memory_catalog_max_size = 16384;	/* kB, per writer */

/* ----------------------------------------------------------------
 * Catalogs that can hold rows in memory
 * ----------------------------------------------------------------
 */
typedef enum TempcatKeyType
{
	TCK_NONE,
	TCK_OID,
	TCK_NAME
} TempcatKeyType;

#define TEMPCAT_NKEYS 2

typedef struct TempcatCatalogDef
{
	Oid			relid;

	/*
	 * A row is kept in memory if its owner column holds an OID that this
	 * session assigned to a temporary object of catalog 'ownerclass' (or of
	 * the catalog named by column 'ownerclassattr').
	 */
	AttrNumber	owner;
	Oid			ownerclass;
	AttrNumber	ownerclassattr;

	AttrNumber	keyattr[TEMPCAT_NKEYS]; /* hashed lookup columns */
	TempcatKeyType keytype[TEMPCAT_NKEYS];
} TempcatCatalogDef;

#define REL RelationRelationId

static const TempcatCatalogDef tempcat_catalogs[] =
{
	{RelationRelationId, Anum_pg_class_oid, REL, 0,
	{Anum_pg_class_oid, Anum_pg_class_relname}, {TCK_OID, TCK_NAME}},
	{AttributeRelationId, Anum_pg_attribute_attrelid, REL, 0,
	{Anum_pg_attribute_attrelid, 0}, {TCK_OID, TCK_NONE}},
	{TypeRelationId, Anum_pg_type_oid, TypeRelationId, 0,
	{Anum_pg_type_oid, Anum_pg_type_typname}, {TCK_OID, TCK_NAME}},
	{NamespaceRelationId, Anum_pg_namespace_oid, NamespaceRelationId, 0,
	{Anum_pg_namespace_oid, Anum_pg_namespace_nspname}, {TCK_OID, TCK_NAME}},
	{IndexRelationId, Anum_pg_index_indrelid, REL, 0,
	{Anum_pg_index_indrelid, Anum_pg_index_indexrelid}, {TCK_OID, TCK_OID}},
	{AttrDefaultRelationId, Anum_pg_attrdef_adrelid, REL, 0,
	{Anum_pg_attrdef_adrelid, Anum_pg_attrdef_oid}, {TCK_OID, TCK_OID}},
	/* conrelid, or contypid for domain constraints; see tempcat_route_insert() */
	{ConstraintRelationId, Anum_pg_constraint_conrelid, REL, 0,
	{Anum_pg_constraint_conrelid, Anum_pg_constraint_oid}, {TCK_OID, TCK_OID}},
	{RewriteRelationId, Anum_pg_rewrite_ev_class, REL, 0,
	{Anum_pg_rewrite_ev_class, Anum_pg_rewrite_oid}, {TCK_OID, TCK_OID}},
	{TriggerRelationId, Anum_pg_trigger_tgrelid, REL, 0,
	{Anum_pg_trigger_tgrelid, Anum_pg_trigger_oid}, {TCK_OID, TCK_OID}},
	{StatisticRelationId, Anum_pg_statistic_starelid, REL, 0,
	{Anum_pg_statistic_starelid, 0}, {TCK_OID, TCK_NONE}},
	{DescriptionRelationId, Anum_pg_description_objoid, InvalidOid,
		Anum_pg_description_classoid,
	{Anum_pg_description_objoid, 0}, {TCK_OID, TCK_NONE}},
	{DependRelationId, Anum_pg_depend_objid, InvalidOid, Anum_pg_depend_classid,
	{Anum_pg_depend_objid, Anum_pg_depend_refobjid}, {TCK_OID, TCK_OID}},
	{InheritsRelationId, Anum_pg_inherits_inhrelid, REL, 0,
	{Anum_pg_inherits_inhrelid, Anum_pg_inherits_inhparent}, {TCK_OID, TCK_OID}},
	{PartitionedRelationId, Anum_pg_partitioned_table_partrelid, REL, 0,
	{Anum_pg_partitioned_table_partrelid, 0}, {TCK_OID, TCK_NONE}},
	{SequenceRelationId, Anum_pg_sequence_seqrelid, REL, 0,
	{Anum_pg_sequence_seqrelid, 0}, {TCK_OID, TCK_NONE}},
	{GpPolicyRelationId, Anum_gp_distribution_policy_localoid, REL, 0,
	{Anum_gp_distribution_policy_localoid, 0}, {TCK_OID, TCK_NONE}},
	{AppendOnlyRelationId, Anum_pg_appendonly_relid, REL, 0,
	{Anum_pg_appendonly_relid, 0}, {TCK_OID, TCK_NONE}},
	{FastSequenceRelationId, Anum_gp_fastsequence_objid, REL, 0,
	{Anum_gp_fastsequence_objid, 0}, {TCK_OID, TCK_NONE}},
	{AttributeEncodingRelationId, Anum_pg_attribute_encoding_attrelid, REL, 0,
	{Anum_pg_attribute_encoding_attrelid, 0}, {TCK_OID, TCK_NONE}},
	{PartitionTemplateRelationId, Anum_gp_partition_template_relid, REL, 0,
	{Anum_gp_partition_template_relid, 0}, {TCK_OID, TCK_NONE}},
	{PolicyRelationId, Anum_pg_policy_polrelid, REL, 0,
	{Anum_pg_policy_polrelid, Anum_pg_policy_oid}, {TCK_OID, TCK_OID}},
	/* foreign and external tables */
	{ForeignTableRelationId, Anum_pg_foreign_table_ftrelid, REL, 0,
	{Anum_pg_foreign_table_ftrelid, 0}, {TCK_OID, TCK_NONE}},
	{SecLabelRelationId, Anum_pg_seclabel_objoid, InvalidOid,
		Anum_pg_seclabel_classoid,
	{Anum_pg_seclabel_objoid, 0}, {TCK_OID, TCK_NONE}},
	/* initial privileges of extension members (recordExtensionInitPriv()) */
	{InitPrivsRelationId, Anum_pg_init_privs_objoid, InvalidOid,
		Anum_pg_init_privs_classoid,
	{Anum_pg_init_privs_objoid, 0}, {TCK_OID, TCK_NONE}},
	/* GPDB's DDL tracking (MetaTrackAddObject() and friends) */
	{StatLastOpRelationId, Anum_pg_stat_last_operation_objid, InvalidOid,
		Anum_pg_stat_last_operation_classid,
	{Anum_pg_stat_last_operation_objid, 0}, {TCK_OID, TCK_NONE}},
	/* objects in a temporary namespace kept in memory, and their parts */
	{ProcedureRelationId, Anum_pg_proc_pronamespace, NamespaceRelationId, 0,
	{Anum_pg_proc_oid, Anum_pg_proc_proname}, {TCK_OID, TCK_NAME}},
	{AggregateRelationId, Anum_pg_aggregate_aggfnoid, ProcedureRelationId, 0,
	{Anum_pg_aggregate_aggfnoid, 0}, {TCK_OID, TCK_NONE}},
	{ProcCallbackRelationId, Anum_pg_proc_callback_profnoid, ProcedureRelationId, 0,
	{Anum_pg_proc_callback_profnoid, 0}, {TCK_OID, TCK_NONE}},
	{OperatorRelationId, Anum_pg_operator_oprnamespace, NamespaceRelationId, 0,
	{Anum_pg_operator_oid, Anum_pg_operator_oprname}, {TCK_OID, TCK_NAME}},
	{EnumRelationId, Anum_pg_enum_enumtypid, TypeRelationId, 0,
	{Anum_pg_enum_enumtypid, Anum_pg_enum_oid}, {TCK_OID, TCK_OID}},
	{RangeRelationId, Anum_pg_range_rngtypid, TypeRelationId, 0,
	{Anum_pg_range_rngtypid, 0}, {TCK_OID, TCK_NONE}},
	{TypeEncodingRelationId, Anum_pg_type_encoding_typid, TypeRelationId, 0,
	{Anum_pg_type_encoding_typid, 0}, {TCK_OID, TCK_NONE}},
	{StatisticExtRelationId, Anum_pg_statistic_ext_stxrelid, REL, 0,
	{Anum_pg_statistic_ext_stxrelid, Anum_pg_statistic_ext_oid}, {TCK_OID, TCK_OID}},
	{StatisticExtDataRelationId, Anum_pg_statistic_ext_data_stxoid, StatisticExtRelationId, 0,
	{Anum_pg_statistic_ext_data_stxoid, 0}, {TCK_OID, TCK_NONE}},
	{SharedDependRelationId, Anum_pg_shdepend_objid, InvalidOid,
		Anum_pg_shdepend_classid,
	{Anum_pg_shdepend_objid, Anum_pg_shdepend_refobjid}, {TCK_OID, TCK_OID}},
};

#undef REL

#define TEMPCAT_NCATALOGS lengthof(tempcat_catalogs)

/*
 * Catalogs whose objects get reserved OIDs (see tempcat_slice_crowded()).
 */
static const Oid tempcat_object_classes[] =
{
	RelationRelationId,
	TypeRelationId,
	NamespaceRelationId,
	ProcedureRelationId,
	OperatorRelationId,
	ConstraintRelationId,
	AttrDefaultRelationId,
	RewriteRelationId,
	TriggerRelationId,
	PolicyRelationId,
	StatisticExtRelationId,
};

/* ----------------------------------------------------------------
 * Structures in the session's area
 *
 * Everything below lives in the area and refers to other objects by
 * dsa_pointer.  The writer modifies them under the exclusive lock; readers
 * take the lock in shared mode.
 * ----------------------------------------------------------------
 */
typedef struct TempcatHash
{
	uint32		nbuckets;		/* power of two, or 0 if not used yet */
	uint32		nentries;
	uint32		shrink_below;	/* shrink when nentries drops below it */
	dsa_pointer buckets;		/* dsa_pointer[nbuckets] */
	dsa_pointer home;			/* the buckets while there are
								 * TEMPCAT_HASH_MIN, in the root (invalid
								 * for an unused key) */
} TempcatHash;

/* Buckets of a hash table at the least, see tempcat_hash_shrink() */
#define TEMPCAT_HASH_MIN	64

typedef struct TempcatHashNode
{
	dsa_pointer next;
	dsa_pointer entry;
	uint32		hash;
} TempcatHashNode;

typedef struct TempcatCatalog
{
	int32		nentries;		/* entries of any visibility */
	dsa_pointer head;			/* list of all entries */
	TempcatHash keys[TEMPCAT_NKEYS];
} TempcatCatalog;

/* Free chunks of the blocks of rows by size, see tempcat_alloc() */
#define TEMPCAT_NBINS		12

typedef struct TempcatRoot
{
	LWLock		lock;
	int			writer_pgprocno;	/* the writer's, see tempcat_writer_state() */
	pg_atomic_uint32 cache_epoch;	/* see tempcat_forget_cached_oids() */
	uint64		next_tid;
	TempcatHash tids;
	TempcatCatalog catalogs[TEMPCAT_NCATALOGS];
	dsa_pointer bins[TEMPCAT_NBINS];
	Size		taken;			/* of the area for blocks and big chunks */
	/*
	 * the home buckets of the hash tables, TEMPCAT_HASH_MIN each: of the
	 * catalogs' keys in use, then of 'tids'
	 */
	dsa_pointer home_buckets[FLEXIBLE_ARRAY_MEMBER];
} TempcatRoot;

/* xmin is known to be committed (or frozen) */
#define TCE_XMIN_COMMITTED	0x0001
/* entry is on the writer's pending list */
#define TCE_PENDING			0x0002

typedef struct TempcatEntry
{
	dsa_pointer prev;
	dsa_pointer next;
	TransactionId xmin;
	TransactionId xmax;
	CommandId	cmin;
	CommandId	cmax;
	uint32		keyhash[TEMPCAT_NKEYS];
	uint32		tidhash;
	uint16		flags;
	int16		catidx;
	ItemPointerData tid;
	uint32		len;			/* length of the tuple data that follows */
} TempcatEntry;

#define TCE_DATA(e) \
	((HeapTupleHeader) ((char *) (e) + MAXALIGN(sizeof(TempcatEntry))))

#define TC_ADDR(dp)	dsa_get_address(tc_area, (dp))

/* ----------------------------------------------------------------
 * Process-local state
 * ----------------------------------------------------------------
 */
static dsa_area *tc_area = NULL;
static TempcatRoot *tc_root = NULL;

/* Readers: the writer's cache_epoch when we last caught up with it */
static uint32 tc_seen_cache_epoch = 0;

/*
 * Writer only: entries inserted or deleted by transactions whose outcome
 * has not been applied yet.
 */
static dsa_pointer *tc_pending = NULL;
static int	tc_npending = 0;
static bool tc_resolve_wanted = false;	/* see tempcat_abort_subtransaction() */
static void tempcat_resolve_pending(void);
static int	tc_maxpending = 0;

/* Rows deleted so far, and the last walk of tempcat_reclaim_dead() */
static uint64 tc_ndeletions = 0;
static bool tc_reclaim_failed = false;	/* it freed nothing */
static TransactionId tc_reclaim_xid = InvalidTransactionId;
static CommandId tc_reclaim_cid = InvalidCommandId;
static uint64 tc_reclaim_ndeletions = 0;
static bool tc_reclaim_waiting = false;	/* rows left for a later command */

/* Memory went back to the area; see tempcat_idle() */
static bool tc_trim_wanted = false;

/* The current transaction changed rows in memory; see tempcat_check_prepare() */
static bool tc_xact_changed = false;

/*
 * The deepest subtransaction level whose changes of rows in memory may be
 * on the pending list; see tempcat_abort_subtransaction().
 */
static int	tc_change_level = 0;

/* Has this session started to keep temporary objects in memory? */
static bool tc_activated = false;

/* The published frozen horizon may be too old and needs recomputing. */
static bool tc_horizon_dirty = false;

/* Ordinary objects that the session locks, see tempcat_use_object() */
static HTAB *tc_used = NULL;

/* tc_used may include objects not used any more. */
static bool tc_used_dirty = false;

/*
 * The deepest subtransaction level that changed our pg_depend or pg_shdepend
 * rows in memory, 0 if none: see tempcat_xact_callback().
 */
static int	tc_depend_level = 0;

struct TempcatScanData
{
	/*
	 * The visible rows in memory that match the keys: for scans through an
	 * index, copies of them; otherwise only their TIDs, each copied when
	 * returned (into 'current', valid until the next one), so that scans of
	 * catalogs with many rows in memory do not keep copies of them all.
	 */
	HeapTuple  *tuples;
	ItemPointerData *tids;
	int			ntuples;
	int			next;
	Oid			relid;
	HeapTuple	current;
	bool		on_virtual;		/* last tuple returned was in memory */

	/*
	 * For scans through an index: rows in memory are sorted by the index key
	 * and merged with the on-disk rows, so that callers relying on index
	 * order (e.g. catcache lists of pg_attribute by attnum) see it.
	 */
	Relation	irel;
	TupleDesc	heapdesc;
	int			nidxkeys;
	FmgrInfo  **procs;
	TupleDesc	idxcoldesc;		/* see tempcat_index_columns() */
	HeapTuple	disk_tuple;		/* fetched on-disk row not returned yet */
	bool		disk_done;
	int			direction;		/* 0 until the first row, then 1 or -1 */
};

typedef enum TempcatXidStatus
{
	TCX_IN_PROGRESS,
	TCX_COMMITTED,
	TCX_ABORTED
} TempcatXidStatus;

static int	tempcat_tranche_id(void);
static void tempcat_activate(Oid oid);
static void tempcat_forget_if_gone(int catidx, Oid objoid);
static int	tempcat_reclaim_dead(void);
static bool tempcat_memory_has_oid(int catidx, Oid oid);
static bool tempcat_take_namespace_oid(void);
static bool tempcat_slice_crowded(int slice);
static void tempcat_let_go_of_slice(int slice);
static void tempcat_count_slice_row(TempcatEntry *e, int delta);
static void tempcat_forget_used_objects(void);
static void tempcat_forget_cached_oids(int slice);

/* OIDs assigned by this session to its temporary objects, see below */
static HTAB *tc_owned = NULL;

/*
 * The OID of the session's temporary namespace, taken before it decided to
 * keep it in memory (see tempcat_want_temp_oid()).
 */
static Oid	tc_namespace_oid = InvalidOid;

/*
 * Reader processes (reader gangs, entry-db readers, and parallel retrieve
 * cursor connections) only look at the writer's area; everything else owns
 * its own.
 */
static inline bool
tempcat_is_reader(void)
{
	return (Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer) ||
		am_cursor_retrieve_handler;
}

/*
 * The index of each covered catalog in tempcat_catalogs[], plus one, by the
 * catalog's OID (0 for others), built at the first lookup: catalog scans
 * look it up all the time, also with the feature off.  Catalogs have OIDs
 * below FirstGenbkiObjectId.
 */
static uint8 tc_catidx_by_relid[FirstGenbkiObjectId];
static bool tc_catidx_ready = false;

StaticAssertDecl(lengthof(tempcat_catalogs) < PG_UINT8_MAX,
				 "too many catalogs for tc_catidx_by_relid");

static int
tempcat_catalog_index(Oid relid)
{
	if (unlikely(!tc_catidx_ready))
	{
		int			i;

		for (i = 0; i < TEMPCAT_NCATALOGS; i++)
		{
			Assert(tempcat_catalogs[i].relid < FirstGenbkiObjectId);
			tc_catidx_by_relid[tempcat_catalogs[i].relid] = i + 1;
		}
		tc_catidx_ready = true;
	}
	if (relid >= FirstGenbkiObjectId)
		return -1;
	return (int) tc_catidx_by_relid[relid] - 1;
}

/* The first attribute of these catalogs is their oid column. */
static inline bool
tempcat_catalog_has_oid(int catidx)
{
	switch (tempcat_catalogs[catidx].relid)
	{
		case RelationRelationId:
		case TypeRelationId:
		case NamespaceRelationId:
		case AttrDefaultRelationId:
		case ConstraintRelationId:
		case RewriteRelationId:
		case TriggerRelationId:
		case PolicyRelationId:
		case ProcedureRelationId:
		case OperatorRelationId:
		case StatisticExtRelationId:
			return true;
		default:
			return false;
	}
}

static inline Oid
tempcat_entry_oid(TempcatEntry *e)
{
	HeapTupleHeader hdr = TCE_DATA(e);

	return *(Oid *) ((char *) hdr + hdr->t_hoff);
}

static Oid
tempcat_getoid(HeapTuple tup, TupleDesc desc, AttrNumber attno)
{
	bool		isnull;
	Datum		value = heap_getattr(tup, attno, desc, &isnull);

	return isnull ? InvalidOid : DatumGetObjectId(value);
}

/* ----------------------------------------------------------------
 * Shared memory
 *
 * Per PGPROC: the oldest relfrozenxid/relminmxid of the in-memory temporary
 * tables of the session using it, which stand in for the pg_class rows that
 * vac_update_datfrozenxid() cannot see.
 *
 * Per slice of the reserved range: the process of this node holding it, its
 * database, and, on the coordinator, the next OID to assign.  And where the
 * coordinator looks for a free slice next (tempcat_claim_free_slice()), and
 * which ordinary objects the temporary objects of sessions use
 * (tempcat_use_object()).
 * ----------------------------------------------------------------
 */
#define TEMPCAT_SLICE_BITS	16
#define TEMPCAT_SLICE_SIZE	((Oid) 1 << TEMPCAT_SLICE_BITS)
#define TEMPCAT_NSLICES \
	((int) ((((uint64) LastTempcatObjectId - FirstTempcatObjectId) >> TEMPCAT_SLICE_BITS) + 1))

#define tempcat_slice_of(oid) \
	((int) (((Oid) (oid) - FirstTempcatObjectId) >> TEMPCAT_SLICE_BITS))

typedef struct TempcatProcState
{
	Oid			databaseId;		/* database of the published horizon */
	pg_atomic_uint32 frozenXid; /* or InvalidTransactionId */
	pg_atomic_uint32 minMulti;	/* or InvalidMultiXactId */
} TempcatProcState;

typedef struct TempcatSliceState
{
	pg_atomic_uint32 holder;	/* pgprocno + 1, or 0 */
	Oid			databaseId;		/* the holder's database */
	Oid			nextOid;		/* coordinator: next OID to assign */
} TempcatSliceState;

/*
 * An ordinary object (or column) that temporary objects of sessions use, with
 * the number of sessions, the processes of some of them, and the
 * (sub)transaction of a DROP or ALTER of it in progress, if any (see
 * tempcat_use_object() and tempcat_lock_out_users()).
 */
#define TEMPCAT_USED_PROCS	4

typedef struct TempcatUsedObject
{
	Oid			databaseId;		/* InvalidOid for shared objects */
	Oid			classId;
	Oid			objectId;
	int32		objectSubId;	/* a column, or 0 for the whole object */
} TempcatUsedObject;

typedef struct TempcatUsedObjectEntry
{
	TempcatUsedObject key;
	int			nsessions;
	int			procs[TEMPCAT_USED_PROCS];	/* pgprocnos, or -1 */
	TransactionId changer_xid;	/* or InvalidTransactionId */
} TempcatUsedObjectEntry;

typedef struct TempcatSharedData
{
	int			tranche_id;		/* LWLock tranche of the session areas */
	LWLock		used_lock;		/* protects TempcatUsedObjects */
	/* sessions that use objects missing from it, as it was full */
	pg_atomic_uint32 nused_overflow;
	/* its entries, so that DROP looks at it only when it has any */
	pg_atomic_uint32 nused_entries;
	int			nprocs;
	pg_atomic_uint32 next_slice;	/* coordinator: see tempcat_claim_free_slice() */
	bool		areas_created;	/* see tempcat_areas_created() */
	TempcatSliceState slices[TEMPCAT_NSLICES];
	TempcatProcState procs[FLEXIBLE_ARRAY_MEMBER];
} TempcatSharedData;

static TempcatSharedData *TempcatShared = NULL;
static HTAB *TempcatUsedObjects = NULL;

static inline int
tempcat_total_procs(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;
}

static inline long
tempcat_used_objects_size(void)
{
	return Max(4096, (long) MaxBackends * 32);
}

Size
TempcatShmemSize(void)
{
	Size		size;

	size = add_size(offsetof(TempcatSharedData, procs),
					mul_size(tempcat_total_procs(), sizeof(TempcatProcState)));
	return add_size(size, hash_estimate_size(tempcat_used_objects_size(),
											 sizeof(TempcatUsedObjectEntry)));
}

void
TempcatShmemInit(void)
{
	bool		found;
	int			i;
	HASHCTL		info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TempcatUsedObject);
	info.entrysize = sizeof(TempcatUsedObjectEntry);
	TempcatUsedObjects = ShmemInitHash("Temporary catalog used objects",
									   tempcat_used_objects_size(),
									   tempcat_used_objects_size(),
									   &info,
									   HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);

	TempcatShared = ShmemInitStruct("Temporary catalog data",
									offsetof(TempcatSharedData, procs) +
									mul_size(tempcat_total_procs(),
											 sizeof(TempcatProcState)),
									&found);
	if (found)
		return;

	/* A dynamic tranche, so that the builtin tranche IDs stay unchanged. */
	TempcatShared->tranche_id = LWLockNewTrancheId();
	LWLockInitialize(&TempcatShared->used_lock, TempcatShared->tranche_id);
	pg_atomic_init_u32(&TempcatShared->nused_overflow, 0);
	pg_atomic_init_u32(&TempcatShared->nused_entries, 0);
	TempcatShared->areas_created = false;
	/*
	 * Start the search for free slices at a random point, not where the
	 * coordinator before a restart or a failover started: writer QEs of its
	 * sessions may still hold their slices on the segments for a while (until
	 * they notice that their coordinator is gone), where new holders would
	 * wait for them (tempcat_claim_slice()).
	 */
	{
		uint32		start;

		if (!pg_strong_random(&start, sizeof(start)))
			start = (uint32) (GetCurrentTimestamp() ^ MyProcPid);
		pg_atomic_init_u32(&TempcatShared->next_slice, start % TEMPCAT_NSLICES);
	}
	for (i = 0; i < TEMPCAT_NSLICES; i++)
	{
		TempcatSliceState *ss = &TempcatShared->slices[i];

		pg_atomic_init_u32(&ss->holder, 0);
		ss->databaseId = InvalidOid;
		ss->nextOid = InvalidOid;
	}
	TempcatShared->nprocs = tempcat_total_procs();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		st->databaseId = InvalidOid;
		pg_atomic_init_u32(&st->frozenXid, InvalidTransactionId);
		pg_atomic_init_u32(&st->minMulti, InvalidMultiXactId);
	}
}

static int
tempcat_tranche_id(void)
{
	return TempcatShared->tranche_id;
}

static inline TempcatProcState *
tempcat_my_state(void)
{
	Assert(MyProc->pgprocno < TempcatShared->nprocs);
	return &TempcatShared->procs[MyProc->pgprocno];
}

/* ----------------------------------------------------------------
 * The session's area
 * ----------------------------------------------------------------
 */

/*
 * The area does not grow into the last quarter of the space for dynamic
 * shared memory (/dev/shm with the default dynamic_shared_memory_type =
 * posix, 64 MB in a default Docker container): other users of DSM (e.g.
 * combo CIDs shared with reader gangs) need some too.  Nor into the last
 * quarter of the slots of the DSM control segment, which every segment
 * takes one of.
 */
#define TEMPCAT_DSA_FIRST_SEGMENT	(1024 * 1024)
#define TEMPCAT_DSM_HEADROOM_DIV	4

/* Would creating 'needed' bytes of segments eat into the headroom? */
static bool
tempcat_dsm_space_low(size_t needed)
{
	Size		free_bytes;
	Size		total_bytes;

#ifdef FAULT_INJECTOR
	/* Test hook: pretend that DSM space is short. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_dsm_space_low") == FaultInjectorTypeSkip)
		return true;
#endif
	return dsm_impl_free_space(&free_bytes, &total_bytes) &&
		free_bytes < needed + total_bytes / TEMPCAT_DSM_HEADROOM_DIV;
}

/* Would creating a segment eat into the slots left to other users? */
static bool
tempcat_dsm_slots_low(void)
{
	uint32		nfree;
	uint32		nmax;

	return dsm_free_slots(&nfree, &nmax) &&
		nfree <= nmax / TEMPCAT_DSM_HEADROOM_DIV;
}

static inline size_t
tempcat_memory_bytes(void)
{
	return (size_t) gp_temp_memory_catalog_max_size * 1024;
}

/* The limit of the session's area, as set when it was created */
static size_t tc_area_limit = 0;

/*
 * Did any session keep its temporary objects in memory since the shared
 * memory was set up?  Then the postmaster removes temporary files after a
 * crash (see PostmasterStateMachine()).
 */
bool
tempcat_areas_created(void)
{
	return TempcatShared != NULL && TempcatShared->areas_created;
}

/*
 * Create the session's area (writer only).  Returns false if it cannot be
 * created: no slot of the DSM control segment to spare, or no space.
 *
 * The coordinator decides whether a session keeps its temporary objects in
 * memory, and leaves room to other users of dynamic shared memory
 * ('headroom'): if it cannot, the session keeps them on disk.  A segment
 * cannot decide otherwise, and fails only if the area cannot be created at
 * all.  As the area grows, it leaves the room to others on all nodes.
 *
 * The area's memory is the session's, though shared: charge it to the
 * session's vmem (gp_vmem_protect_limit, resource groups), its first segment
 * here and later ones as the area grows (dsa_set_vmem_tracked()).  Charge it
 * before creating the area, so that a refusal costs nothing;
 * dsa_try_create() can still fail with an error (e.g. out of local memory),
 * which must not leave vmem charged.
 */
static bool
tempcat_create_area(bool headroom)
{
	MemoryContext oldcxt;
	dsa_pointer rootptr;
	dsa_area   *area = NULL;

	Assert(tc_area == NULL && !tempcat_is_reader());

	if (headroom &&
		(tempcat_dsm_slots_low() ||
		 tempcat_dsm_space_low(2 * TEMPCAT_DSA_FIRST_SEGMENT)))
		return false;

	if (VmemTracker_ReserveVmem(TEMPCAT_DSA_FIRST_SEGMENT) != MemoryAllocation_Success)
		return false;
	TempcatShared->areas_created = true;
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	PG_TRY();
	{
		area = dsa_try_create(tempcat_tranche_id());
	}
	PG_CATCH();
	{
		VmemTracker_ReleaseVmem(TEMPCAT_DSA_FIRST_SEGMENT);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcxt);
	if (area == NULL)
	{
		VmemTracker_ReleaseVmem(TEMPCAT_DSA_FIRST_SEGMENT);
		return false;
	}

	dsa_pin_mapping(area);
	dsa_set_vmem_tracked(area);
	tc_area_limit = tempcat_memory_bytes();
	dsa_set_size_limit(area, tc_area_limit);
	{
		Size		free_bytes;
		Size		total_bytes;
		uint32		nfree;
		uint32		nmax;

		if (dsm_impl_free_space(&free_bytes, &total_bytes))
			dsa_set_min_free_space(area, total_bytes / TEMPCAT_DSM_HEADROOM_DIV);
		if (dsm_free_slots(&nfree, &nmax))
			dsa_set_min_free_slots(area, nmax / TEMPCAT_DSM_HEADROOM_DIV);
	}

	/* (in the first segment, which has room for it) */
	{
		int			nhomes = 1;	/* tids' */
		dsa_pointer home;
		int			c;
		int			k;

		for (c = 0; c < TEMPCAT_NCATALOGS; c++)
			for (k = 0; k < TEMPCAT_NKEYS; k++)
				if (tempcat_catalogs[c].keytype[k] != TCK_NONE)
					nhomes++;
		rootptr = dsa_allocate0(area, offsetof(TempcatRoot, home_buckets) +
								sizeof(dsa_pointer) * TEMPCAT_HASH_MIN * nhomes);
		tc_root = dsa_get_address(area, rootptr);

		home = rootptr + offsetof(TempcatRoot, home_buckets);
		for (c = 0; c < TEMPCAT_NCATALOGS; c++)
		{
			for (k = 0; k < TEMPCAT_NKEYS; k++)
			{
				if (tempcat_catalogs[c].keytype[k] == TCK_NONE)
					continue;
				tc_root->catalogs[c].keys[k].home = home;
				home += sizeof(dsa_pointer) * TEMPCAT_HASH_MIN;
			}
		}
		tc_root->tids.home = home;
	}
	LWLockInitialize(&tc_root->lock, tempcat_tranche_id());
	pg_atomic_init_u32(&tc_root->cache_epoch, 0);
	tc_root->writer_pgprocno = MyProc->pgprocno;
	tc_area = area;

	/* Publish the area to the readers of this session. */
	if (SharedLocalSnapshotSlot != NULL)
	{
		SharedLocalSnapshotSlot->tempcat_root = rootptr;
		pg_write_barrier();
		SharedLocalSnapshotSlot->tempcat_handle = dsa_get_handle(tc_area);
	}

	return true;
}

/*
 * Attach to the session's area: readers to the writer's, if it has one.
 * Returns false if there is no area (yet).
 */
static bool
tempcat_attach(void)
{
	static bool tranche_registered = false;
	static LocalTransactionId no_area_lxid = InvalidLocalTransactionId;
	MemoryContext oldcxt;
	dsa_pointer rootptr;
	dsa_handle	handle;

	if (tc_area != NULL)
		return true;

	/*
	 * Once per process, also where no area is to come: pg_stat_activity
	 * names other processes' waits for the lock by this process's tranches.
	 */
	if (unlikely(!tranche_registered))
	{
		if (TempcatShared == NULL)
			return false;
		LWLockRegisterTranche(tempcat_tranche_id(), "tempcat");
		tranche_registered = true;
	}

	if (!tempcat_is_reader())
		return false;

	if (am_cursor_retrieve_handler)
	{
		/*
		 * A retrieve connection outputs the rows of another session's
		 * parallel retrieve cursor and must see that session's temporary
		 * types.  It runs its own transactions, so it sees their committed
		 * catalog rows only, like on disk.  (Do not search the slots, under
		 * their lock, at every catalog scan of a transaction that found no
		 * area.)
		 */
		int			sessionId = RetrieveSessionId();

		if (no_area_lxid == MyProc->lxid && MyProc->lxid != InvalidLocalTransactionId)
			return false;
		if (sessionId < 0 ||
			!SharedSnapshotGetTempcatArea(sessionId, &handle, &rootptr))
		{
			no_area_lxid = MyProc->lxid;
			return false;
		}
	}
	else
	{
		/* (a slot that another session took over is not ours) */
		if (SharedLocalSnapshotSlot == NULL ||
			SharedLocalSnapshotSlot->slotid != gp_session_id)
			return false;
		handle = SharedLocalSnapshotSlot->tempcat_handle;
		if (handle == DSM_HANDLE_INVALID)
			return false;
		pg_read_barrier();
		rootptr = SharedLocalSnapshotSlot->tempcat_root;
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	tc_area = dsa_attach(handle);
	dsa_pin_mapping(tc_area);
	MemoryContextSwitchTo(oldcxt);

	tc_root = dsa_get_address(tc_area, rootptr);
	tc_seen_cache_epoch = pg_atomic_read_u32(&tc_root->cache_epoch);
	return true;
}

/* ----------------------------------------------------------------
 * Slices
 * ----------------------------------------------------------------
 */

/* How long to wait for an earlier holder of a slice to go away */
#define TEMPCAT_CLAIM_TIMEOUT_MS	60000

/* The error a segment refuses a slice with; see tempcat_xact_callback() */
#define TEMPCAT_SLICE_REFUSED		ERRCODE_OBJECT_IN_USE

/*
 * Per slice of the reserved range, what this process knows about it: whether
 * it holds it, whether it ever gave an OID of it (on the coordinator, which
 * then takes it again only when no other is free, see
 * tempcat_claim_free_slice()), and how many of its rows in memory, of any
 * visibility, have an OID of it (see tempcat_count_slice_row()).  Allocated
 * when the process takes its first slice.
 */
#define TCS_HELD		0x01
#define TCS_USED		0x02

static uint8 *tc_slice_flags = NULL;
static int32 *tc_slice_rows = NULL;

/*
 * The highest OID of each slice that this process took hold of for a row in
 * memory, and the highest it had when it last let go of the slice: one not
 * above that comes back, see tempcat_activate().
 */
static Oid *tc_slice_max_oid = NULL;
static Oid *tc_slice_gone_max_oid = NULL;

/*
 * Coordinator: the slice the session takes its OIDs from (or -1), the next
 * OID to give and the last one it may give from it, whether a transaction
 * that gave OIDs of it to segments committed; whether the current one gave
 * any, the start of the statement that gave the first, and the lowest
 * nesting level that gave any that did not roll back (or 0); and whether a
 * segment refused it in the current transaction (see
 * tempcat_xact_callback()).
 */
static int	tc_slice = -1;
static Oid	tc_next_oid = InvalidOid;
static Oid	tc_slice_end = InvalidOid;
static bool tc_slice_kept = false;
static bool tc_slice_given = false;
static TimestampTz tc_slice_given_stmt = 0;
static int	tc_slice_given_level = 0;
static bool tc_slice_refused = false;

/* Slices may have been left without rows; see tempcat_release_unused_slices() */
static bool tc_release_wanted = false;

/* The statistics to send at exit wait for the transaction to roll back */
static bool tc_stats_deferred = false;

static inline Oid
tempcat_slice_base(int slice)
{
	return FirstTempcatObjectId + ((Oid) slice << TEMPCAT_SLICE_BITS);
}

static inline Oid
tempcat_slice_last(int slice)
{
	Oid			base = tempcat_slice_base(slice);

	return Min((uint64) base + TEMPCAT_SLICE_SIZE - 1, (uint64) LastTempcatObjectId);
}

static inline bool
tempcat_holds_slice(int slice)
{
	return tc_slice_flags != NULL && (tc_slice_flags[slice] & TCS_HELD) != 0;
}

static void tempcat_proc_exit(int code, Datum arg);
static void tempcat_before_exit(int code, Datum arg);
static void tempcat_xact_callback(XactEvent event, void *arg);
static void tempcat_subxact_callback(SubXactEvent event,
									 SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 void *arg);
static void tempcat_forget_slice_oids(int slice);
static void tempcat_note_given(void);

/*
 * Register what lets go of the slices and settles the session's rows: before
 * taking a slice, so that it cannot be kept for good.
 */
static void
tempcat_register_callbacks(void)
{
	static bool registered = false;

	if (registered)
		return;
	on_shmem_exit(tempcat_proc_exit, (Datum) 0);
	before_shmem_exit(tempcat_before_exit, (Datum) 0);
	RegisterSubXactCallback(tempcat_subxact_callback, NULL);
	RegisterXactCallback(tempcat_xact_callback, NULL);
	registered = true;
}

/*
 * Before taking hold of a slice: register what lets go of it, and allocate
 * what the process knows per slice, in one piece.  An error once a slice is
 * ours in shared memory, before its flag is set here, would leave it held
 * for good (tempcat_proc_exit() goes by the flags).
 */
static void
tempcat_prepare_to_hold(void)
{
	char	   *chunk;

	tempcat_register_callbacks();
	if (tc_slice_flags != NULL)
		return;
	chunk = MemoryContextAllocZero(TopMemoryContext,
								   (sizeof(int32) + 2 * sizeof(Oid) + sizeof(uint8)) *
								   TEMPCAT_NSLICES);
	tc_slice_rows = (int32 *) chunk;
	tc_slice_max_oid = (Oid *) (tc_slice_rows + TEMPCAT_NSLICES);
	tc_slice_gone_max_oid = tc_slice_max_oid + TEMPCAT_NSLICES;
	tc_slice_flags = (uint8 *) (tc_slice_gone_max_oid + TEMPCAT_NSLICES);
}

/* Note that this process took hold of 'slice'. */
static void
tempcat_hold_slice(int slice)
{
	Assert(tc_slice_flags != NULL);
	tc_slice_flags[slice] |= TCS_HELD;
	TempcatShared->slices[slice].databaseId = MyDatabaseId;

	/*
	 * The row that the slice was taken for may never come (e.g. an error
	 * stops it): let go of the slice then, see tempcat_release_unused_slices().
	 */
	tc_release_wanted = true;
}

/*
 * The coordinator takes hold of a free slice to give its session's OIDs
 * from, when it needs the first one or has given all of the last one (which
 * it keeps holding while objects have OIDs of it).  It takes one it gave
 * OIDs of before only if no other is free: they come back then, and
 * tempcat_activate() makes the processes of the session forget what they
 * cached about them first (see the file header).  It looks for one from
 * where the last search stopped, so that a slice let go of is taken again
 * only after all others were: the writer QEs of the session that held it may
 * still be dropping their objects on the segments, where a new holder would
 * wait for them (tempcat_claim_slice()).  It passes over slices whose OIDs
 * ordinary objects of its database take too many of
 * (tempcat_slice_crowded()).  Returns false if no slice is left.
 *
 * The session starts where the one that held the slice before stopped, so
 * that consecutive sessions keep moving forward rather than reusing recent
 * OIDs; at a random point if no session has used it since the node started,
 * rather than where its users before a restart or a failover started, whose
 * objects may still be kept in writer QEs on the segments.  But if little of
 * the slice is left from there, or if the session used it before, from its
 * start.
 */
static bool
tempcat_claim_free_slice(void)
{
	uint32		me = MyProc->pgprocno + 1;
	bool		reuse_only = false;
	bool		from_base = false;
	int			pass;
	int			i;

#ifdef FAULT_INJECTOR
	/* Test hook: other sessions hold all slices. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_no_free_slice") == FaultInjectorTypeSkip)
		return false;
	/* Test hook: other sessions hold all slices but those we used. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_reuse_slice") == FaultInjectorTypeSkip)
		reuse_only = true;
	/* Test hook: start at the start of the slice. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_slice_from_base") == FaultInjectorTypeSkip)
		from_base = true;
#endif
	tempcat_prepare_to_hold();
#ifdef FAULT_INJECTOR
	/*
	 * Test hook: start the search at the slice taken last, as if all others
	 * had been taken since, to take it again once its holder let go of it
	 * here (see tempcat_claim_slice()).
	 */
	if (SIMPLE_FAULT_INJECTOR("tempcat_claim_slice_again") == FaultInjectorTypeSkip)
		pg_atomic_fetch_sub_u32(&TempcatShared->next_slice, 1);
#endif
	/* (first the slices the session did not use, then any) */
	for (pass = 0; pass < 2; pass++)
	for (i = 0; i < TEMPCAT_NSLICES; i++)
	{
		int			slice = pg_atomic_fetch_add_u32(&TempcatShared->next_slice, 1) %
		TEMPCAT_NSLICES;
		TempcatSliceState *ss = &TempcatShared->slices[slice];
		uint32		expected = 0;
		bool		crowded;
		bool		used;
		Oid			base;
		Oid			last;
		Oid			start;

		used = tc_slice_flags != NULL && (tc_slice_flags[slice] & TCS_USED);
		if ((used && pass == 0) || (!used && reuse_only))
			continue;
		if (!pg_atomic_compare_exchange_u32(&ss->holder, &expected, me))
			continue;

		/*
		 * Ours already while it is checked: an error there, or the exit of
		 * the process (tempcat_proc_exit()), lets go of it.
		 */
		tempcat_hold_slice(slice);
		PG_TRY();
		{
			crowded = tempcat_slice_crowded(slice);
		}
		PG_CATCH();
		{
			tempcat_let_go_of_slice(slice);
			PG_RE_THROW();
		}
		PG_END_TRY();
		if (crowded)
		{
			tempcat_let_go_of_slice(slice);
			continue;
		}

		base = tempcat_slice_base(slice);
		last = tempcat_slice_last(slice);
		start = ss->nextOid;
		if (!OidIsValid(start))
			start = base + (Oid) (random() % (last - base + 1));
		if (start < base || start > last ||
			last - start < (last - base) / 4 || used || from_base)
			start = base;

		/* (the last one, holding OIDs of ours, is let go of once it has none) */
		if (tc_slice >= 0)
			tc_release_wanted = true;
		tc_slice = slice;
		tc_next_oid = start;
		tc_slice_end = last;
		tc_slice_kept = false;
		tc_slice_given = false;
		tc_slice_given_level = 0;
		return true;
	}
	return false;
}

/*
 * A writer QE takes hold of the slice of the OIDs its coordinator assigns,
 * on this node, for as long as its objects have OIDs of it.  The writer QE
 * of an earlier session that held the slice may still be on its way out
 * (e.g. dropping its temporary objects): wait for it.
 */
static void
tempcat_claim_slice(int slice)
{
	TempcatSliceState *ss = &TempcatShared->slices[slice];
	uint32		me = MyProc->pgprocno + 1;
	int			waited = 0;

	for (;;)
	{
		uint32		expected = 0;

		if (pg_atomic_compare_exchange_u32(&ss->holder, &expected, me))
			break;
		if (expected == me)
			break;
#ifdef FAULT_INJECTOR
		/* Test hook: report the wait for the earlier holder. */
		if (waited == 0)
			SIMPLE_FAULT_INJECTOR("tempcat_claim_slice_wait");
		/* Test hook: give up at once. */
		if (waited == 0 &&
			SIMPLE_FAULT_INJECTOR("tempcat_claim_slice_timeout") == FaultInjectorTypeSkip)
			waited = TEMPCAT_CLAIM_TIMEOUT_MS;
#endif
		/* (the coordinator knows the error by its function, see tempcat_note_qe_error()) */
		if (waited >= TEMPCAT_CLAIM_TIMEOUT_MS)
			ereport(ERROR,
					(errcode(TEMPCAT_SLICE_REFUSED),
					 errmsg("an earlier session using the same temporary object OIDs is still running on this node"),
					 errdetail("Process %d holds OIDs %u to %u.",
							   ProcGlobal->allProcs[expected - 1].pid,
							   tempcat_slice_base(slice),
							   tempcat_slice_last(slice)),
					 errhint("Retry once that session has ended.")));
		CHECK_FOR_INTERRUPTS();
		pg_usleep(10000L);
		waited += 10;
	}
	tempcat_hold_slice(slice);
}

/* The OID column and index of the objects of catalog 'classId', if known */
static bool
tempcat_object_oid_index(Oid classId, AttrNumber *oidattr, Oid *indexId)
{
	/* (not in objectaddress.c's ObjectProperty in this version) */
	if (classId == AttrDefaultRelationId)
	{
		*oidattr = Anum_pg_attrdef_oid;
		*indexId = AttrDefaultOidIndexId;
	}
	else if (is_objectclass_supported(classId))
	{
		*oidattr = get_object_attnum_oid(classId);
		*indexId = get_object_oid_index(classId);
	}
	else
		return false;
	return true;
}

/*
 * Do ordinary objects of this database (created before the range was
 * reserved, or carried over by pg_upgrade) take so many OIDs of the slice in
 * one of the catalogs whose objects can be temporary objects in memory that
 * the session could run out of them?  Then it takes another slice.  The
 * scans of the OID indexes over the slice usually find nothing.
 */
#define TEMPCAT_SLICE_CROWDED	(TEMPCAT_SLICE_SIZE / 2)

static bool
tempcat_slice_crowded(int slice)
{
	Oid			lo = tempcat_slice_base(slice);
	Oid			hi = tempcat_slice_last(slice);
	int			limit = TEMPCAT_SLICE_CROWDED;
	int			i;

#ifdef FAULT_INJECTOR
	/* Test hook: one ordinary object is too many. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_slice_crowded") == FaultInjectorTypeSkip)
		limit = 0;
#endif

	for (i = 0; i < lengthof(tempcat_object_classes); i++)
	{
		Oid			classId = tempcat_object_classes[i];
		AttrNumber	oidattr;
		Oid			indexId;
		Relation	rel;
		ScanKeyData key[2];
		SysScanDesc scan;
		HeapTuple	tup;
		int			n = 0;

		if (!tempcat_object_oid_index(classId, &oidattr, &indexId))
			continue;
		rel = table_open(classId, AccessShareLock);
		ScanKeyInit(&key[0], oidattr, BTGreaterEqualStrategyNumber,
					F_OIDGE, ObjectIdGetDatum(lo));
		ScanKeyInit(&key[1], oidattr, BTLessEqualStrategyNumber,
					F_OIDLE, ObjectIdGetDatum(hi));
		scan = systable_beginscan(rel, indexId, true, NULL, 2, key);
		while (n <= limit && HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			if (!IsTempcatTid(&tup->t_self))
				n++;
		}
		systable_endscan(scan);
		table_close(rel, AccessShareLock);
		if (n > limit)
			return true;
	}
	return false;
}

/*
 * The session gets a reserved OID of slice 'slice(oid)' for one of its
 * temporary objects: on the coordinator when it assigns one, on a segment
 * when it receives one.  Take hold of the slice, if not held yet (the
 * coordinator holds the slices it gives OIDs from).
 */
static void
tempcat_activate(Oid oid)
{
	int			slice = tempcat_slice_of(oid);

	if (!tempcat_holds_slice(slice))
	{
		tempcat_prepare_to_hold();
		tempcat_claim_slice(slice);
	}

	/*
	 * The coordinator gives the OIDs of a slice in ascending order, and
	 * starts a slice it takes again at its start: an OID not above the
	 * highest this process had of a slice it let go of comes back.  (Not the
	 * highest so far: a writer QE lets go of a slice that the coordinator
	 * still gives OIDs from as soon as none of its rows has one, and may get
	 * them out of order.)
	 */
	if (oid <= tc_slice_gone_max_oid[slice])
	{
		tempcat_forget_cached_oids(slice);
		tc_slice_gone_max_oid[slice] = InvalidOid;
	}
	if (oid > tc_slice_max_oid[slice])
		tc_slice_max_oid[slice] = oid;

	if (!tc_activated)
	{
		TempcatProcState *st = tempcat_my_state();

		tc_activated = true;
		/* (rows still pending keep theirs, see tempcat_add_pending()) */
		if (tc_npending == 0)
		{
			pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
			pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
		}
		st->databaseId = MyDatabaseId;
	}
}

/*
 * OIDs of 'slice' that this process saw before come back: forget all that it
 * cached about them, with all its system caches (cached plans, PL/pgSQL
 * casts, operators, ...) and the entries of the type cache in the slice,
 * which no invalidation resets (see TypeCacheForgetRange()).  The objects
 * that had them are gone, as the slice was let go of.  The readers of the
 * session do the same at the start of their next transaction
 * (tempcat_start_transaction()).
 */
static void
tempcat_forget_cached_oids(int slice)
{
	elog(DEBUG1, "tempcat: OIDs %u to %u come back, forgetting cached data",
		 tempcat_slice_base(slice), tempcat_slice_last(slice));
	InvalidateSystemCaches();
	/* (ordinary objects with OIDs of the slice may exist and be in use) */
	TypeCacheForgetRange(tempcat_slice_base(slice), tempcat_slice_last(slice),
						 true);
	if (tc_root != NULL)
		pg_atomic_fetch_add_u32(&tc_root->cache_epoch, 1);
}

/*
 * A row in memory with an object OID ('delta' 1) is added, or goes away
 * (-1): count it for the slice of the OID.  A slice that none of our rows
 * has an OID of any more is let go of, see tempcat_release_unused_slices().
 * Enum labels count too: they get OIDs of the slice, though they belong to
 * their type.
 */
static void
tempcat_count_slice_row(TempcatEntry *e, int delta)
{
	Oid			oid;
	int			slice;

	if (!tempcat_catalog_has_oid(e->catidx) &&
		tempcat_catalogs[e->catidx].relid != EnumRelationId)
		return;
	oid = tempcat_entry_oid(e);
	if (!IsTempcatOid(oid))
		return;
	slice = tempcat_slice_of(oid);
	/*
	 * (the OID's slice was taken before: tempcat_activate(), from
	 * tempcat_note_preassigned_oid() or tempcat_note_enum_label_oid())
	 */
	Assert(tempcat_holds_slice(slice));
	if (tc_slice_rows == NULL)
		return;
	tc_slice_rows[slice] += delta;
	Assert(tc_slice_rows[slice] >= 0);
	if (tc_slice_rows[slice] == 0)
		tc_release_wanted = true;
}

/*
 * Let go of the slices that none of our rows in memory has an OID of any
 * more, but the one the coordinator gives OIDs from: its objects were
 * dropped, or their creation rolled back.  When the session goes idle, or
 * starts its next transaction, after the outcome of the finished ones was
 * applied to the rows: no OID that was given is waiting for its row then.
 *
 * A writer QE may let go of a slice its coordinator still gives OIDs from;
 * it takes it again with the next one (tempcat_activate()), as no other
 * session can take it meanwhile.
 */
static void
tempcat_release_unused_slices(bool xact_start)
{
	int			slice;

	/*
	 * The statistics of the objects dropped go before their OIDs can go to
	 * another session (see tempcat_forget_if_gone()): send them now, after
	 * the counts still pending for them, which works out of a transaction or
	 * at its start, before it counts anything (PostgreSQL 15 asserts that
	 * pgstat_report_stat() runs out of a transaction: the purges will have to
	 * go alone then); in an aborted one, leave the slices to the next time
	 * the session goes idle.
	 */
	if (pgstat_have_purges())
	{
		if (!xact_start && IsTransactionOrTransactionBlock())
			return;
		pgstat_report_stat(true);
	}

	tc_release_wanted = false;
	if (tc_slice_flags == NULL)
		return;
	for (slice = 0; slice < TEMPCAT_NSLICES; slice++)
	{
		if ((tc_slice_flags[slice] & TCS_HELD) && slice != tc_slice &&
			tc_slice_rows[slice] == 0)
			tempcat_let_go_of_slice(slice);
	}
}

/*
 * Coordinator: give the next OIDs from another slice.  The current one is
 * let go of once nothing has OIDs of it, see tempcat_release_unused_slices();
 * with the OID of the temporary namespace taken from it ahead, if unused.
 */
static void
tempcat_drop_slice(void)
{
	if (tc_slice < 0)
		return;
	if (OidIsValid(tc_namespace_oid) &&
		tempcat_slice_of(tc_namespace_oid) == tc_slice)
		tc_namespace_oid = InvalidOid;
	tc_slice = -1;
	tc_slice_given = false;
	tc_slice_given_level = 0;
	tc_release_wanted = true;
}

/*
 * The session is reset (e.g. after its gang was lost): its writer QEs are
 * replaced, and the old ones may hold the slice on the segments for a while
 * as they drop their objects, which the new ones would wait for
 * (tempcat_claim_slice()).  Take another slice for the next OIDs; the old
 * one goes once the old objects are dropped on the coordinator too.  Only
 * process-local state changes here: called from GpScheduleSessionReset()
 * and friends, possibly out of a transaction.
 */
void
tempcat_session_reset(void)
{
	tempcat_drop_slice();
}

/*
 * Let go of our slices, after the cleanup of our temporary objects at exit
 * (RemoveTempRelationsCallback() runs earlier).  The locks on the objects
 * they used went with ShutdownPostgres(), after that cleanup too.
 */
static void
tempcat_proc_exit(int code, Datum arg)
{
	TempcatProcState *st = tempcat_my_state();
	int			slice;

	pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
	pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
	st->databaseId = InvalidOid;

	tempcat_forget_used_objects();

	/* (the transaction is rolled back by now, see tempcat_before_exit()) */
	if (tc_stats_deferred)
		pgstat_report_stat(true);

	if (tc_slice_flags == NULL)
		return;
#ifdef FAULT_INJECTOR
	/* Test hook: hold on to the slices on the way out (with 'suspend'). */
	SIMPLE_FAULT_INJECTOR("tempcat_release_slice");
#endif
	for (slice = 0; slice < TEMPCAT_NSLICES; slice++)
	{
		if (tc_slice_flags[slice] & TCS_HELD)
			tempcat_let_go_of_slice(slice);
	}
}

/*
 * Let go of a slice, and of the OIDs of it that we owned (given to objects
 * whose creation did not get as far as their rows, if any).
 */
static void
tempcat_let_go_of_slice(int slice)
{
	TempcatSliceState *ss = &TempcatShared->slices[slice];

	Assert(tempcat_holds_slice(slice));
	ss->databaseId = InvalidOid;
	pg_write_barrier();
	pg_atomic_write_u32(&ss->holder, 0);
	tc_slice_flags[slice] &= ~TCS_HELD;
	if (tc_slice_max_oid[slice] > tc_slice_gone_max_oid[slice])
		tc_slice_gone_max_oid[slice] = tc_slice_max_oid[slice];
	if (slice == tc_slice)
		tc_slice = -1;
	if (!proc_exit_inprogress)
		tempcat_forget_slice_oids(slice);
}

/* ----------------------------------------------------------------
 * Ownership
 * ----------------------------------------------------------------
 */

/* OIDs assigned by this session to its temporary objects, per catalog. */
typedef struct TempcatOwnedKey
{
	Oid			catalog;
	Oid			oid;
} TempcatOwnedKey;

static void
tempcat_remember_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TempcatOwnedKey);
		ctl.entrysize = sizeof(TempcatOwnedKey);
		ctl.hcxt = TopMemoryContext;
		tc_owned = hash_create("tempcat owned OIDs", 256, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	key.catalog = catalog;
	key.oid = oid;
	(void) hash_search(tc_owned, &key, HASH_ENTER, NULL);
}

/* Returns whether we owned it. */
static bool
tempcat_forget_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL)
		return false;
	key.catalog = catalog;
	key.oid = oid;
	return hash_search(tc_owned, &key, HASH_REMOVE, NULL) != NULL;
}

/* Forget the OIDs of 'slice' that we owned; see tempcat_let_go_of_slice(). */
static void
tempcat_forget_slice_oids(int slice)
{
	HASH_SEQ_STATUS seq;
	TempcatOwnedKey *owned;

	if (tc_owned == NULL)
		return;
	hash_seq_init(&seq, tc_owned);
	while ((owned = hash_seq_search(&seq)) != NULL)
	{
		if (IsTempcatOid(owned->oid) && tempcat_slice_of(owned->oid) == slice)
			(void) hash_search(tc_owned, owned, HASH_REMOVE, NULL);
	}
}

/*
 * Did this session assign 'oid' to one of its temporary objects in
 * 'catalog'?
 */
bool
tempcat_owns_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL || !IsTempcatOid(oid))
		return false;
	key.catalog = catalog;
	key.oid = oid;
	return hash_search(tc_owned, &key, HASH_FIND, NULL) != NULL;
}

/*
 * The state of the writer whose area a reader looks at: NULL if not known
 * (a parallel retrieve connection to a session without an area).
 */
static TempcatProcState *
tempcat_writer_state(void)
{
	if (am_cursor_retrieve_handler)
		return tempcat_attach() ?
			&TempcatShared->procs[tc_root->writer_pgprocno] : NULL;
	if (SharedLocalSnapshotSlot == NULL ||
		SharedLocalSnapshotSlot->writer_proc == NULL)
		return NULL;
	return &TempcatShared->procs[SharedLocalSnapshotSlot->writer_proc->pgprocno];
}

/* ----------------------------------------------------------------
 * Published horizon
 * ----------------------------------------------------------------
 */

/* Lower the published horizon to cover 'xid'.  Lowering is always safe. */
static void
tempcat_cover_xid(TransactionId xid)
{
	TempcatProcState *st;
	TransactionId cur;

	if (!TransactionIdIsNormal(xid))
		return;
	st = tempcat_my_state();
	cur = pg_atomic_read_u32(&st->frozenXid);
	if (!TransactionIdIsValid(cur) || TransactionIdPrecedes(xid, cur))
		pg_atomic_write_u32(&st->frozenXid, xid);
}

/*
 * Lower the published horizon to cover a pg_class row of ours.  Lowering is
 * always safe; raising waits for tempcat_recompute_horizon().
 */
static void
tempcat_cover_pg_class_row(HeapTupleHeader hdr)
{
	Form_pg_class form = (Form_pg_class) ((char *) hdr + hdr->t_hoff);
	TempcatProcState *st = tempcat_my_state();
	MultiXactId mxid = form->relminmxid;

	tempcat_cover_xid(form->relfrozenxid);
	if (MultiXactIdIsValid(mxid))
	{
		MultiXactId cur = pg_atomic_read_u32(&st->minMulti);

		if (!MultiXactIdIsValid(cur) || MultiXactIdPrecedes(mxid, cur))
			pg_atomic_write_u32(&st->minMulti, mxid);
	}
}

/*
 * Fold the horizons published by sessions of this database into the result
 * of vac_update_datfrozenxid()'s pg_class scan.
 *
 * A temporary table created concurrently is not a problem: its relfrozenxid
 * is not older than the oldest xmin the caller started from.
 */
void
tempcat_fold_frozen_horizon(TransactionId *frozenXid, MultiXactId *minMulti)
{
	int			i;

	if (TempcatShared == NULL)
		return;

	pg_read_barrier();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];
		TransactionId xid;
		MultiXactId mxid;

		if (st->databaseId != MyDatabaseId)
			continue;

		xid = pg_atomic_read_u32(&st->frozenXid);
		mxid = pg_atomic_read_u32(&st->minMulti);
		if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, *frozenXid))
			*frozenXid = xid;
		if (MultiXactIdIsValid(mxid) && MultiXactIdPrecedes(mxid, *minMulti))
			*minMulti = mxid;
	}
}

/* ----------------------------------------------------------------
 * Ordinary objects that temporary objects use
 *
 * Temporary objects kept in memory depend on ordinary objects (the types of
 * their columns, the functions of their defaults, their owners, ...) through
 * pg_depend and pg_shdepend rows in memory, which other sessions cannot see.
 * Instead, the session counts itself among the users of each such object in
 * shared memory (TempcatUsedObjects) for as long as its rows refer to it.
 * DROP and the ALTERs that look for dependent objects
 * (tempcat_lock_out_users()) fail if other sessions use the object, as they
 * would on dependent objects they could see, naming the sessions.
 * (CASCADE cannot drop objects in other sessions' memory.)  Otherwise they
 * mark the object with their transaction, which a session that comes to
 * use it meanwhile waits for, as for a lock.  All on the coordinator only,
 * where all DDL starts.
 *
 * Columns are told apart as on disk, where DROP COLUMN and ALTER COLUMN TYPE
 * look at the dependencies on that column only, and DROP of the relation at
 * all of them: a dependency on a column counts as a use of the column and as
 * a use of the relation.  So a temporary view does not keep other sessions
 * from dropping or changing the columns it does not use.
 *
 * Nothing of this takes room in the lock table, which a session using many
 * objects for a long time would fill.  Only when TempcatUsedObjects is full
 * does a session lock the objects it cannot count there instead: with a
 * session-level lock of USER_LOCKMETHOD (session-level locks of
 * DEFAULT_LOCKMETHOD go when a transaction aborts), whose tag is that of a
 * lock on the object itself in the other lock method; and DROP and friends
 * try to lock every object they check, as long as such a session has an
 * object locked instead of counted (tempcat_recompute_used()).  At
 * exit, the session counts itself out after its temporary objects are
 * dropped (tempcat_proc_exit() runs after RemoveTempRelationsCallback()).
 * ----------------------------------------------------------------
 */

typedef struct TempcatUsedKey
{
	Oid			classId;
	Oid			objectId;
	int32		objectSubId;	/* a column, or 0 for the whole object */
} TempcatUsedKey;

typedef struct TempcatUsed
{
	TempcatUsedKey key;
	bool		counted;		/* in TempcatUsedObjects */
	bool		locked;			/* or locked instead, see above */
} TempcatUsed;

/* Some of tc_used could not be counted, see above */
static bool tc_used_overflow = false;

/* The lock modes when TempcatUsedObjects is full: users share theirs */
#define TEMPCAT_USER_LOCKMODE		ShareLock
#define TEMPCAT_CHANGER_LOCKMODE	RowExclusiveLock

static inline Oid
tempcat_used_database(Oid classId)
{
	return IsSharedRelation(classId) ? InvalidOid : MyDatabaseId;
}

static void
tempcat_used_locktag(LOCKTAG *tag, Oid classId, Oid objectId, int32 subId)
{
	SET_LOCKTAG_OBJECT(*tag, tempcat_used_database(classId),
					   classId, objectId, subId);
	tag->locktag_lockmethodid = USER_LOCKMETHOD;
}

static void
tempcat_used_object_key(TempcatUsedObject *key,
						Oid classId, Oid objectId, int32 subId)
{
	memset(key, 0, sizeof(*key));
	key->databaseId = tempcat_used_database(classId);
	key->classId = classId;
	key->objectId = objectId;
	key->objectSubId = subId;
}

/* Is it an object whose use tempcat_use_object() counts? */
static bool
tempcat_lockable_use(Oid classId, Oid objectId)
{
	return Gp_role != GP_ROLE_EXECUTE &&
		!tempcat_owns_oid(classId, objectId);
}

/* Is a DROP or ALTER of another (sub)transaction in progress marked? */
static bool
tempcat_changer_active(TempcatUsedObjectEntry *entry)
{
	return TransactionIdIsValid(entry->changer_xid) &&
		!TransactionIdIsCurrentTransactionId(entry->changer_xid) &&
		TransactionIdIsInProgress(entry->changer_xid);
}

/*
 * Count the session among the users of an object (or column) in
 * TempcatUsedObjects; after waiting for a DROP or ALTER of it in progress
 * that marked it, if any (see tempcat_others_use(); the recorded dependency
 * may then refer to an object that is gone, as with any dependency on an
 * object that is being dropped).  Returns false if TempcatUsedObjects is
 * full.
 */
static bool
tempcat_count_use(Oid classId, Oid objectId, int32 subId)
{
	TempcatUsedObject key;

#ifdef FAULT_INJECTOR
	/* Test hook: TempcatUsedObjects is full. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_used_objects_full") == FaultInjectorTypeSkip)
		return false;
#endif
	tempcat_used_object_key(&key, classId, objectId, subId);
	for (;;)
	{
		TempcatUsedObjectEntry *entry;
		TransactionId changer = InvalidTransactionId;
		bool		found;
		int			i;

		LWLockAcquire(&TempcatShared->used_lock, LW_EXCLUSIVE);
		entry = hash_search(TempcatUsedObjects, &key, HASH_ENTER_NULL, &found);
		if (entry == NULL)
		{
			LWLockRelease(&TempcatShared->used_lock);
			return false;
		}
		if (!found)
		{
			entry->nsessions = 0;
			for (i = 0; i < TEMPCAT_USED_PROCS; i++)
				entry->procs[i] = -1;
			entry->changer_xid = InvalidTransactionId;
			pg_atomic_fetch_add_u32(&TempcatShared->nused_entries, 1);
		}
		else if (tempcat_changer_active(entry))
			changer = entry->changer_xid;
		if (!TransactionIdIsValid(changer))
		{
			entry->nsessions++;
			for (i = 0; i < TEMPCAT_USED_PROCS; i++)
			{
				if (entry->procs[i] < 0)
				{
					entry->procs[i] = MyProc->pgprocno;
					break;
				}
			}
		}
		LWLockRelease(&TempcatShared->used_lock);
		if (!TransactionIdIsValid(changer))
			return true;
		XactLockTableWait(changer, NULL, NULL, XLTW_None);
	}
}

static void
tempcat_uncount_use(TempcatUsed *used)
{
	TempcatUsedObject key;
	TempcatUsedObjectEntry *entry;
	int			i;

	if (!used->counted)
		return;
	tempcat_used_object_key(&key, used->key.classId, used->key.objectId,
							used->key.objectSubId);
	LWLockAcquire(&TempcatShared->used_lock, LW_EXCLUSIVE);
	entry = hash_search(TempcatUsedObjects, &key, HASH_FIND, NULL);
	if (entry != NULL)
	{
		for (i = 0; i < TEMPCAT_USED_PROCS; i++)
		{
			if (entry->procs[i] == MyProc->pgprocno)
				entry->procs[i] = -1;
		}
		if (--entry->nsessions <= 0)
		{
			(void) hash_search(TempcatUsedObjects, &key, HASH_REMOVE, NULL);
			pg_atomic_fetch_sub_u32(&TempcatShared->nused_entries, 1);
		}
	}
	LWLockRelease(&TempcatShared->used_lock);
	used->counted = false;
}

/*
 * Count the use of an object, or of one of its columns, if not counted yet;
 * or lock it if TempcatUsedObjects is full.  Noted in tc_used first, so that
 * an error on the way (e.g. a cancel of the wait for a DROP in progress, see
 * tempcat_count_use()) leaves nothing that tempcat_recompute_used() does not
 * undo.  An object noted already but neither counted nor locked is one
 * whose wait failed in a subtransaction that rolled back, which
 * tempcat_recompute_used() has not looked at yet.
 */
static void
tempcat_use_object_part(Oid classId, Oid objectId, int32 subId)
{
	TempcatUsedKey key;
	TempcatUsed *used;
	bool		found;
	LOCKTAG		tag;

	if (tc_used == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TempcatUsedKey);
		ctl.entrysize = sizeof(TempcatUsed);
		ctl.hcxt = TopMemoryContext;
		tc_used = hash_create("tempcat used objects", 64, &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	memset(&key, 0, sizeof(key));
	key.classId = classId;
	key.objectId = objectId;
	key.objectSubId = subId;
	used = hash_search(tc_used, &key, HASH_ENTER, &found);
	if (!found)
	{
		used->counted = false;
		used->locked = false;
	}
	tempcat_used_locktag(&tag, classId, objectId, subId);
	if (used->counted || (used->locked && LockHeldByMe(&tag, TEMPCAT_USER_LOCKMODE)))
		return;

	if (!used->locked && tempcat_count_use(classId, objectId, subId))
	{
		used->counted = true;
		return;
	}

	/* TempcatUsedObjects is full: lock it instead, see above */
	if (!tc_used_overflow)
	{
		tc_used_overflow = true;
		pg_atomic_fetch_add_u32(&TempcatShared->nused_overflow, 1);
	}
	used->locked = true;
	(void) LockAcquire(&tag, TEMPCAT_USER_LOCKMODE, true, false);
}

/*
 * A temporary object of ours kept in memory comes to depend on an ordinary
 * object, or on a column of it ('subId'): count its use, and that of the
 * whole object for a column.  Before the dependency is recorded, so that a
 * DROP of the object under way finishes first.
 */
static void
tempcat_use_object(Oid classId, Oid objectId, int32 subId)
{
	if (!tempcat_lockable_use(classId, objectId))
		return;
	tempcat_use_object_part(classId, objectId, 0);
	if (subId != 0)
		tempcat_use_object_part(classId, objectId, subId);
}

/* A pg_depend or pg_shdepend row of ours goes to memory: see above. */
static void
tempcat_use_referenced(Relation rel, HeapTuple tup)
{
	TupleDesc	desc = RelationGetDescr(rel);

	if (RelationGetRelid(rel) == DependRelationId ||
		RelationGetRelid(rel) == SharedDependRelationId)
		tc_depend_level = Max(tc_depend_level, GetCurrentTransactionNestLevel());
	if (RelationGetRelid(rel) == DependRelationId)
	{
		bool		isnull;
		Datum		subid = heap_getattr(tup, Anum_pg_depend_refobjsubid,
										 desc, &isnull);

		tempcat_use_object(tempcat_getoid(tup, desc, Anum_pg_depend_refclassid),
						   tempcat_getoid(tup, desc, Anum_pg_depend_refobjid),
						   isnull ? 0 : DatumGetInt32(subid));
	}
	else if (RelationGetRelid(rel) == SharedDependRelationId)
		tempcat_use_object(tempcat_getoid(tup, desc, Anum_pg_shdepend_refclassid),
						   tempcat_getoid(tup, desc, Anum_pg_shdepend_refobjid),
						   0);
}

/* The session uses the object no more. */
static void
tempcat_unuse(TempcatUsed *used)
{
	LOCKTAG		tag;

	tempcat_uncount_use(used);
	if (used->locked)
	{
		tempcat_used_locktag(&tag, used->key.classId, used->key.objectId,
							 used->key.objectSubId);
		if (LockHeldByMe(&tag, TEMPCAT_USER_LOCKMODE))
			LockRelease(&tag, TEMPCAT_USER_LOCKMODE, true);
		used->locked = false;
	}
}

/*
 * Count the session out of the users of the objects that our pg_depend and
 * pg_shdepend rows in memory no longer refer to: after some of the rows went
 * away, or after an error, which may have come after an object was counted
 * and before its row was added (e.g. as the area was full).  Rows count of
 * any visibility, unless their insert rolled back or their delete committed;
 * when 'committing', also if the transaction ending now deleted them.
 *
 * Also at the end of a transaction, or of a subtransaction that rolls back,
 * that changed such rows (tempcat_xact_callback()): before its locks go, so
 * that a DROP that waited for them does not find the session among the
 * users any more.  Without raising an error, which an abort could not
 * handle: if memory runs short, the next try does it (at idle, or at the
 * start of the next transaction), as after any other error here.
 */
static int
tempcat_used_key_cmp(const void *a, const void *b)
{
	return memcmp(a, b, sizeof(TempcatUsedKey));
}

static bool
tempcat_entry_gone(TempcatEntry *e, bool committing)
{
	if (!(e->flags & TCE_XMIN_COMMITTED) && TransactionIdIsNormal(e->xmin) &&
		!TransactionIdIsCurrentTransactionId(e->xmin) &&
		!TransactionIdIsInProgress(e->xmin) && !TransactionIdDidCommit(e->xmin))
		return true;
	if (TransactionIdIsValid(e->xmax))
	{
		if (TransactionIdIsCurrentTransactionId(e->xmax))
			return committing;
		if (!TransactionIdIsInProgress(e->xmax) &&
			TransactionIdDidCommit(e->xmax))
			return true;
	}
	return false;
}

/* the array of tempcat_recompute_used(), if an error left it */
static TempcatUsedKey *tc_recount_keys = NULL;

static void
tempcat_recompute_used(bool committing)
{
	static const Oid catalogs[] = {DependRelationId, SharedDependRelationId};
	TempcatUsedKey *still;
	int			nstill = 0;
	int			maxstill = 0;
	HASH_SEQ_STATUS seq;
	TempcatUsed *used;
	int			i;

	if (tc_used == NULL || hash_get_num_entries(tc_used) == 0 || tc_area == NULL)
	{
		tc_used_dirty = false;
		return;
	}

	for (i = 0; i < lengthof(catalogs); i++)
		maxstill += 2 * tc_root->catalogs[tempcat_catalog_index(catalogs[i])].nentries;
	/*
	 * malloc(), not palloc(): MCXT_ALLOC_NO_OOM does not keep gp_malloc()
	 * from raising an error when vmem or the resource group's memory runs
	 * short (gp_failed_to_alloc()).
	 */
	if (tc_recount_keys != NULL)
		free(tc_recount_keys);
	still = tc_recount_keys = malloc(sizeof(TempcatUsedKey) * Max(maxstill, 1));
	if (still == NULL)
	{
		tc_used_dirty = true;
		return;
	}
	for (i = 0; i < lengthof(catalogs); i++)
	{
		int			catidx = tempcat_catalog_index(catalogs[i]);
		dsa_pointer ep;

		for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
		{
			TempcatEntry *e = TC_ADDR(ep);
			HeapTupleHeader hdr = TCE_DATA(e);
			TempcatUsedKey *key;

			ep = e->next;
			if (tempcat_entry_gone(e, committing))
				continue;
			key = &still[nstill++];
			memset(key, 0, sizeof(*key));
			if (catalogs[i] == DependRelationId)
			{
				Form_pg_depend form = (Form_pg_depend) ((char *) hdr + hdr->t_hoff);

				key->classId = form->refclassid;
				key->objectId = form->refobjid;
				if (form->refobjsubid != 0)
				{
					still[nstill] = *key;
					still[nstill++].objectSubId = form->refobjsubid;
				}
			}
			else
			{
				Form_pg_shdepend form = (Form_pg_shdepend) ((char *) hdr + hdr->t_hoff);

				key->classId = form->refclassid;
				key->objectId = form->refobjid;
			}
		}
	}
	Assert(nstill <= maxstill);
	qsort(still, nstill, sizeof(TempcatUsedKey), tempcat_used_key_cmp);

	hash_seq_init(&seq, tc_used);
	while ((used = hash_seq_search(&seq)) != NULL)
	{
		TempcatUsedKey key;

		memset(&key, 0, sizeof(key));
		key.classId = used->key.classId;
		key.objectId = used->key.objectId;
		key.objectSubId = used->key.objectSubId;
		if (bsearch(&key, still, nstill, sizeof(TempcatUsedKey),
					tempcat_used_key_cmp) != NULL)
			continue;
		tempcat_unuse(used);
		(void) hash_search(tc_used, &used->key, HASH_REMOVE, NULL);
	}
	free(still);
	tc_recount_keys = NULL;

	/* Nothing locked instead of counted any more: DROP need not try locks */
	if (tc_used_overflow)
	{
		bool		locked = false;

		hash_seq_init(&seq, tc_used);
		while ((used = hash_seq_search(&seq)) != NULL)
		{
			if (used->locked)
				locked = true;
		}
		if (!locked)
		{
			pg_atomic_fetch_sub_u32(&TempcatShared->nused_overflow, 1);
			tc_used_overflow = false;
		}
	}
	tc_used_dirty = false;
}

/*
 * At exit: the session uses nothing any more.  (Locks taken instead went
 * with ShutdownPostgres().)
 */
static void
tempcat_forget_used_objects(void)
{
	HASH_SEQ_STATUS seq;
	TempcatUsed *used;

	if (tc_used != NULL)
	{
		hash_seq_init(&seq, tc_used);
		while ((used = hash_seq_search(&seq)) != NULL)
			tempcat_uncount_use(used);
	}
	if (tc_used_overflow)
	{
		pg_atomic_fetch_sub_u32(&TempcatShared->nused_overflow, 1);
		tc_used_overflow = false;
	}
}

/*
 * Take the locks again that pg_advisory_unlock_all() released, which
 * releases all session-level locks of USER_LOCKMETHOD, of the objects locked
 * instead of counted (see above).  (DISCARD ALL does too, but drops the
 * temporary objects right after.)  When the session goes idle, as it may do
 * so only after such a call; without waiting, as a DROP that took a lock
 * meanwhile cannot be stopped any more, and without an error when the lock
 * table is full, as no lock is taken then either.
 */
static void
tempcat_relock_used(void)
{
	HASH_SEQ_STATUS seq;
	TempcatUsed *used;

	if (!tc_used_overflow)
		return;
	hash_seq_init(&seq, tc_used);
	while ((used = hash_seq_search(&seq)) != NULL)
	{
		LOCKTAG		tag;

		if (!used->locked)
			continue;
		tempcat_used_locktag(&tag, used->key.classId, used->key.objectId,
							 used->key.objectSubId);
		if (!LockHeldByMe(&tag, TEMPCAT_USER_LOCKMODE))
			(void) LockAcquireExtended(&tag, TEMPCAT_USER_LOCKMODE, true, true,
									   false, NULL);
	}
}

/*
 * Append the processes that hold the lock on an object instead, but ours;
 * only those connected to our database if 'this_db'.  Returns whether it
 * found any.
 */
static bool
tempcat_locker_pids(const LOCKTAG *tag, bool this_db, StringInfo pids)
{
	LockData   *data = GetLockStatusData();
	bool		found = false;
	int			i;

	for (i = 0; i < data->nelements; i++)
	{
		LockInstanceData *inst = &data->locks[i];

		if (inst->pid == MyProcPid ||
			!(inst->holdMask & LOCKBIT_ON(TEMPCAT_USER_LOCKMODE)) ||
			inst->locktag.locktag_field1 != tag->locktag_field1 ||
			inst->locktag.locktag_field2 != tag->locktag_field2 ||
			inst->locktag.locktag_field3 != tag->locktag_field3 ||
			inst->locktag.locktag_field4 != tag->locktag_field4 ||
			inst->locktag.locktag_type != tag->locktag_type ||
			inst->locktag.locktag_lockmethodid != tag->locktag_lockmethodid)
			continue;
		if (this_db)
		{
			PGPROC	   *proc = BackendPidGetProc(inst->pid);

			if (proc == NULL || proc->databaseId != MyDatabaseId)
				continue;
		}
		appendStringInfo(pids, "%s%d", pids->len > 0 ? ", " : "", inst->pid);
		found = true;
	}
	return found;
}

/*
 * Do temporary objects of other sessions use the object (or column)?  If
 * they do, return the processes of some of those sessions in 'pids'.  Our
 * own uses do not count, and our rows can be seen.  With 'this_db', only
 * sessions connected to our database count, as far as can be told (only
 * some processes of the sessions are noted).  If not, and 'mark', and
 * our session uses it, mark it with the current (sub)transaction: until it
 * ends, a session that comes to use it waits (see tempcat_count_use()).  An
 * object that nobody uses, as most are, is not marked, so that DROP does not
 * take the lock in exclusive mode for every object: a session that comes to
 * use it meanwhile records a dependency on an object that may be gone, as on
 * disk for objects that are not locked while used, like types and functions.
 */
static bool
tempcat_others_use(Oid classId, Oid objectId, int32 subId, bool mark,
				   bool this_db, StringInfo pids)
{
	TempcatUsedObject key;
	TempcatUsedObjectEntry *entry;
	TransactionId xid = InvalidTransactionId;
	bool		used = false;

	if (TempcatShared == NULL || !tempcat_lockable_use(classId, objectId))
		return false;

	/* Objects that sessions could not count, but locked instead */
	if (pg_atomic_read_u32(&TempcatShared->nused_overflow) > 0)
	{
		LOCKTAG		tag;

		tempcat_used_locktag(&tag, classId, objectId, subId);
		if (LockAcquire(&tag, TEMPCAT_CHANGER_LOCKMODE, false, true) ==
			LOCKACQUIRE_NOT_AVAIL &&
			(tempcat_locker_pids(&tag, this_db, pids) || !this_db))
			used = true;
	}

	/*
	 * Most objects nobody uses, and nobody uses any while the feature is not
	 * used: look without a lock, then in shared mode, before taking it in
	 * exclusive mode, as DROP comes here for every object it drops.
	 */
	if (pg_atomic_read_u32(&TempcatShared->nused_entries) == 0)
		return used;
	tempcat_used_object_key(&key, classId, objectId, subId);
	LWLockAcquire(&TempcatShared->used_lock, LW_SHARED);
	entry = hash_search(TempcatUsedObjects, &key, HASH_FIND, NULL);
	LWLockRelease(&TempcatShared->used_lock);
	if (entry == NULL)
		return used;

	if (mark)
		xid = GetCurrentTransactionId();
	LWLockAcquire(&TempcatShared->used_lock, LW_EXCLUSIVE);
	entry = hash_search(TempcatUsedObjects, &key, HASH_FIND, NULL);
	if (entry != NULL)
	{
		TempcatUsedKey ukey;
		TempcatUsed *mine = NULL;
		int			others;
		int			i;

		if (tc_used != NULL)
		{
			memset(&ukey, 0, sizeof(ukey));
			ukey.classId = classId;
			ukey.objectId = objectId;
			ukey.objectSubId = subId;
			mine = hash_search(tc_used, &ukey, HASH_FIND, NULL);
		}
		others = entry->nsessions - ((mine != NULL && mine->counted) ? 1 : 0);
		if (others > 0)
		{
			int			noted = 0;
			int			here = 0;

			for (i = 0; i < TEMPCAT_USED_PROCS; i++)
			{
				int			procno = entry->procs[i];
				PGPROC	   *proc;

				if (procno < 0 || procno == MyProc->pgprocno)
					continue;
				noted++;
				proc = &ProcGlobal->allProcs[procno];
				if (this_db && proc->databaseId != MyDatabaseId)
					continue;
				here++;
				appendStringInfo(pids, "%s%d", pids->len > 0 ? ", " : "",
								 proc->pid);
			}
			/* (sessions not noted may be connected to any database) */
			if (!this_db || here > 0 || others > noted)
				used = true;
		}
		else if (mark && !used)
			entry->changer_xid = xid;
	}
	LWLockRelease(&TempcatShared->used_lock);
	return used;
}

/*
 * The caller is about to drop an object, or a column of it ('subId'), or to
 * change it in a way that objects using it must not miss: fail if temporary
 * objects of other sessions kept in memory use it (see
 * tempcat_use_object()), as for dependent objects on disk.  A whole object
 * is used by them if they use any of its columns.
 */
void
tempcat_lock_out_users(Oid classId, Oid objectId, int32 subId)
{
	StringInfoData pids;
	ObjectAddress object;

	initStringInfo(&pids);
	if (!tempcat_others_use(classId, objectId, subId, true, false, &pids))
	{
		pfree(pids.data);
		return;
	}
	ObjectAddressSubSet(object, classId, objectId, subId);
	ereport(ERROR,
			(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
			 errmsg("temporary objects of other sessions depend on %s",
					getObjectDescription(&object)),
			 pids.len > 0 ?
			 errdetail("The sessions of processes %s keep them in memory.", pids.data) : 0,
			 errhint("Retry once those sessions have dropped them, or have ended.")));
}

/*
 * DROP OWNED and REASSIGN OWNED ('command') do not reach the temporary
 * objects of other sessions kept in memory: warn if any of a session
 * connected to our database depend on the role, as these commands act in
 * our database only.  (DROP ROLE then fails, see tempcat_lock_out_users().)
 */
void
tempcat_warn_users(Oid classId, Oid objectId, const char *command)
{
	StringInfoData pids;
	ObjectAddress object;

	initStringInfo(&pids);
	if (!tempcat_others_use(classId, objectId, 0, false, true, &pids))
	{
		pfree(pids.data);
		return;
	}
	ObjectAddressSet(object, classId, objectId);
	ereport(WARNING,
			(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
			 errmsg("%s does not reach the temporary objects of other sessions that depend on %s",
					command, getObjectDescription(&object)),
			 pids.len > 0 ?
			 errdetail("The sessions of processes %s keep them in memory.", pids.data) : 0));
	pfree(pids.data);
}
/* ----------------------------------------------------------------
 * OID assignment
 * ----------------------------------------------------------------
 */

/*
 * Does the dispatch key describe a temporary object of this session?
 * 'new_namespace' says whether a temporary namespace may be created in
 * memory.
 */
static bool
tempcat_key_is_temp(Oid catalog, const char *objname, Oid namespaceOid,
					Oid keyOid1, Oid keyOid2, bool new_namespace)
{
	switch (catalog)
	{
		case NamespaceRelationId:
			return new_namespace && objname != NULL &&
				(strncmp(objname, "pg_temp_", 8) == 0 ||
				 strncmp(objname, "pg_toast_temp_", 14) == 0);

		case RelationRelationId:
		case TypeRelationId:
		case ProcedureRelationId:
		case OperatorRelationId:
			return tempcat_owns_oid(NamespaceRelationId, namespaceOid);

		case AttrDefaultRelationId:
		case RewriteRelationId:
		case TriggerRelationId:
		case PolicyRelationId:
			return tempcat_owns_oid(RelationRelationId, keyOid1);

		case ConstraintRelationId:
			return tempcat_owns_oid(RelationRelationId, keyOid1) ||
				tempcat_owns_oid(TypeRelationId, keyOid2);

		default:
			return false;
	}
}

/*
 * Can this session keep its temporary objects in memory?  Decided when its
 * temporary namespace is created on the coordinator.
 */
static bool
tempcat_can_start(void)
{
	/*
	 * A utility-mode session on a segment keeps its temporary objects on
	 * disk: OIDs from the reserved range could be the ones that the
	 * coordinator gives the temporary objects of its sessions on this
	 * segment.
	 */
	if (Gp_role == GP_ROLE_UTILITY && !IS_QUERY_DISPATCHER())
		return false;
	if (IsBinaryUpgrade || !IsUnderPostmaster || TempcatShared == NULL)
		return false;

	/*
	 * A slice of the reserved range, and the memory to keep them in: the
	 * slice first, so that no area is made for nothing when there is none,
	 * and if no area can be made, the slice goes back unused.
	 */
	if (tc_slice < 0 && !tempcat_claim_free_slice())
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no range of OIDs for temporary objects kept in memory is free"),
				 errdetail("Other sessions hold all %d ranges, or ordinary objects take too many of their OIDs; the session keeps its temporary objects on disk.",
						   TEMPCAT_NSLICES)));
		return false;
	}
	if (tc_area == NULL && !tempcat_create_area(true))
	{
		/* (no area, so nothing has OIDs of the slice) */
		if (tc_slice >= 0)
			tempcat_let_go_of_slice(tc_slice);
		ereport(WARNING,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not create the in-memory catalog of temporary objects"),
				 errdetail("Dynamic shared memory, or slots for its segments, run short; the session keeps its temporary objects on disk.")));
		return false;
	}
	return true;
}

/*
 * Should a new object get an OID from the reserved range?  Called on the
 * coordinator by oid_dispatch.c with the same key it dispatches to the
 * segments.
 */
bool
tempcat_want_temp_oid(Oid catalog, const char *objname, Oid namespaceOid,
					  Oid keyOid1, Oid keyOid2)
{
	/*
	 * The temporary namespace decides, and the temporary toast namespace,
	 * created right after it, goes where it went: its relations belong to
	 * the other's.
	 */
	static bool namespace_in_memory = false;

	if (catalog == NamespaceRelationId && objname != NULL &&
		strncmp(objname, "pg_toast_temp_", 14) == 0)
		return namespace_in_memory;
	if (catalog == NamespaceRelationId)
	{
		namespace_in_memory =
			tempcat_key_is_temp(catalog, objname, namespaceOid,
								keyOid1, keyOid2,
								gp_enable_temp_memory_catalog) &&
			tempcat_can_start() &&
			tempcat_take_namespace_oid();

		/*
		 * A temporary namespace on disk, after one in memory rolled back:
		 * the slice taken for it goes (see tempcat_release_unused_slices()).
		 */
		if (!namespace_in_memory && objname != NULL &&
			strncmp(objname, "pg_temp_", 8) == 0)
			tempcat_drop_slice();
		return namespace_in_memory;
	}
	return tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1,
							   keyOid2, false);
}

/*
 * Only relations, types, functions and operators can be created in a
 * temporary namespace kept in memory: other objects (operator classes,
 * collations, text search objects, statistics objects, ...) would get
 * on-disk catalog rows pointing at the namespace that other sessions cannot
 * see.  Called by oid_dispatch.c on the coordinator.
 */
void
tempcat_check_namespace_object(Oid catalog, Oid namespaceOid)
{
	if (catalog == RelationRelationId || catalog == TypeRelationId ||
		catalog == ProcedureRelationId || catalog == OperatorRelationId ||
		!tempcat_owns_oid(NamespaceRelationId, namespaceOid))
		return;
	if (catalog == DefaultAclRelationId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("default privileges cannot be set for a temporary schema kept in memory"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("only relations, types, functions and operators can be created in a temporary schema kept in memory"),
			 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Allocate an OID for a new temporary object in 'relation' from this
 * session's slice of the reserved range, taking another slice when it is
 * used up.  Returns InvalidOid if no slice is left.
 *
 * Like GetNewOidWithIndex(), make sure the OID is not used in the catalog
 * yet: ordinary objects may have OIDs in the range (see the file header).
 * The SnapshotAny scan sees them, and the rows in memory, which have none of
 * the OIDs still to give (they are given once).
 */
static Oid
tempcat_try_allocate_oid(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
#ifdef FAULT_INJECTOR
	/* Test hook: ordinary objects take every OID of the slices. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_oid_exhausted") == FaultInjectorTypeSkip)
		return InvalidOid;
#endif

	for (;;)
	{
		Oid			oid;
		ScanKeyData key;
		SysScanDesc scan;
		bool		collides;

		if (tc_slice < 0 || tc_next_oid > tc_slice_end ||
			!OidIsValid(tc_next_oid))
		{
			if (!tempcat_claim_free_slice())
				return InvalidOid;
		}
#ifdef FAULT_INJECTOR
		/* Test hook: go near the end of the slice, to use it up soon. */
		if (SIMPLE_FAULT_INJECTOR("tempcat_slice_near_end") == FaultInjectorTypeSkip)
			tc_next_oid = Max(tc_next_oid, tc_slice_end - 1);
#endif
		oid = tc_next_oid++;
		TempcatShared->slices[tc_slice].nextOid = tc_next_oid;
		tc_slice_flags[tc_slice] |= TCS_USED;
		/* (statistics objects are made on the coordinator only) */
		if (RelationGetRelid(relation) != StatisticExtRelationId)
			tempcat_note_given();

		tempcat_activate(oid);

		ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(oid));
		scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
		collides = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);

		if (!collides)
		{
			/*
			 * Enum labels are no objects of their own: their rows go with
			 * their type (see tempcat_catalogs[]), and nothing looks for
			 * their OIDs among ours.
			 */
			if (RelationGetRelid(relation) != EnumRelationId)
				tempcat_remember_oid(RelationGetRelid(relation), oid);
			return oid;
		}
	}
}


Oid
tempcat_allocate_oid(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	Oid			oid;

	if (RelationGetRelid(relation) == NamespaceRelationId &&
		OidIsValid(tc_namespace_oid))
	{
		oid = tc_namespace_oid;
		tc_namespace_oid = InvalidOid;
		return oid;
	}

	oid = tempcat_try_allocate_oid(relation, indexId, oidcolumn);
	if (!OidIsValid(oid))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of OIDs for temporary objects kept in memory"),
				 errdetail("Other sessions hold all ranges of OIDs for temporary objects kept in memory."),
				 errhint("Retry once some of them have ended.")));
	return oid;
}

/*
 * Take the OID of the session's temporary namespace from its slice.  If
 * ordinary objects took every OID of the slices left (created before the
 * range was reserved, or carried over by pg_upgrade), the session keeps its
 * temporary objects on disk.
 */
static bool
tempcat_take_namespace_oid(void)
{
	Relation	rel;

	/* (taken already by an attempt that failed later) */
	if (OidIsValid(tc_namespace_oid))
		return true;
	rel = table_open(NamespaceRelationId, AccessShareLock);
	tc_namespace_oid = tempcat_try_allocate_oid(rel, NamespaceOidIndexId,
												Anum_pg_namespace_oid);
	table_close(rel, AccessShareLock);

	if (!OidIsValid(tc_namespace_oid))
	{
		/* (if nothing has OIDs of it) */
		if (tc_slice >= 0 && tc_slice_rows[tc_slice] == 0)
			tempcat_let_go_of_slice(tc_slice);
		ereport(WARNING,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of OIDs for temporary objects kept in memory"),
				 errdetail("Ordinary objects have all OIDs that the session could give its temporary objects; it keeps them on disk.")));
	}
	return OidIsValid(tc_namespace_oid);
}

/*
 * A segment received a reserved OID for a temporary object of ours.  The
 * coordinator checked that no object of its catalog has it, but an ordinary
 * object can exist on this node alone (created in utility mode, or left by
 * a broken catalog), and two rows with one OID would corrupt the catalog:
 * refuse.  Only live rows (and rows of transactions in progress) count.
 */
static void
tempcat_check_preassigned_oid(Relation relation, Oid indexId,
							  AttrNumber oidcolumn, Oid oid)
{
	ScanKeyData key;
	SysScanDesc scan;
	SnapshotData snapshot;
	bool		taken;

	InitDirtySnapshot(snapshot);
	ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(oid));
	scan = systable_beginscan(relation, indexId, true, &snapshot, 1, &key);
	taken = HeapTupleIsValid(systable_getnext(scan));
	systable_endscan(scan);

	if (taken)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("OID of a new temporary object is already used in catalog \"%s\" on this node",
						RelationGetRelationName(relation)),
				 errhint("OID %u belongs to an object that exists on this node only; gpcheckcat reports such objects.",
						 oid)));
}

/*
 * A segment received a pre-assigned OID from the coordinator.  If the
 * coordinator gave a temporary object of this session a reserved OID ('temp'
 * says that the object is one even if the key does not tell), take its
 * slice, check that the OID is free on this node, then remember that we own
 * it.
 */
void
tempcat_note_preassigned_oid(Relation relation, Oid indexId,
							 AttrNumber oidcolumn, const char *objname,
							 Oid namespaceOid, Oid keyOid1, Oid keyOid2,
							 bool temp, Oid oid)
{
	Oid			catalog = RelationGetRelid(relation);

	if (IsBinaryUpgrade || !IsTempcatOid(oid) || tempcat_is_reader())
		return;

	if (temp ||
		tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1, keyOid2,
							true))
	{
		if (tc_area == NULL && !tempcat_create_area(false))
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not create the in-memory catalog of temporary objects on this node"),
					 errdetail("Dynamic shared memory, or slots for its segments, ran out."),
					 errhint("Turn off gp_enable_temp_memory_catalog to keep the session's temporary objects on disk.")));
		tempcat_activate(oid);
		tempcat_check_preassigned_oid(relation, indexId, oidcolumn, oid);
		tempcat_remember_oid(catalog, oid);
	}
}

/*
 * A segment received the OID of a label of enum type 'typid', which comes
 * with a preassignment of its own (GetPreassignedOidForEnum()).  For a label
 * of a temporary enum of ours, do what tempcat_note_preassigned_oid() does:
 * take the slice of the OID, and check that the OID is free on this node.
 * (Labels are no objects of their own: their OIDs are not remembered.)
 */
void
tempcat_note_enum_label_oid(Relation enumrel, Oid typid, Oid oid)
{
	if (IsBinaryUpgrade || !IsTempcatOid(oid) || tempcat_is_reader() ||
		!tempcat_owns_oid(TypeRelationId, typid))
		return;
	tempcat_activate(oid);
	tempcat_check_preassigned_oid(enumrel, EnumOidIndexId, Anum_pg_enum_oid, oid);
}

/* ----------------------------------------------------------------
 * Object restrictions
 * ----------------------------------------------------------------
 */

/*
 * A temporary table kept in memory cannot inherit from an ordinary table.
 * Other sessions could not see it among the ordinary table's children, and
 * would treat the ordinary table as if it had none: ANALYZE would clear its
 * relhassubclass, hiding the child's rows from the child's own session, and
 * ALTER TABLE would leave the child behind instead of failing.  Called for
 * each new pg_inherits row.
 */
void
tempcat_check_inheritance(Oid relid, Oid parentid)
{
	if (tempcat_owns_oid(RelationRelationId, relid) &&
		!tempcat_owns_oid(RelationRelationId, parentid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary tables kept in memory cannot inherit from ordinary table \"%s\"",
						get_rel_name(parentid)),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Nor can it be a typed table of an ordinary composite type: other sessions
 * could not see it among the type's typed tables, and ALTER TYPE would
 * change the type without it instead of failing.
 */
void
tempcat_check_typed_table(Oid relid, Oid typid)
{
	if (tempcat_owns_oid(RelationRelationId, relid) &&
		!tempcat_owns_oid(TypeRelationId, typid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary tables kept in memory cannot be typed tables of ordinary type %s",
						format_type_be(typid)),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * An operator in a temporary schema kept in memory and an ordinary operator
 * cannot be commutators or negators of each other: each one's pg_operator
 * row points at the other (oprcom, oprnegate) without a pg_depend row.  The
 * ordinary one's row would point at an operator that other sessions cannot
 * see, and after a crash nobody would reset it.  Called for the commutator
 * and the negator (perhaps shells just made) of a new operator in
 * 'namespaceOid'.
 */
void
tempcat_check_operator_link(Oid namespaceOid, Oid otherOprOid)
{
	if (OidIsValid(otherOprOid) &&
		tempcat_owns_oid(NamespaceRelationId, namespaceOid) !=
		tempcat_owns_oid(OperatorRelationId, otherOprOid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary operators kept in memory and ordinary operators cannot be commutators or negators of each other"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * An ordinary object cannot depend on a temporary object kept in memory
 * (e.g. a column of an ordinary table of a temporary table's row type):
 * its on-disk rows would point at an object that other sessions cannot see,
 * and that is gone after a crash.  Called for each new pg_depend row.
 */
void
tempcat_check_dependency(const ObjectAddress *depender,
						 const ObjectAddress *referenced)
{
	if (tempcat_owns_oid(referenced->classId, referenced->objectId) &&
		!tempcat_owns_oid(depender->classId, depender->objectId))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ordinary objects cannot depend on temporary objects kept in memory"),
				 errdetail("The new object would depend on %s.",
						   getObjectDescription(referenced)),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Some objects can be made for temporary types kept in memory, but would
 * not be kept in memory themselves (casts, transforms): refuse.
 */
void
tempcat_check_unsupported(Oid catalog, Oid objectId, const char *what)
{
	if (tempcat_owns_oid(catalog, objectId))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s are not supported for temporary objects kept in memory", what),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/* ----------------------------------------------------------------
 * Memory for rows
 *
 * Rows, with their hash nodes, and the bucket arrays of the hash tables
 * (but the smallest ones, which are in the root, see tempcat_hash_shrink())
 * do not come from the size classes of the area: DSA keeps objects of each
 * size class in superblocks of 64 kB, so rows of the objects that stay
 * would keep the superblocks of those dropped from going back, and the
 * room these left would serve rows of the same size only.  They are cut
 * instead out of blocks of 64 kB that the area hands out as whole pages
 * (more than its largest size class), first fit, with the size of each
 * chunk and of the one before it in its header, so that free neighbours
 * merge; a block that empties goes back to the area, whose pages serve any
 * size.  Bigger ones come from the area directly.  The writer allocates
 * and frees under the exclusive lock.
 * ----------------------------------------------------------------
 */
#define TEMPCAT_BLOCK_SIZE	(64 * 1024)

#define TCC_INUSE	0x1
#define TCC_BIG		0x2			/* from the area itself, not a block */
#define TCC_FLAGS	0x7

typedef struct TempcatChunk
{
	uint32		size;			/* with this header, | TCC_*; the end of a
								 * block has one of size 0, in use */
	uint32		prev_size;		/* of the chunk before it, 0 if first */
} TempcatChunk;

typedef struct TempcatFreeChunk
{
	TempcatChunk hdr;
	dsa_pointer next;			/* in its bin */
	dsa_pointer prev;
} TempcatFreeChunk;

#define TCC_HDR		MAXALIGN(sizeof(TempcatChunk))
#define TCC_MIN		MAXALIGN(sizeof(TempcatFreeChunk))
#define TCC_SIZE(c)	((c)->size & ~TCC_FLAGS)

/* How many chunks of its own bin a search looks at before going up */
#define TEMPCAT_BIN_SEARCH	32

StaticAssertDecl(TCC_HDR == sizeof(TempcatChunk), "chunk header padded");

/* bins of sizes 24-31, 32-63, 64-127, ..., 32 kB and more */
static inline int
tempcat_bin(uint32 size)
{
	return Min(TEMPCAT_NBINS - 1, pg_leftmost_one_pos32(size) - 4);
}

static void
tempcat_bin_push(dsa_pointer cp)
{
	TempcatFreeChunk *c = TC_ADDR(cp);
	dsa_pointer *head = &tc_root->bins[tempcat_bin(TCC_SIZE(&c->hdr))];

	c->prev = InvalidDsaPointer;
	c->next = *head;
	if (DsaPointerIsValid(*head))
		((TempcatFreeChunk *) TC_ADDR(*head))->prev = cp;
	*head = cp;
}

static void
tempcat_bin_unlink(dsa_pointer cp)
{
	TempcatFreeChunk *c = TC_ADDR(cp);

	if (DsaPointerIsValid(c->prev))
		((TempcatFreeChunk *) TC_ADDR(c->prev))->next = c->next;
	else
		tc_root->bins[tempcat_bin(TCC_SIZE(&c->hdr))] = c->next;
	if (DsaPointerIsValid(c->next))
		((TempcatFreeChunk *) TC_ADDR(c->next))->prev = c->prev;
}

/*
 * Allocate 'size' bytes for rows (see above).  Unless 'grow', only from free
 * room the session has already: of the blocks taken, or, for a size bigger
 * than a block, of the segments of the area.  Returns InvalidDsaPointer if
 * the area is out of memory.
 */
static dsa_pointer
tempcat_alloc_ext(Size size, bool grow)
{
	Size		need = MAXALIGN(size) + TCC_HDR;
	dsa_pointer cp = InvalidDsaPointer;
	dsa_pointer rest = InvalidDsaPointer;
	TempcatChunk *c;
	uint32		csize;
	int			bin;

	if (need < TCC_MIN)
		need = TCC_MIN;

	/* (a block keeps room for the header that ends it) */
	if (need > TEMPCAT_BLOCK_SIZE - TCC_HDR)
	{
		cp = dsa_allocate_extended(tc_area, need, DSA_ALLOC_NO_OOM |
								   (grow ? 0 : DSA_ALLOC_NO_GROW));
		if (!DsaPointerIsValid(cp))
			return InvalidDsaPointer;
		c = TC_ADDR(cp);
		c->size = need | TCC_BIG | TCC_INUSE;
		c->prev_size = 0;
		tc_root->taken += need;
		return cp + TCC_HDR;
	}

	/* first fit: in the bin of the size, then any chunk of a bigger one */
	for (bin = tempcat_bin(need);
		 bin < TEMPCAT_NBINS && !DsaPointerIsValid(cp); bin++)
	{
		dsa_pointer fp = tc_root->bins[bin];
		int			n = 0;

		while (DsaPointerIsValid(fp) && n++ < TEMPCAT_BIN_SEARCH)
		{
			TempcatFreeChunk *f = TC_ADDR(fp);

			if (TCC_SIZE(&f->hdr) >= need)
			{
				cp = fp;
				break;
			}
			fp = f->next;
		}
		if (bin == tempcat_bin(need))
			rest = fp;
	}

	/* (and the rest of the bin of the size, before taking another block) */
	if (!DsaPointerIsValid(cp))
	{
		dsa_pointer fp = rest;

		while (DsaPointerIsValid(fp))
		{
			TempcatFreeChunk *f = TC_ADDR(fp);

			if (TCC_SIZE(&f->hdr) >= need)
			{
				cp = fp;
				break;
			}
			fp = f->next;
		}
	}

	if (!DsaPointerIsValid(cp))
	{
		TempcatChunk *end;

		if (!grow)
			return InvalidDsaPointer;
		cp = dsa_allocate_extended(tc_area, TEMPCAT_BLOCK_SIZE,
								   DSA_ALLOC_NO_OOM);
		if (!DsaPointerIsValid(cp))
			return InvalidDsaPointer;
		c = TC_ADDR(cp);
		c->size = TEMPCAT_BLOCK_SIZE - TCC_HDR;
		c->prev_size = 0;
		end = TC_ADDR(cp + TEMPCAT_BLOCK_SIZE - TCC_HDR);
		end->size = TCC_INUSE;
		end->prev_size = TEMPCAT_BLOCK_SIZE - TCC_HDR;
		tempcat_bin_push(cp);
		tc_root->taken += TEMPCAT_BLOCK_SIZE;
	}

	tempcat_bin_unlink(cp);
	c = TC_ADDR(cp);
	csize = TCC_SIZE(c);
	if (csize - need >= TCC_MIN)
	{
		dsa_pointer rp = cp + need;
		TempcatChunk *r = TC_ADDR(rp);

		r->size = csize - need;
		r->prev_size = need;
		((TempcatChunk *) TC_ADDR(rp + r->size))->prev_size = r->size;
		tempcat_bin_push(rp);
		csize = need;
	}
	c->size = csize | TCC_INUSE;
	return cp + TCC_HDR;
}

static inline dsa_pointer
tempcat_alloc(Size size)
{
	return tempcat_alloc_ext(size, true);
}

/* Free what tempcat_alloc() returned. */
static void
tempcat_free(dsa_pointer dp)
{
	dsa_pointer cp = dp - TCC_HDR;
	TempcatChunk *c = TC_ADDR(cp);
	TempcatChunk *next;
	uint32		size;

	Assert(c->size & TCC_INUSE);
	if (c->size & TCC_BIG)
	{
		tc_root->taken -= TCC_SIZE(c);
		dsa_free(tc_area, cp);
		tc_trim_wanted = true;
		return;
	}

	size = TCC_SIZE(c);
	next = TC_ADDR(cp + size);
	if (!(next->size & TCC_INUSE))
	{
		tempcat_bin_unlink(cp + size);
		size += TCC_SIZE(next);
	}
	if (c->prev_size != 0)
	{
		dsa_pointer pp = cp - c->prev_size;
		TempcatChunk *p = TC_ADDR(pp);

		if (!(p->size & TCC_INUSE))
		{
			tempcat_bin_unlink(pp);
			size += TCC_SIZE(p);
			cp = pp;
			c = p;
		}
	}
	c->size = size;
	next = TC_ADDR(cp + size);
	next->prev_size = size;

	/* the whole block: give it back */
	if (c->prev_size == 0 && TCC_SIZE(next) == 0)
	{
		tc_root->taken -= TEMPCAT_BLOCK_SIZE;
		dsa_free(tc_area, cp);
		tc_trim_wanted = true;
		return;
	}
	tempcat_bin_push(cp);
}

/* ----------------------------------------------------------------
 * Hash tables in the area
 * ----------------------------------------------------------------
 */

/*
 * Double the number of buckets.  Failing to allocate the new array is not an
 * error: chains just get longer.
 */
static void
tempcat_hash_grow(TempcatHash *h)
{
	uint32		newn = h->nbuckets * 2;
	dsa_pointer newb;
	dsa_pointer *oldarr;
	dsa_pointer *newarr;
	uint32		i;

	newb = tempcat_alloc(sizeof(dsa_pointer) * newn);
	if (!DsaPointerIsValid(newb))
		return;

	oldarr = TC_ADDR(h->buckets);
	newarr = TC_ADDR(newb);
	memset(newarr, 0, sizeof(dsa_pointer) * newn);
	for (i = 0; i < h->nbuckets; i++)
	{
		dsa_pointer np = oldarr[i];

		while (DsaPointerIsValid(np))
		{
			TempcatHashNode *node = TC_ADDR(np);
			dsa_pointer next = node->next;
			uint32		idx = node->hash & (newn - 1);

			node->next = newarr[idx];
			newarr[idx] = np;
			np = next;
		}
	}

	if (h->buckets != h->home)
		tempcat_free(h->buckets);
	h->buckets = newb;
	h->nbuckets = newn;
	h->shrink_below = newn / 4;
}

/*
 * Shrink the buckets of a table that has fewer than a quarter as many
 * entries, so that the buckets of a table that was big once do not keep the
 * memory (and, scattered in the blocks, keep blocks from going back to the
 * area): to one or two buckets per entry, its home buckets in the root at
 * the least.  Only from free room the session has already, as it is
 * to give memory back; failing that, to the home buckets if there are few
 * enough entries for them, or else try again once the entries have halved.
 */
static void
tempcat_hash_shrink(TempcatHash *h)
{
	uint32		newn = h->nbuckets / 2;
	dsa_pointer newb;
	dsa_pointer *oldarr;
	dsa_pointer *newarr;
	uint32		i;

	Assert(newn >= TEMPCAT_HASH_MIN && h->buckets != h->home);
	while (newn > TEMPCAT_HASH_MIN && h->nentries < newn / 2)
		newn /= 2;
	if (newn == TEMPCAT_HASH_MIN)
		newb = h->home;
	else
	{
		newb = tempcat_alloc_ext(sizeof(dsa_pointer) * newn, false);
		if (!DsaPointerIsValid(newb))
		{
			if (h->nentries >= TEMPCAT_HASH_MIN * 2)
			{
				h->shrink_below = Max(h->nentries / 2, TEMPCAT_HASH_MIN * 2);
				return;
			}
			/* (not a load to grow at, see tempcat_hash_prepare()) */
			newn = TEMPCAT_HASH_MIN;
			newb = h->home;
		}
	}

	oldarr = TC_ADDR(h->buckets);
	newarr = TC_ADDR(newb);
	memset(newarr, 0, sizeof(dsa_pointer) * newn);
	for (i = 0; i < h->nbuckets; i++)
	{
		dsa_pointer np = oldarr[i];

		while (DsaPointerIsValid(np))
		{
			TempcatHashNode *node = TC_ADDR(np);
			dsa_pointer next = node->next;
			uint32		idx = node->hash & (newn - 1);

			node->next = newarr[idx];
			newarr[idx] = np;
			np = next;
		}
	}

	tempcat_free(h->buckets);
	h->buckets = newb;
	h->nbuckets = newn;
	h->shrink_below = newn / 4;
}

/*
 * Make sure the table has buckets, and more of them if it has twice as many
 * entries (failing to allocate them is not an error).
 */
static void
tempcat_hash_prepare(TempcatHash *h)
{
	if (h->nbuckets == 0)
	{
		Assert(DsaPointerIsValid(h->home));
		h->buckets = h->home;
		memset(TC_ADDR(h->buckets), 0, sizeof(dsa_pointer) * TEMPCAT_HASH_MIN);
		h->nbuckets = TEMPCAT_HASH_MIN;
	}
	else if (h->nentries >= h->nbuckets * 2)
		tempcat_hash_grow(h);
}

/*
 * Link a node, which is part of the entry's allocation (see
 * tempcat_entry_create()).  The table must have been prepared.
 */
static void
tempcat_hash_link(TempcatHash *h, dsa_pointer np, uint32 hash, dsa_pointer entry)
{
	TempcatHashNode *node = TC_ADDR(np);
	dsa_pointer *arr = TC_ADDR(h->buckets);
	uint32		idx = hash & (h->nbuckets - 1);

	node->hash = hash;
	node->entry = entry;
	node->next = arr[idx];
	arr[idx] = np;
	h->nentries++;
}

static void
tempcat_hash_remove(TempcatHash *h, uint32 hash, dsa_pointer entry)
{
	dsa_pointer *link;

	Assert(h->nbuckets > 0);
	link = &((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
	while (DsaPointerIsValid(*link))
	{
		TempcatHashNode *node = TC_ADDR(*link);

		if (node->entry == entry)
		{
			*link = node->next;
			h->nentries--;
			if (h->nbuckets > TEMPCAT_HASH_MIN && h->nentries < h->shrink_below)
				tempcat_hash_shrink(h);
			return;
		}
		link = &node->next;
	}
	elog(ERROR, "tempcat: hash entry not found");
}

static inline uint32
tempcat_tid_hash(ItemPointer tid)
{
	return DatumGetUInt32(hash_any((const unsigned char *) tid,
								   sizeof(ItemPointerData)));
}

static uint32
tempcat_key_hash(TempcatKeyType type, Datum value)
{
	if (type == TCK_OID)
		return DatumGetUInt32(hash_uint32(DatumGetObjectId(value)));
	else
	{
		/* works for both a Name and a C string argument */
		const char *s = DatumGetCString(value);

		return DatumGetUInt32(hash_any((const unsigned char *) s,
									   strnlen(s, NAMEDATALEN)));
	}
}

/* Does the catalog still hold an entry (of any visibility) for 'oid'? */
static bool
tempcat_memory_has_oid(int catidx, Oid oid)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	uint32		hash = DatumGetUInt32(hash_uint32(oid));
	int			k;

	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		TempcatHash *h = &tc_root->catalogs[catidx].keys[k];
		dsa_pointer np;

		/* the oid column is always attribute 1 */
		if (def->keyattr[k] != 1 || h->nbuckets == 0)
			continue;

		np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
		while (DsaPointerIsValid(np))
		{
			TempcatHashNode *node = TC_ADDR(np);

			if (node->hash == hash &&
				tempcat_entry_oid(TC_ADDR(node->entry)) == oid)
				return true;
			np = node->next;
		}
	}
	return false;
}

/* ----------------------------------------------------------------
 * Entries
 * ----------------------------------------------------------------
 */

static dsa_pointer
tempcat_find_tid(ItemPointer tid)
{
	uint32		hash = tempcat_tid_hash(tid);
	TempcatHash *h = &tc_root->tids;
	dsa_pointer np;

	if (h->nbuckets == 0)
		return InvalidDsaPointer;

	np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
	while (DsaPointerIsValid(np))
	{
		TempcatHashNode *node = TC_ADDR(np);

		if (node->hash == hash &&
			ItemPointerEquals(&((TempcatEntry *) TC_ADDR(node->entry))->tid, tid))
			return node->entry;
		np = node->next;
	}
	return InvalidDsaPointer;
}

static ItemPointerData
tempcat_new_tid(void)
{
	ItemPointerData tid;
	uint64		n = tc_root->next_tid++;

	ItemPointerSetBlockNumber(&tid, (BlockNumber) (n / 0x4000));
	tid.ip_posid = TEMPCAT_TID_BIT | (OffsetNumber) ((n % 0x4000) + 1);
	return tid;
}

/*
 * Create an entry for a tuple and link it into the catalog's list and hash
 * tables.  Returns InvalidDsaPointer if the area is out of memory, in which
 * case nothing has changed.  Caller holds the exclusive lock.
 */
static dsa_pointer
tempcat_entry_create(int catidx, HeapTuple tup, TupleDesc desc,
					 TransactionId xmin, CommandId cmin)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	TempcatCatalog *cat = &tc_root->catalogs[catidx];
	dsa_pointer ep;
	dsa_pointer np;
	TempcatEntry *e;
	HeapTupleHeader hdr;
	int			nnodes = 1;		/* the TID's */
	int			k;

	/*
	 * Allocate everything up front, so that running out changes nothing (but
	 * the bucket arrays that grew): the entry, the tuple, and the nodes of
	 * the hash tables, which come and go with the entry.
	 */
	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		if (def->keytype[k] == TCK_NONE)
			continue;
		tempcat_hash_prepare(&cat->keys[k]);
		nnodes++;
	}
	tempcat_hash_prepare(&tc_root->tids);
	ep = tempcat_alloc(MAXALIGN(sizeof(TempcatEntry)) + MAXALIGN(tup->t_len) +
					   nnodes * sizeof(TempcatHashNode));
#ifdef FAULT_INJECTOR
	/* Test hook: the area runs full for a row. */
	if (DsaPointerIsValid(ep) &&
		SIMPLE_FAULT_INJECTOR("tempcat_row_alloc") == FaultInjectorTypeSkip)
	{
		tempcat_free(ep);
		ep = InvalidDsaPointer;
	}
#endif
	if (!DsaPointerIsValid(ep))
		return InvalidDsaPointer;

	e = TC_ADDR(ep);
	memset(e, 0, sizeof(TempcatEntry));
	e->xmin = xmin;
	e->xmax = InvalidTransactionId;
	e->cmin = cmin;
	e->cmax = InvalidCommandId;
	e->catidx = catidx;
	e->len = tup->t_len;
	e->tid = tempcat_new_tid();
	e->tidhash = tempcat_tid_hash(&e->tid);

	hdr = TCE_DATA(e);
	memcpy(hdr, tup->t_data, tup->t_len);
	hdr->t_infomask &= ~HEAP_XACT_MASK;
	hdr->t_ctid = e->tid;

	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		bool		isnull;
		Datum		value;

		if (def->keytype[k] == TCK_NONE)
			continue;
		value = heap_getattr(tup, def->keyattr[k], desc, &isnull);
		e->keyhash[k] = isnull ? 0 : tempcat_key_hash(def->keytype[k], value);
	}

	/* link into the catalog's list */
	e->prev = InvalidDsaPointer;
	e->next = cat->head;
	if (DsaPointerIsValid(cat->head))
		((TempcatEntry *) TC_ADDR(cat->head))->prev = ep;
	cat->head = ep;
	cat->nentries++;

	np = ep + MAXALIGN(sizeof(TempcatEntry)) + MAXALIGN(tup->t_len);
	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		if (def->keytype[k] == TCK_NONE)
			continue;
		tempcat_hash_link(&cat->keys[k], np, e->keyhash[k], ep);
		np += sizeof(TempcatHashNode);
	}
	tempcat_hash_link(&tc_root->tids, np, e->tidhash, ep);

	tempcat_count_slice_row(e, 1);

	return ep;
}

/* Caller holds the exclusive lock. */
static void
tempcat_entry_remove(dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);
	const TempcatCatalogDef *def = &tempcat_catalogs[e->catidx];
	TempcatCatalog *cat = &tc_root->catalogs[e->catidx];
	int			k;

	/* (see tempcat_recompute_used()) */
	if (def->relid == DependRelationId || def->relid == SharedDependRelationId)
		tc_used_dirty = true;

	tempcat_count_slice_row(e, -1);

	if (DsaPointerIsValid(e->prev))
		((TempcatEntry *) TC_ADDR(e->prev))->next = e->next;
	else
		cat->head = e->next;
	if (DsaPointerIsValid(e->next))
		((TempcatEntry *) TC_ADDR(e->next))->prev = e->prev;
	cat->nentries--;

	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		if (def->keytype[k] != TCK_NONE)
			tempcat_hash_remove(&cat->keys[k], e->keyhash[k], ep);
	}
	tempcat_hash_remove(&tc_root->tids, e->tidhash, ep);

	tempcat_free(ep);
}

/* Build a palloc'd HeapTuple from an entry, with its header filled in. */
static HeapTuple
tempcat_entry_copy(TempcatEntry *e, Oid relid)
{
	HeapTuple	tup = (HeapTuple) palloc(HEAPTUPLESIZE + e->len);
	HeapTupleHeader hdr;

	tup->t_len = e->len;
	tup->t_self = e->tid;
	tup->t_tableOid = relid;
	tup->t_data = hdr = (HeapTupleHeader) ((char *) tup + HEAPTUPLESIZE);
	memcpy(hdr, TCE_DATA(e), e->len);

	/*
	 * A copy of a row visible to the scan's snapshot: its xmax, if any,
	 * shows (e.g. in the xmax system column) but is marked invalid, as
	 * visibility checks of the copy must not take it away again.
	 */
	hdr->t_infomask &= ~HEAP_XACT_MASK;
	hdr->t_infomask |= HEAP_XMAX_INVALID;
	if (e->flags & TCE_XMIN_COMMITTED)
		hdr->t_infomask |= HEAP_XMIN_COMMITTED;
	HeapTupleHeaderSetXmin(hdr, e->xmin);
	HeapTupleHeaderSetXmax(hdr, e->xmax);
	HeapTupleHeaderSetCmin(hdr, e->cmin);
	hdr->t_ctid = e->tid;

	return tup;
}

/* Mirror what heap_insert() leaves in the caller's tuple. */
static void
tempcat_set_caller_tuple(HeapTuple tup, Relation rel, TempcatEntry *e)
{
	tup->t_self = e->tid;
	tup->t_tableOid = RelationGetRelid(rel);
	tup->t_data->t_infomask &= ~HEAP_XACT_MASK;
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, e->xmin);
	HeapTupleHeaderSetXmax(tup->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tup->t_data, e->cmin);
	tup->t_data->t_ctid = e->tid;
}

/* ----------------------------------------------------------------
 * Pending entries and visibility
 * ----------------------------------------------------------------
 */

/*
 * Make room for 'n' more pending entries.  Called before creating or
 * changing entries: running out of memory after that would leave an entry
 * that no transaction end looks at (an aborted insertion never freed).
 */
static void
tempcat_reserve_pending(int n)
{
	/* (see tempcat_abort_subtransaction()) */
	if (tc_resolve_wanted)
		tempcat_resolve_pending();

	if (tc_npending + n > tc_maxpending)
	{
		int			newmax = Max(64, Max(tc_maxpending * 2, tc_npending + n));

		if (tc_pending == NULL)
			tc_pending = MemoryContextAlloc(TopMemoryContext,
											sizeof(dsa_pointer) * newmax);
		else
			tc_pending = repalloc(tc_pending, sizeof(dsa_pointer) * newmax);
		tc_maxpending = newmax;
	}
}

/* Caller made room with tempcat_reserve_pending(). */
static void
tempcat_add_pending(dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);

	tc_xact_changed = true;
	tc_change_level = Max(tc_change_level, GetCurrentTransactionNestLevel());

	/*
	 * Until the outcome of its transactions is applied to the row, when the
	 * session goes idle or starts its next transaction, its xids are looked
	 * up in the clog: keep vac_update_datfrozenxid(), and so clog
	 * truncation, from passing them.
	 */
	if (!(e->flags & TCE_XMIN_COMMITTED))
		tempcat_cover_xid(e->xmin);
	tempcat_cover_xid(e->xmax);

	if (e->flags & TCE_PENDING)
		return;

	Assert(tc_npending < tc_maxpending);
	tc_pending[tc_npending++] = ep;
	e->flags |= TCE_PENDING;
}

/*
 * Take entry 'i' off the pending list (the last one takes its place).  Do
 * it before anything that can fail, e.g. freeing the row: an error must not
 * leave the list holding a row freed already, which the next pass would
 * free again.
 */
static void
tempcat_pending_take(int i)
{
	Assert(i >= 0 && i < tc_npending);
	tc_pending[i] = tc_pending[--tc_npending];
}

static bool
tempcat_entry_visible(TempcatEntry *e, Snapshot snapshot)
{
	bool		use_cid;

	switch (snapshot->snapshot_type)
	{
		case SNAPSHOT_ANY:
			return true;
		case SNAPSHOT_DIRTY:
			snapshot->xmin = snapshot->xmax = InvalidTransactionId;
			snapshot->speculativeToken = 0;
			use_cid = false;
			break;
		case SNAPSHOT_MVCC:
		case SNAPSHOT_HISTORIC_MVCC:
			use_cid = true;
			break;
		default:
			use_cid = false;
			break;
	}

	/*
	 * Only this session changes the rows, one transaction after another, so
	 * a transaction that is not ours is visible once committed.
	 */
	if (!(e->flags & TCE_XMIN_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(e->xmin))
		{
			if (use_cid && e->cmin >= snapshot->curcid)
				return false;	/* inserted after scan started */
		}
		else if (!TransactionIdDidCommit(e->xmin))
			return false;		/* aborted, or prepared and not finished */
	}

	if (!TransactionIdIsValid(e->xmax))
		return true;

	if (TransactionIdIsCurrentTransactionId(e->xmax))
		return use_cid && e->cmax >= snapshot->curcid;

	return !TransactionIdDidCommit(e->xmax);
}

static TempcatXidStatus
tempcat_xid_status(TransactionId xid)
{
	if (TransactionIdIsCurrentTransactionId(xid) ||
		TransactionIdIsInProgress(xid))
		return TCX_IN_PROGRESS;
	if (TransactionIdDidCommit(xid))
		return TCX_COMMITTED;
	return TCX_ABORTED;
}

/*
 * After removing an entry, stop treating its object's OID as ours if no
 * other version of the object is left in memory.  Keeps the owned set from
 * growing in long sessions.
 */
static void
tempcat_forget_if_gone(int catidx, Oid objoid)
{
#ifdef FAULT_INJECTOR
	/* Test hook: fail while the outcomes are applied (with 'error'). */
	SIMPLE_FAULT_INJECTOR("tempcat_forget_if_gone");
#endif

	if (tempcat_catalogs[catidx].relid == RelationRelationId)
		tc_horizon_dirty = true;

	if (OidIsValid(objoid) && !tempcat_memory_has_oid(catidx, objoid) &&
		tempcat_forget_oid(tempcat_catalogs[catidx].relid, objoid))
	{
		/*
		 * pgstat_vacuum_stat() keeps the statistics of OIDs of live
		 * sessions' temporary objects (tempcat_oid_maybe_live()): purge those
		 * of our dropped tables and functions ourselves.
		 */
		if (tempcat_catalogs[catidx].relid == RelationRelationId)
			pgstat_purge_relation(objoid);
		else if (tempcat_catalogs[catidx].relid == ProcedureRelationId)
			pgstat_purge_function(objoid);
	}
}

/*
 * Apply the outcome of finished transactions to the pending entries: drop
 * aborted inserts and committed deletes, clear aborted deletes.  Entries of
 * a prepared transaction that has not been finished yet stay pending.
 *
 * Called by the writer at the start of each transaction, and when it goes
 * idle (tempcat_idle()), when none of the pending XIDs can be the current
 * one anymore, and after a subtransaction abort.
 */
static void
tempcat_resolve_pending(void)
{
	int			i = 0;
	int			npending = tc_npending;

	tc_resolve_wanted = false;
	if (tc_npending == 0)
		return;

	Assert(tc_area != NULL);

#ifdef FAULT_INJECTOR
	/* Test hook: count the walks; see tempcat_abort_subtransaction(). */
	SIMPLE_FAULT_INJECTOR("tempcat_resolve_pending");
#endif

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);

	while (i < tc_npending)
	{
		dsa_pointer ep = tc_pending[i];
		TempcatEntry *e = TC_ADDR(ep);
		int			catidx = e->catidx;
		Oid			objoid;
		bool		in_progress = false;

		/* (a row freed already would fail this; see tempcat_pending_take()) */
		Assert(catidx >= 0 && catidx < TEMPCAT_NCATALOGS &&
			   (e->flags & TCE_PENDING));
		objoid = tempcat_catalog_has_oid(catidx) ? tempcat_entry_oid(e) : InvalidOid;

		if (!(e->flags & TCE_XMIN_COMMITTED))
		{
			switch (tempcat_xid_status(e->xmin))
			{
				case TCX_ABORTED:
					tempcat_pending_take(i);
					tempcat_entry_remove(ep);
					tempcat_forget_if_gone(catidx, objoid);
					continue;
				case TCX_COMMITTED:
					e->flags |= TCE_XMIN_COMMITTED;
					break;
				case TCX_IN_PROGRESS:
					in_progress = true;
					break;
			}
		}

		if (TransactionIdIsValid(e->xmax))
		{
			switch (tempcat_xid_status(e->xmax))
			{
				case TCX_COMMITTED:
					tempcat_pending_take(i);
					tempcat_entry_remove(ep);
					tempcat_forget_if_gone(catidx, objoid);
					continue;
				case TCX_ABORTED:
					e->xmax = InvalidTransactionId;
					e->cmax = InvalidCommandId;
					break;
				case TCX_IN_PROGRESS:
					in_progress = true;
					break;
			}
		}

		if (in_progress)
			i++;
		else
		{
			tempcat_pending_take(i);
			e->flags &= ~TCE_PENDING;
		}
	}

	LWLockRelease(&tc_root->lock);

	/* Their xids need no cover any more; see tempcat_add_pending(). */
	if (tc_npending < npending)
		tc_horizon_dirty = true;
}

/*
 * Recompute the published horizon from all our in-memory pg_class rows, of
 * any visibility, after tables were dropped or vacuumed.
 */
static void
tempcat_recompute_horizon(void)
{
	int			catidx = tempcat_catalog_index(RelationRelationId);
	TempcatProcState *st = tempcat_my_state();
	TransactionId minxid = InvalidTransactionId;
	MultiXactId minmxid = InvalidMultiXactId;
	dsa_pointer ep;
	int			i;

	tc_horizon_dirty = false;

	/* the xids of rows still pending; see tempcat_add_pending() */
	for (i = 0; i < tc_npending; i++)
	{
		TempcatEntry *e = TC_ADDR(tc_pending[i]);

		if (!(e->flags & TCE_XMIN_COMMITTED) && TransactionIdIsNormal(e->xmin) &&
			(!TransactionIdIsValid(minxid) || TransactionIdPrecedes(e->xmin, minxid)))
			minxid = e->xmin;
		if (TransactionIdIsNormal(e->xmax) &&
			(!TransactionIdIsValid(minxid) || TransactionIdPrecedes(e->xmax, minxid)))
			minxid = e->xmax;
	}

	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);
		Form_pg_class form = (Form_pg_class) ((char *) hdr + hdr->t_hoff);

		if (TransactionIdIsNormal(form->relfrozenxid) &&
			(!TransactionIdIsValid(minxid) ||
			 TransactionIdPrecedes(form->relfrozenxid, minxid)))
			minxid = form->relfrozenxid;
		if (MultiXactIdIsValid(form->relminmxid) &&
			(!MultiXactIdIsValid(minmxid) ||
			 MultiXactIdPrecedes(form->relminmxid, minmxid)))
			minmxid = form->relminmxid;
		ep = e->next;
	}

	pg_atomic_write_u32(&st->frozenXid, minxid);
	pg_atomic_write_u32(&st->minMulti, minmxid);
}

/*
 * Command IDs of the snapshots a writer QE handed to cursors in the current
 * transaction (dumpSharedLocalSnapshot_forCursor()).  The cursors' reader
 * gangs scan with them in other processes, where SnapshotsSeeCommandRange()
 * does not see them; they are kept for the rest of the transaction, as the
 * writer does not learn when the cursors close.
 */
static List *tc_cursor_cids = NIL;
static LocalTransactionId tc_cursor_lxid = InvalidLocalTransactionId;

void
tempcat_note_cursor_snapshot(CommandId cid)
{
	MemoryContext oldcxt;

	if (Gp_role != GP_ROLE_EXECUTE || !IsTransactionState())
		return;
	if (tc_cursor_lxid != MyProc->lxid)
	{
		/* (the list of an earlier transaction went with its memory) */
		tc_cursor_cids = NIL;
		tc_cursor_lxid = MyProc->lxid;
	}
	if (list_member_int(tc_cursor_cids, (int) cid))
		return;
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	tc_cursor_cids = lappend_int(tc_cursor_cids, (int) cid);
	MemoryContextSwitchTo(oldcxt);
}

/* Can a cursor's snapshot see a row of this command range? */
static bool
tempcat_cursors_see_command_range(CommandId cmin, CommandId cmax)
{
	ListCell   *lc;

	if (tc_cursor_lxid != MyProc->lxid)
		return false;
	foreach(lc, tc_cursor_cids)
	{
		CommandId	cid = (CommandId) lfirst_int(lc);

		if (cmin < cid && cid <= cmax)
			return true;
	}
	return false;
}

/*
 * Free rows that the current transaction inserted and then deleted in an
 * earlier command, before the transaction ends.  Called when the area is
 * full, so that a transaction creating and dropping many temporary tables
 * in a loop does not run out of room.  Caller holds the exclusive lock.
 * Returns the number of rows freed.
 *
 * Such a row is dead whatever the transaction's fate, as long as its
 * deletion cannot be undone on its own: rows deleted by the top-level
 * transaction itself, or by the same (sub)transaction that inserted them
 * (any rollback undoing the deletion then undoes the insertion too).  And
 * only rows that no snapshot of ours still in use can see
 * (SnapshotsSeeCommandRange()): an older snapshot of the transaction, e.g.
 * that of the outer query or of a cursor, still sees rows deleted after it
 * was taken.  Scans through an index copy the rows they return when they
 * start; other scans keep only their TIDs then, and copy each row when they
 * return it, which they can as long as their snapshot keeps it from being
 * freed here.
 */
static int
tempcat_reclaim_dead(void)
{
	TransactionId topxid = GetTopTransactionIdIfAny();
	CommandId	curcid = GetCurrentCommandId(false);
	int			nfreed = 0;
	bool		waiting = false;
	int			i = 0;

	if (!TransactionIdIsValid(topxid))
		return 0;

	/*
	 * The walk is linear in the pending rows, and once the area is full it
	 * is tried for every row added.  Do not repeat one that freed nothing
	 * unless something could have changed: rows deleted since, or, if rows
	 * were left only because of the command or the snapshots in use, a new
	 * command.
	 */
	if (tc_reclaim_failed &&
		TransactionIdEquals(tc_reclaim_xid, topxid) &&
		tc_reclaim_ndeletions == tc_ndeletions &&
		(!tc_reclaim_waiting || tc_reclaim_cid == curcid))
		return 0;

	while (i < tc_npending)
	{
		dsa_pointer ep = tc_pending[i];
		TempcatEntry *e = TC_ADDR(ep);
		bool		candidate;

		Assert(e->catidx >= 0 && e->catidx < TEMPCAT_NCATALOGS &&
			   (e->flags & TCE_PENDING));

		candidate = (TransactionIdEquals(e->xmax, topxid) ||
					 TransactionIdEquals(e->xmax, e->xmin)) &&
			!(e->flags & TCE_XMIN_COMMITTED) &&
			TransactionIdIsCurrentTransactionId(e->xmin);
		if (candidate &&
			(e->cmax >= curcid || SnapshotsSeeCommandRange(e->cmin, e->cmax) ||
			 tempcat_cursors_see_command_range(e->cmin, e->cmax)))
		{
			waiting = true;
			candidate = false;
		}

		if (candidate)
		{
			int			catidx = e->catidx;
			Oid			objoid = tempcat_catalog_has_oid(catidx) ?
				tempcat_entry_oid(e) : InvalidOid;

			tempcat_pending_take(i);
			tempcat_entry_remove(ep);
			tempcat_forget_if_gone(catidx, objoid);
			nfreed++;
		}
		else
			i++;
	}

	tc_reclaim_failed = (nfreed == 0);
	tc_reclaim_xid = topxid;
	tc_reclaim_cid = curcid;
	tc_reclaim_ndeletions = tc_ndeletions;
	tc_reclaim_waiting = waiting;

	return nfreed;
}

/* ----------------------------------------------------------------
 * Transactions
 * ----------------------------------------------------------------
 */

void
tempcat_start_transaction(void)
{
	bool		forget = false;

	tc_xact_changed = false;
	tc_change_level = 0;

	/*
	 * A retrieve connection can outlive the writer of the cursor session it
	 * serves, which ends or is replaced (e.g. after its gang was reset).
	 * Detach from that writer's area then, and forget what we cached about
	 * its objects: the next writer gives their OIDs to its own.  The next
	 * lookup attaches to the session's current area, if any.
	 */
	if (am_cursor_retrieve_handler && tc_area != NULL)
	{
		dsa_handle	handle;
		dsa_pointer rootptr;
		int			sessionId = RetrieveSessionId();

		if (sessionId < 0 ||
			!SharedSnapshotGetTempcatArea(sessionId, &handle, &rootptr) ||
			handle != dsa_get_handle(tc_area))
		{
			dsa_detach(tc_area);
			tc_area = NULL;
			tc_root = NULL;
			forget = true;
		}
	}

	/* (see tempcat_forget_cached_oids()) */
	if (tempcat_is_reader() && tc_area != NULL &&
		pg_atomic_read_u32(&tc_root->cache_epoch) != tc_seen_cache_epoch)
	{
		tc_seen_cache_epoch = pg_atomic_read_u32(&tc_root->cache_epoch);
		forget = true;
	}
	if (forget)
	{
		InvalidateSystemCaches();
		/* (at the start of a transaction, nothing uses any of them) */
		TypeCacheForgetRange(FirstTempcatObjectId, LastTempcatObjectId, false);
	}

	if (tempcat_is_reader() || tc_area == NULL)
		return;

	tempcat_resolve_pending();
	if (tc_horizon_dirty)
		tempcat_recompute_horizon();
	if (tc_used_dirty)
		tempcat_recompute_used(false);
	if (tc_release_wanted)
		tempcat_release_unused_slices(true);
}

/*
 * The session goes idle, out of any transaction or in one that rolled back
 * already: apply the outcome of its finished transactions to the pending
 * rows now, rather than at the start of its next transaction, let the
 * published horizon rise (see tempcat_add_pending()), and let go of the
 * objects its rows no longer use (tempcat_recompute_used()) and of the
 * slices they no longer have OIDs of (tempcat_release_unused_slices()), and
 * give the segments of the area that emptied back to the OS (not as soon as
 * they empty, see dsa_trim()), once more than half of the area is free: a
 * session at the edge of a segment that creates and drops an object in
 * turn would create and free the segment every time.  The next transaction
 * may be long in coming, especially for a writer QE.
 *
 * A reader lets go of its mappings of the segments of the area: it may stay
 * idle long after the writer gave some back (e.g. in the gang of a cursor
 * that the transaction closed), which would stay in /dev/shm until it
 * detached from them.
 *
 * Out of a transaction, an error would not release the lock, and could
 * repeat at every return here: log it instead, and leave the rows to the
 * start of the next transaction, which reports any error as usual.
 */
void
tempcat_idle(void)
{
	MemoryContext oldcxt = CurrentMemoryContext;

	if (tc_area == NULL)
		return;
	if (tempcat_is_reader())
	{
		dsa_unmap_segments(tc_area);
		return;
	}
	if (tc_npending == 0 && !tc_horizon_dirty && !tc_used_dirty &&
		!tc_release_wanted && !tc_used_overflow && !tc_trim_wanted)
		return;

	PG_TRY();
	{
		tempcat_resolve_pending();
		if (tc_horizon_dirty)
			tempcat_recompute_horizon();
		if (tc_used_dirty)
			tempcat_recompute_used(false);
		tempcat_relock_used();
		if (tc_release_wanted)
			tempcat_release_unused_slices(false);
		if (tc_trim_wanted)
		{
			size_t		total = dsa_get_total_size(tc_area);

			tc_trim_wanted = false;
			if (total - Min(tc_root->taken, total) > total / 2)
				dsa_trim(tc_area);
		}
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		LWLockReleaseAll();
		pgstat_report_wait_end();
		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		ereport(LOG,
				(errmsg("could not apply the outcome of finished transactions to in-memory catalog rows: %s",
						edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/*
 * PREPARE TRANSACTION by a user (possible in utility mode only) of a
 * transaction that changed rows in memory.  Upstream refuses to prepare
 * transactions that operated on temporary objects; Greengage cannot, as its
 * own two-phase commit prepares every distributed transaction on the
 * segments.  For rows in memory it matters more: if the session ends before
 * the prepared transaction is finished, its changes to them are lost,
 * while its on-disk effects (relation files) are committed later anyway.
 */
void
tempcat_check_prepare(void)
{
	if (Gp_role == GP_ROLE_UTILITY && tc_xact_changed)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has operated on temporary objects kept in memory")));
}

/*
 * Did the current transaction change rows in memory?  A QE reports it to the
 * dispatcher like a WAL write for a permanent relation (see dest.c), so that
 * the transaction is committed in two phases: in one phase, an error at
 * commit on one segment (e.g. a deferred unique check) would leave the
 * changes committed on the others while the coordinator rolls back.
 */
bool
tempcat_xact_changed(void)
{
	return tc_xact_changed;
}

/*
 * After a subtransaction abort, free the rows it inserted and undo its
 * deletions soon, so that loops with exception blocks do not keep dead rows
 * around until the end of the top-level transaction.  Only if it, or a
 * subtransaction committed into it, changed rows.  Not here, in the middle
 * of the abort, where an error could not be handled: before the next change
 * of rows in memory (tempcat_reserve_pending()), or at the start or end of
 * the next transaction, which see the same outcomes.
 */
void
tempcat_abort_subtransaction(void)
{
	int			level = GetCurrentTransactionNestLevel();

	if (tc_change_level < level || tc_area == NULL || tempcat_is_reader())
		return;
	tc_resolve_wanted = true;
	tc_change_level = level - 1;
}

/*
 * Coordinator: a segment refused the slice of the OIDs it received (the
 * writer QE of the slice's earlier holder lingers there, see
 * tempcat_claim_slice()).  Called with the error of the QE that the
 * dispatcher turns into an error here, which PL/pgSQL may catch: the first
 * one, which is the refusal, as the segment that refuses is the last one
 * still at work by then.
 */
void
tempcat_note_qe_error(int sqlerrcode, const char *funcname)
{
	if (sqlerrcode == TEMPCAT_SLICE_REFUSED && funcname != NULL &&
		strcmp(funcname, "tempcat_claim_slice") == 0)
		tc_slice_refused = true;
}

/* Coordinator: an OID of the slice goes to segments. */
static void
tempcat_note_given(void)
{
	int			level = GetCurrentTransactionNestLevel();

	/* (the first again after the subtransactions that gave any rolled back) */
	if (!tc_slice_given || tc_slice_given_level == 0)
	{
		tc_slice_given = true;
		tc_slice_given_stmt = GetCurrentStatementStartTimestamp();
	}
	if (tc_slice_given_level == 0 || level < tc_slice_given_level)
		tc_slice_given_level = level;
}

/*
 * The coordinator stops giving OIDs from a slice that a segment refused, if
 * the transaction or subtransaction (e.g. of a PL/pgSQL EXCEPTION block, or
 * a SAVEPOINT) rolls back for that; or that a segment may have been waiting
 * for when the statement was canceled (e.g. by statement_timeout), if the
 * statement gave the first OIDs of it of the transaction (after an earlier
 * one did, all segments hold it; but the statements of one simple Query
 * message share their start); if no transaction that gave OIDs of it
 * committed (then its writer QEs hold it everywhere): a segment would hold
 * up every attempt of the session with it for a while.  A cancel is seen
 * when the error reaches the transaction, as PL/pgSQL drops the error
 * before it rolls back the subtransaction of an EXCEPTION block; not if a
 * handler of query_canceled catches it.  The next attempt takes
 * another one; the slice is let go of once the rows of the transaction are
 * gone (they are freed when the session goes idle or starts its next
 * transaction, and need the clog until then, see tempcat_add_pending()).
 * Not for other rollbacks, which would use up a slice each (a session takes
 * one again only when no other is free, see the file header).
 */
static void
tempcat_drop_refused_slice(void)
{
	/* (the error being handled, if any) */
	if (tc_slice >= 0 && tc_slice_given && !tc_slice_kept &&
		(tc_slice_refused ||
		 (elog_geterrcode() == ERRCODE_QUERY_CANCELED &&
		  tc_slice_given_stmt == GetCurrentStatementStartTimestamp())))
		tempcat_drop_slice();
	tc_slice_refused = false;
}

/*
 * See tempcat_drop_refused_slice().  After an error, the session may also
 * have locked an object for a row in memory that it did not get to add
 * (e.g. as the area was full): look again at what its rows use
 * (tempcat_recompute_used()); and at the end of a transaction that changed
 * such rows, before its locks go.
 */
static void
tempcat_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			if (tc_slice >= 0 && tc_slice_given_level > 0)
				tc_slice_kept = true;
			tc_slice_given = false;
			tc_slice_given_level = 0;
			tc_slice_refused = false;
			if (tc_depend_level > 0)
				tempcat_recompute_used(true);
			tc_depend_level = 0;
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			tempcat_drop_refused_slice();
			tc_slice_given = false;
			tc_slice_given_level = 0;
			if (tc_used != NULL && hash_get_num_entries(tc_used) > 0)
				tc_used_dirty = true;
			if (tc_depend_level > 0)
				tempcat_recompute_used(false);
			tc_depend_level = 0;
			break;
		default:
			break;
	}
}

static void
tempcat_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						 SubTransactionId parentSubid, void *arg)
{
	int			level = GetCurrentTransactionNestLevel();

	/*
	 * (see tempcat_xact_callback()) The OIDs that a subtransaction gave did
	 * not reach the segments for good if it rolls back.
	 */
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		tempcat_drop_refused_slice();
		if (tc_slice_given_level >= level)
			tc_slice_given_level = 0;
		if (tc_used != NULL && hash_get_num_entries(tc_used) > 0)
			tc_used_dirty = true;
		/* only if it changed such rows: not at every exception block */
		if (tc_depend_level >= level)
		{
			tempcat_recompute_used(false);
			tc_depend_level = level - 1;
		}
	}

	/* A committed subtransaction's changes are its parent's now. */
	if (event != SUBXACT_EVENT_COMMIT_SUB)
		return;
	if (tc_change_level >= level)
		tc_change_level = level - 1;
	if (tc_slice_given_level >= level)
		tc_slice_given_level = level - 1;
	if (tc_depend_level >= level)
		tc_depend_level = level - 1;
}

/*
 * Remove the files of a temporary relation whose catalog rows are in
 * memory and going away with the session.
 */
static void
tempcat_unlink_files(Form_pg_class form)
{
	RelFileNode rnode;
	SMgrRelation srel;

	if (form->relpersistence != RELPERSISTENCE_TEMP ||
		!OidIsValid(form->relfilenode))
		return;

	rnode.spcNode = OidIsValid(form->reltablespace) ?
		form->reltablespace : MyDatabaseTableSpace;
	rnode.dbNode = MyDatabaseId;
	rnode.relNode = form->relfilenode;
	srel = smgropen(rnode, TempRelBackendId,
					IsAccessMethodAO(form->relam) ? SMGR_AO : SMGR_MD);
	/* (isRedo: the files may be gone already) */
	smgrdounlinkall(&srel, 1, true);
	smgrclose(srel);
}

/*
 * Runs after the temporary objects were dropped at session exit
 * (RemoveTempRelationsCallback(), registered later, runs earlier), while
 * the area is still attached.  If that cleanup did not finish (it failed, or
 * a prepared transaction kept it from seeing the objects), the files of the
 * relations whose rows are in memory would be left with nothing leading to
 * them until the node restarts: remove them now.
 *
 * And purge the statistics of all our tables and functions: the outcome of
 * that cleanup is not applied to the rows any more (tempcat_forget_if_gone()
 * would purge them), and pgstat_vacuum_stat() keeps the statistics of the
 * OIDs of a slice as long as a session holds it (tempcat_oid_maybe_live()),
 * which the next session to take it may do before: send them now, after
 * the counts still pending, rather than in pgstat_beshutdown_hook(), which
 * runs after the slices are let go of.
 */
static void
tempcat_before_exit(int code, Datum arg)
{
	int			catidx;
	dsa_pointer ep;

	if (tc_area == NULL)
		return;

	/*
	 * Readers that come later, e.g. retrieve connections, must not attach to
	 * an area that is going away with us.
	 */
	if (SharedLocalSnapshotSlot != NULL &&
		SharedLocalSnapshotSlot->tempcat_handle == dsa_get_handle(tc_area))
		SharedLocalSnapshotSlot->tempcat_handle = DSM_HANDLE_INVALID;

	catidx = tempcat_catalog_index(RelationRelationId);
	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);

		if (!TransactionIdIsValid(e->xmax) ||
			tempcat_xid_status(e->xmax) != TCX_COMMITTED)
			tempcat_unlink_files((Form_pg_class) ((char *) hdr + hdr->t_hoff));
		pgstat_purge_relation(tempcat_entry_oid(e));
		ep = e->next;
	}

	catidx = tempcat_catalog_index(ProcedureRelationId);
	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);

		pgstat_purge_function(tempcat_entry_oid(e));
		ep = e->next;
	}

	/*
	 * Now, before the slices are let go of (tempcat_proc_exit()); but not in
	 * a transaction, whose counts are not settled yet (ShutdownPostgres()
	 * rolls it back later, e.g. when the connection went away before the
	 * transaction that created the temporary schema ended, and
	 * RemoveTempRelationsCallback() was not registered to end it):
	 * tempcat_proc_exit() sends them then, still before.
	 */
	if (IsTransactionOrTransactionBlock())
		tc_stats_deferred = true;
	else
		pgstat_report_stat(true);
}

/* ----------------------------------------------------------------
 * Catalog DML
 * ----------------------------------------------------------------
 */

/* Does an owner column of the row hold one of our temporary objects? */
static bool
tempcat_owner_is_temp(HeapTuple tup, TupleDesc desc, AttrNumber attr,
					  Oid ownerclass, AttrNumber ownerclassattr)
{
	Oid			catalog = ownerclassattr != 0 ?
		tempcat_getoid(tup, desc, ownerclassattr) : ownerclass;

	return tempcat_owns_oid(catalog, tempcat_getoid(tup, desc, attr));
}

/*
 * Should this new catalog row be kept in memory?
 */
bool
tempcat_route_insert(Relation rel, HeapTuple tup)
{
	Oid			relid = RelationGetRelid(rel);
	TupleDesc	desc = RelationGetDescr(rel);
	const TempcatCatalogDef *def;
	int			catidx;

	/* The session has no temporary objects in memory: nothing to route. */
	if (tc_owned == NULL)
		return false;

	catidx = tempcat_catalog_index(relid);
	if (catidx < 0)
		return false;
	def = &tempcat_catalogs[catidx];

	/*
	 * pg_shdepend: rows of this database (of our objects, see below).  A row
	 * of one of our objects for another database would point at an object
	 * that does not exist there, and outlive the session on disk (see
	 * copyTemplateDependencies()): refuse it.
	 */
	if (relid == SharedDependRelationId &&
		tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid) != MyDatabaseId)
	{
		if (tempcat_owner_is_temp(tup, desc, def->owner, def->ownerclass,
								  def->ownerclassattr))
			elog(ERROR, "tempcat: pg_shdepend row of a temporary object kept in memory for another database");
		return false;
	}

	if (relid == ConstraintRelationId &&
		!OidIsValid(tempcat_getoid(tup, desc, Anum_pg_constraint_conrelid)))
	{
		/* domain constraint */
		return tempcat_owns_oid(TypeRelationId,
								tempcat_getoid(tup, desc, Anum_pg_constraint_contypid));
	}

	return tempcat_owner_is_temp(tup, desc, def->owner,
								 def->ownerclass, def->ownerclassattr);
}

/*
 * A catalog row that SQL writes (INSERT, COPY FROM or UPDATE, under
 * allow_system_table_mods) is checked as the code that makes such rows
 * checks them: dependencies as recordDependencyOn() does, inheritance as
 * StoreSingleInheritance(), operator links as OperatorCreate(), typed tables
 * as heap_create_with_catalog().
 */
void
tempcat_sql_check(Relation rel, HeapTuple tup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	bool		isnull;

	if (TempcatShared == NULL || tempcat_catalog_index(RelationGetRelid(rel)) < 0)
		return;
	switch (RelationGetRelid(rel))
	{
		case DependRelationId:
			{
				ObjectAddress depender;
				ObjectAddress referenced;

				depender.classId = tempcat_getoid(tup, desc, Anum_pg_depend_classid);
				depender.objectId = tempcat_getoid(tup, desc, Anum_pg_depend_objid);
				depender.objectSubId = DatumGetInt32(heap_getattr(tup, Anum_pg_depend_objsubid,
																  desc, &isnull));
				referenced.classId = tempcat_getoid(tup, desc, Anum_pg_depend_refclassid);
				referenced.objectId = tempcat_getoid(tup, desc, Anum_pg_depend_refobjid);
				referenced.objectSubId = DatumGetInt32(heap_getattr(tup, Anum_pg_depend_refobjsubid,
																	desc, &isnull));
				tempcat_check_dependency(&depender, &referenced);
			}
			break;
		case InheritsRelationId:
			tempcat_check_inheritance(tempcat_getoid(tup, desc, Anum_pg_inherits_inhrelid),
									  tempcat_getoid(tup, desc, Anum_pg_inherits_inhparent));
			break;
		case OperatorRelationId:
			{
				Oid			nsp = tempcat_getoid(tup, desc, Anum_pg_operator_oprnamespace);

				tempcat_check_operator_link(nsp, tempcat_getoid(tup, desc, Anum_pg_operator_oprcom));
				tempcat_check_operator_link(nsp, tempcat_getoid(tup, desc, Anum_pg_operator_oprnegate));
			}
			break;
		case RelationRelationId:
			{
				Oid			typid = tempcat_getoid(tup, desc, Anum_pg_class_reloftype);

				if (OidIsValid(typid))
					tempcat_check_typed_table(tempcat_getoid(tup, desc, Anum_pg_class_oid),
											  typid);
			}
			break;
		default:
			break;
	}
}

/*
 * A catalog row that SQL adds to the heap (INSERT, COPY FROM, or UPDATE of a
 * row on disk): checked as above, and not one of our objects kept in memory,
 * for which the executor would make index entries.
 */
void
tempcat_sql_insert(Relation rel, HeapTuple tup)
{
	if (TempcatShared == NULL || tempcat_catalog_index(RelationGetRelid(rel)) < 0)
		return;
	tempcat_sql_check(rel, tup);
	if (tempcat_route_insert(rel, tup))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add catalog rows of temporary objects kept in memory with SQL")));
}

/*
 * SQL UPDATE of a catalog row in memory whose new version stays in memory:
 * it gets no entries in the catalog's unique indexes, which would refuse a
 * duplicate key.  Refuse a change of their columns instead; others are fine
 * (the gangsize test changes numsegments in gp_distribution_policy).
 */
void
tempcat_sql_update_check(Relation rel, ItemPointer otid, HeapTuple newtup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Bitmapset  *keys;
	dsa_pointer ep;
	HeapTuple	oldtup;
	int			col = -1;

	ep = tempcat_find_tid(otid);
	if (!DsaPointerIsValid(ep))
		return;
	oldtup = tempcat_entry_copy(TC_ADDR(ep), RelationGetRelid(rel));
	keys = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_KEY);
	while ((col = bms_next_member(keys, col)) >= 0)
	{
		AttrNumber	attnum = col + FirstLowInvalidHeapAttributeNumber;
		Form_pg_attribute att;
		Datum		oldval;
		Datum		newval;
		bool		oldnull;
		bool		newnull;

		if (attnum <= 0)
			continue;
		att = TupleDescAttr(desc, attnum - 1);
		oldval = heap_getattr(oldtup, attnum, desc, &oldnull);
		newval = heap_getattr(newtup, attnum, desc, &newnull);
		if (oldnull != newnull ||
			(!oldnull && !datumIsEqual(oldval, newval, att->attbyval, att->attlen)))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot change key columns of catalog rows of temporary objects kept in memory with SQL"),
					 errdetail("Column \"%s\" of \"%s\" is in a unique index.",
							   NameStr(att->attname), RelationGetRelationName(rel))));
	}
	bms_free(keys);
	heap_freetuple(oldtup);
}

/*
 * A new pg_constraint row in memory: refuse a name its relation or domain
 * has already, as the catalog's unique index does for rows on disk, which
 * rows in memory have no entries in.  Code that makes constraints checks the
 * names first, but for constraint triggers (CreateTrigger()), which counts
 * on the index.
 */
static void
tempcat_check_constraint_name(Relation rel, HeapTuple tup)
{
	Form_pg_constraint form = (Form_pg_constraint) GETSTRUCT(tup);
	ScanKeyData key[3];
	SysScanDesc scan;
	bool		found;

	ScanKeyInit(&key[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(form->conrelid));
	ScanKeyInit(&key[1], Anum_pg_constraint_contypid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(form->contypid));
	ScanKeyInit(&key[2], Anum_pg_constraint_conname, BTEqualStrategyNumber,
				F_NAMEEQ, NameGetDatum(&form->conname));
	scan = systable_beginscan(rel, ConstraintRelidTypidNameIndexId, true,
							  SnapshotSelf, 3, key);
	found = HeapTupleIsValid(systable_getnext(scan));
	systable_endscan(scan);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("duplicate key value violates unique constraint \"%s\"",
						"pg_constraint_conrelid_contypid_conname_index"),
				 errdetail("Key (conrelid, contypid, conname)=(%u, %u, %s) already exists.",
						   form->conrelid, form->contypid, NameStr(form->conname))));
}

/*
 * The row to keep in memory for a catalog row: with the values that live
 * out of line (TOAST pointers, e.g. of a row that SQL UPDATE built from
 * another row) brought in, which could otherwise be dropped under it, and,
 * if it is big, its values compressed as heap_insert() would compress them
 * (statistics, function bodies, rules), so that they do not take the
 * session's memory at full size.  Nothing goes out of line: there is no
 * TOAST table of ours.  Returns 'tup' itself if nothing changes.
 *
 * Then refuse a row that would not fit on disk, as heap_insert() and
 * heap_update() would: in a catalog without a TOAST table, one that
 * compression does not bring under MaxHeapTupleSize (e.g. a long column
 * default in pg_attribute, long options in pg_class).
 */
static HeapTuple
tempcat_prepare_row(Relation rel, HeapTuple tup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	HeapTuple	row = tup;
	Size		len;

	if (HeapTupleHasExternal(row))
		row = toast_flatten_tuple(row, desc);

	if (row->t_len > TOAST_TUPLE_THRESHOLD)
	{
		Datum	   *values = palloc(sizeof(Datum) * desc->natts);
		bool	   *nulls = palloc(sizeof(bool) * desc->natts);
		bool		compressed = false;
		int			i;

		heap_deform_tuple(row, desc, values, nulls);
		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);
			Datum		cvalue;

			if (nulls[i] || att->attlen != -1 ||
				(att->attstorage != 'x' && att->attstorage != 'm') ||
				!VARATT_IS_4B_U(DatumGetPointer(values[i])))
				continue;
			cvalue = toast_compress_datum(values[i]);
			if (DatumGetPointer(cvalue) != NULL)
			{
				values[i] = cvalue;
				compressed = true;
			}
		}
		if (compressed)
		{
			HeapTuple	crow = heap_form_tuple(desc, values, nulls);

			crow->t_self = tup->t_self;
			crow->t_tableOid = tup->t_tableOid;
			if (row != tup)
				heap_freetuple(row);
			row = crow;
		}
		pfree(values);
		pfree(nulls);
	}

	len = MAXALIGN(row->t_len);
	if (len > MaxHeapTupleSize && !OidIsValid(rel->rd_rel->reltoastrelid))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %zu, maximum size %zu",
						len, MaxHeapTupleSize)));
	return row;
}

/*
 * The area is full even after freeing what could be freed, for a row of
 * 'len' bytes.
 */
static void
tempcat_report_full(uint32 len)
{
	size_t		limit = tc_area_limit;
	size_t		total = dsa_get_total_size(tc_area);

	/*
	 * (well short of the limit, by more than the last segment could have
	 * been and than the row needs: the room left to others, or the session's
	 * vmem, see tempcat_create_area())
	 */
	if (total + MAXALIGN(sizeof(TempcatEntry)) + len < limit - limit / 8)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of room for the catalog rows of temporary objects"),
				 errdetail("The session keeps them in %zu kB of memory, as dynamic shared memory left to others or the memory limit of the session does not let it take up to %zu kB.",
						   total / 1024, limit / 1024),
				 errhint("Drop temporary objects.")));
	ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("out of room for the catalog rows of temporary objects"),
			 errdetail("The session keeps them in up to %zu kB of memory.",
					   limit / 1024),
			 errhint("Drop temporary objects, or raise gp_temp_memory_catalog_max_size for new sessions.")));
}

/*
 * Create an entry, freeing dead rows of the current transaction if the area
 * is full.  Caller holds the exclusive lock, and releases it on error.
 */
static dsa_pointer
tempcat_entry_create_or_reclaim(int catidx, HeapTuple tup, TupleDesc desc,
								TransactionId xid, CommandId cid)
{
	dsa_pointer ep;

	ep = tempcat_entry_create(catidx, tup, desc, xid, cid);
	if (!DsaPointerIsValid(ep) && tempcat_reclaim_dead() > 0)
		ep = tempcat_entry_create(catidx, tup, desc, xid, cid);
	return ep;
}

/* Insert a row in memory. */
void
tempcat_insert(Relation rel, HeapTuple tup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	TransactionId xid;
	CommandId	cid;
	dsa_pointer ep;
	HeapTuple	row;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader() && tc_area != NULL);
	/* as heap_insert() does; see tempcat_delete() */
	gp_expand_protect_catalog_changes(rel);
	if (RelationGetRelid(rel) == ConstraintRelationId)
		tempcat_check_constraint_name(rel, tup);
	row = tempcat_prepare_row(rel, tup);

	tempcat_use_referenced(rel, tup);

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	tempcat_reserve_pending(1);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	ep = tempcat_entry_create_or_reclaim(catidx, row, RelationGetDescr(rel),
										 xid, cid);
	if (DsaPointerIsValid(ep))
		tempcat_add_pending(ep);
	LWLockRelease(&tc_root->lock);

	if (!DsaPointerIsValid(ep))
		tempcat_report_full(row->t_len);
	if (row != tup)
		heap_freetuple(row);

	tempcat_set_caller_tuple(tup, rel, TC_ADDR(ep));
	if (RelationGetRelid(rel) == RelationRelationId)
		tempcat_cover_pg_class_row(tup->t_data);

	CacheInvalidateHeapTuple(rel, tup, NULL);
}

static TempcatEntry *
tempcat_lookup_tid(Relation rel, ItemPointer tid, dsa_pointer *ep)
{
	if (tc_area == NULL && !tempcat_attach())
		elog(ERROR, "tempcat: no in-memory catalog for row (%u,%u)",
			 ItemPointerGetBlockNumberNoCheck(tid), tid->ip_posid);

	*ep = tempcat_find_tid(tid);
	if (!DsaPointerIsValid(*ep))
		elog(ERROR, "tempcat: row (%u,%u) of \"%s\" not found in memory",
			 ItemPointerGetBlockNumberNoCheck(tid), tid->ip_posid,
			 RelationGetRelationName(rel));

	return TC_ADDR(*ep);
}

/* Complain if the row cannot be deleted, like heap_delete() would. */
static void
tempcat_check_deletable(TempcatEntry *e)
{
	if (TransactionIdIsValid(e->xmax))
	{
		if (TransactionIdIsCurrentTransactionId(e->xmax))
			elog(ERROR, "tuple already updated by self");
		if (TransactionIdDidCommit(e->xmax))
			elog(ERROR, "tuple concurrently updated");
		/* deleted by an aborted transaction: may be overwritten */
	}
}

/*
 * For SQL DML on a catalog (heapam_handler.c): has the current transaction
 * already deleted or updated the row?  Like heap_delete() and
 * heap_update(): TM_SelfModified, with 'tmfd' filled in, if the command
 * 'cid' (or a later one) did, e.g. one that meets the row twice, and the
 * executor skips the row; TM_Invisible if an earlier command did, which the
 * caller's snapshot should not have let it see; TM_Ok otherwise.  (A
 * TM_Result, as an int for tempcat.h.)
 */
int
tempcat_self_modified(Relation rel, ItemPointer tid, CommandId cid,
					  TM_FailureData *tmfd)
{
	dsa_pointer ep;
	TempcatEntry *e = tempcat_lookup_tid(rel, tid, &ep);

	if (!TransactionIdIsValid(e->xmax) ||
		!TransactionIdIsCurrentTransactionId(e->xmax))
		return TM_Ok;
	if (e->cmax < cid)
		return TM_Invisible;
	tmfd->ctid = *tid;
	tmfd->xmax = e->xmax;
	tmfd->cmax = e->cmax;
	return TM_SelfModified;
}

/* Set xmax on an entry.  Caller holds the lock and has checked it. */
static void
tempcat_mark_deleted(TempcatEntry *e, dsa_pointer ep,
					 TransactionId xid, CommandId cid)
{
	e->xmax = xid;
	e->cmax = cid;
	tempcat_add_pending(ep);
	tc_ndeletions++;
	if (tempcat_catalogs[e->catidx].relid == DependRelationId ||
		tempcat_catalogs[e->catidx].relid == SharedDependRelationId)
		tc_depend_level = Max(tc_depend_level, GetCurrentTransactionNestLevel());
}

/* Replace a row in memory by a new version, in memory, by command 'cid'. */
void
tempcat_update(Relation rel, ItemPointer otid, HeapTuple newtup,
			   CommandId cid)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	dsa_pointer oldep;
	dsa_pointer newep;
	TempcatEntry *olde;
	HeapTuple	oldtup;
	HeapTuple	row;
	TransactionId xid;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader());
	/* as heap_update() does; see tempcat_delete() */
	gp_expand_protect_catalog_changes(rel);
	row = tempcat_prepare_row(rel, newtup);

	olde = tempcat_lookup_tid(rel, otid, &oldep);
	tempcat_check_deletable(olde);
	oldtup = tempcat_entry_copy(olde, RelationGetRelid(rel));

	tempcat_use_referenced(rel, newtup);

	xid = GetCurrentTransactionId();

	tempcat_reserve_pending(2);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	newep = tempcat_entry_create_or_reclaim(catidx, row, RelationGetDescr(rel),
											xid, cid);
	if (!DsaPointerIsValid(newep))
	{
		LWLockRelease(&tc_root->lock);
		tempcat_report_full(row->t_len);
	}
	tempcat_mark_deleted(olde, oldep, xid, cid);
	tempcat_add_pending(newep);
	LWLockRelease(&tc_root->lock);
	if (row != newtup)
		heap_freetuple(row);

	tempcat_set_caller_tuple(newtup, rel, TC_ADDR(newep));
	if (RelationGetRelid(rel) == RelationRelationId)
	{
		tempcat_cover_pg_class_row(newtup->t_data);
		tc_horizon_dirty = true;
	}

	CacheInvalidateHeapTuple(rel, oldtup, newtup);
	heap_freetuple(oldtup);
}

void
tempcat_delete(Relation rel, ItemPointer tid, CommandId cid)
{
	dsa_pointer ep;
	TempcatEntry *e;
	HeapTuple	oldtup;
	TransactionId xid;

	Assert(!tempcat_is_reader());

	/*
	 * As heap_delete() does: while gpexpand runs, and once it has added
	 * segments, the session's catalog rows change as little as on disk.  Rows
	 * in memory are not copied to new segments, but the session still has
	 * the old segments in its gangs and in the numsegments of new tables
	 * (cdbcomponent_getCdbComponents() is not refreshed while it has a
	 * temporary namespace): a new connection should be made.
	 */
	gp_expand_protect_catalog_changes(rel);

	e = tempcat_lookup_tid(rel, tid, &ep);
	tempcat_check_deletable(e);
	oldtup = tempcat_entry_copy(e, RelationGetRelid(rel));

	xid = GetCurrentTransactionId();

	tempcat_reserve_pending(1);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	tempcat_mark_deleted(e, ep, xid, cid);
	LWLockRelease(&tc_root->lock);

	if (RelationGetRelid(rel) == RelationRelationId)
		tc_horizon_dirty = true;

	CacheInvalidateHeapTuple(rel, oldtup, NULL);
	heap_freetuple(oldtup);
}

/*
 * Overwrite a row in place, non-transactionally, like heap_inplace_update().
 * In-place updates overwrite fixed-width columns only.
 */
void
tempcat_inplace_update(Relation rel, HeapTuple tup)
{
	dsa_pointer ep;
	TempcatEntry *e;
	HeapTupleHeader hdr;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, &tup->t_self, &ep);
	hdr = TCE_DATA(e);
	if (tup->t_len != e->len || tup->t_data->t_hoff != hdr->t_hoff)
		elog(ERROR, "wrong tuple length");

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	memcpy((char *) hdr + hdr->t_hoff,
		   (char *) tup->t_data + tup->t_data->t_hoff,
		   tup->t_len - hdr->t_hoff);
	LWLockRelease(&tc_root->lock);

	/* e.g. VACUUM advancing relfrozenxid */
	if (RelationGetRelid(rel) == RelationRelationId)
	{
		tempcat_cover_pg_class_row(hdr);
		tc_horizon_dirty = true;
	}

	(void) GetCurrentCommandId(true);
	CacheInvalidateHeapTuple(rel, tup, NULL);
}

/*
 * Make a row visible to everybody regardless of the inserting transaction's
 * fate, like heap_freeze_tuple_wal_logged().
 */
void
tempcat_freeze(Relation rel, HeapTuple tup)
{
	dsa_pointer ep;
	TempcatEntry *e;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, &tup->t_self, &ep);

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	e->xmin = FrozenTransactionId;
	e->flags |= TCE_XMIN_COMMITTED;
	LWLockRelease(&tc_root->lock);

	HeapTupleHeaderSetXmin(tup->t_data, FrozenTransactionId);
}

/* ----------------------------------------------------------------
 * Catalog scans
 * ----------------------------------------------------------------
 */

/* Compare two values of key column 'i' of the scan's index. */
static int
tempcat_compare_key(TempcatScan scan, int i,
					Datum ad, bool anull, Datum bd, bool bnull)
{
	int			cmp;

	if (anull || bnull)
	{
		bool		nulls_first =
		(scan->irel->rd_indoption[i] & INDOPTION_NULLS_FIRST) != 0;

		if (anull && bnull)
			return 0;
		return (anull == nulls_first) ? -1 : 1;
	}

	cmp = DatumGetInt32(FunctionCall2Coll(scan->procs[i],
										  scan->irel->rd_indcollation[i],
										  ad, bd));
	if (scan->irel->rd_indoption[i] & INDOPTION_DESC)
		cmp = -cmp;
	return cmp;
}

/* Compare two rows of the scanned catalog by the scan's index key. */
static int
tempcat_compare(TempcatScan scan, HeapTuple a, HeapTuple b)
{
	int			i;

	for (i = 0; i < scan->nidxkeys; i++)
	{
		AttrNumber	attno = scan->irel->rd_index->indkey.values[i];
		bool		anull;
		bool		bnull;
		Datum		ad = heap_getattr(a, attno, scan->heapdesc, &anull);
		Datum		bd = heap_getattr(b, attno, scan->heapdesc, &bnull);
		int			cmp = tempcat_compare_key(scan, i, ad, anull, bd, bnull);

		if (cmp != 0)
			return cmp;
	}
	return 0;
}

/* Compare a row of the scanned catalog with an entry of the scan's index. */
static int
tempcat_compare_itup(TempcatScan scan, HeapTuple a,
					 IndexTuple b, TupleDesc itupdesc)
{
	int			i;

	for (i = 0; i < scan->nidxkeys; i++)
	{
		AttrNumber	attno = scan->irel->rd_index->indkey.values[i];
		bool		anull;
		bool		bnull;
		Datum		ad = heap_getattr(a, attno, scan->heapdesc, &anull);
		Datum		bd = index_getattr(b, i + 1, itupdesc, &bnull);
		int			cmp = tempcat_compare_key(scan, i, ad, anull, bd, bnull);

		if (cmp != 0)
			return cmp;
	}
	return 0;
}

static int
tempcat_qsort_cmp(const void *a, const void *b, void *arg)
{
	return tempcat_compare((TempcatScan) arg,
						   *(const HeapTuple *) a, *(const HeapTuple *) b);
}

/*
 * Collect the visible rows in memory matching the scan keys.  Must be called
 * while the keys still refer to heap attribute numbers, i.e. before
 * systable_beginscan() converts them to index column numbers.  With an
 * index, the rows are sorted by its key, to be merged with the on-disk rows
 * (tempcat_getnext()).
 *
 * Returns NULL if there can be no rows in memory for this catalog.
 */
TempcatScan
tempcat_beginscan(Relation rel, Relation irel, Snapshot snapshot,
				  int nkeys, ScanKey key)
{
	int			catidx;
	const TempcatCatalogDef *def;
	TempcatCatalog *cat;
	TempcatScan scan;
	TupleDesc	desc;
	bool		reader;
	int			usekey = -1;
	uint32		usehash = 0;
	dsa_pointer np;
	int			k;
	int			i;
	int			maxtuples;

	/* the area first: there is none with the feature off, most often */
	if (tc_area == NULL && !tempcat_attach())
		return NULL;
	catidx = tempcat_catalog_index(RelationGetRelid(rel));
	if (catidx < 0)
		return NULL;

	cat = &tc_root->catalogs[catidx];
	if (cat->nentries == 0)
		return NULL;

	def = &tempcat_catalogs[catidx];
	desc = RelationGetDescr(rel);
	if (snapshot == NULL)
		snapshot = GetCatalogSnapshot(RelationGetRelid(rel));

	/* Use a hashed column if the scan has an equality key on it. */
	for (k = 0; k < TEMPCAT_NKEYS && usekey < 0; k++)
	{
		if (def->keytype[k] == TCK_NONE)
			continue;
		for (i = 0; i < nkeys; i++)
		{
			/*
			 * The argument must have the column's type for the hash to
			 * match: index scan keys can be cross-type (e.g. name = text).
			 */
			if (key[i].sk_attno == def->keyattr[k] &&
				key[i].sk_strategy == BTEqualStrategyNumber &&
				!(key[i].sk_flags & SK_ISNULL) &&
				(key[i].sk_subtype == InvalidOid ||
				 key[i].sk_subtype == (def->keytype[k] == TCK_OID ? OIDOID : NAMEOID)))
			{
				usekey = k;
				usehash = tempcat_key_hash(def->keytype[k], key[i].sk_argument);
				break;
			}
		}
	}

	scan = palloc(sizeof(struct TempcatScanData));
	scan->ntuples = 0;
	scan->next = 0;
	scan->relid = RelationGetRelid(rel);
	scan->current = NULL;
	scan->on_virtual = false;
	scan->irel = irel;
	scan->heapdesc = desc;
	scan->nidxkeys = 0;
	scan->procs = NULL;
	scan->idxcoldesc = NULL;
	scan->disk_tuple = NULL;
	scan->disk_done = false;
	scan->direction = 0;
	maxtuples = 8;
	if (irel != NULL)
	{
		scan->tuples = palloc(sizeof(HeapTuple) * maxtuples);
		scan->tids = NULL;
	}
	else
	{
		scan->tuples = NULL;
		scan->tids = palloc(sizeof(ItemPointerData) * maxtuples);
	}

	reader = tempcat_is_reader();
	if (reader)
		LWLockAcquire(&tc_root->lock, LW_SHARED);

	if (usekey >= 0)
	{
		TempcatHash *h = &cat->keys[usekey];

		np = h->nbuckets == 0 ? InvalidDsaPointer :
			((dsa_pointer *) TC_ADDR(h->buckets))[usehash & (h->nbuckets - 1)];
	}
	else
		np = cat->head;

	while (DsaPointerIsValid(np))
	{
		TempcatEntry *e;
		HeapTupleData htup;
		bool		matches;

		if (usekey >= 0)
		{
			TempcatHashNode *node = TC_ADDR(np);

			np = node->next;
			if (node->hash != usehash)
				continue;
			e = TC_ADDR(node->entry);
		}
		else
		{
			e = TC_ADDR(np);
			np = e->next;
		}

		if (!tempcat_entry_visible(e, snapshot))
			continue;

		htup.t_len = e->len;
		htup.t_self = e->tid;
		htup.t_tableOid = RelationGetRelid(rel);
		htup.t_data = TCE_DATA(e);
		HeapKeyTest(&htup, desc, nkeys, key, matches);
		if (!matches)
			continue;

		if (scan->ntuples >= maxtuples)
		{
			maxtuples *= 2;
			if (irel != NULL)
				scan->tuples = repalloc(scan->tuples, sizeof(HeapTuple) * maxtuples);
			else
				scan->tids = repalloc(scan->tids, sizeof(ItemPointerData) * maxtuples);
		}
		if (irel != NULL)
			scan->tuples[scan->ntuples++] = tempcat_entry_copy(e, RelationGetRelid(rel));
		else
			scan->tids[scan->ntuples++] = e->tid;
	}

	if (reader)
		LWLockRelease(&tc_root->lock);

	if (irel != NULL)
	{
		scan->nidxkeys = IndexRelationGetNumberOfKeyAttributes(irel);
		scan->procs = palloc(sizeof(FmgrInfo *) * scan->nidxkeys);
		for (i = 0; i < scan->nidxkeys; i++)
			scan->procs[i] = index_getprocinfo(irel, i + 1, BTORDER_PROC);
		if (scan->ntuples > 1)
			qsort_arg(scan->tuples, scan->ntuples, sizeof(HeapTuple),
					  tempcat_qsort_cmp, scan);
	}

	return scan;
}

/*
 * Begin a scan of the visible rows in memory of catalog 'rel' for a scan of
 * its index 'irel' by a plan, or return NULL if there are none.  Index and
 * index-only scans merge the rows into the scan of the index (see
 * index_getnext_slot() and index_getnext_tid()), bitmap heap scans return
 * them after the heap (see BitmapHeapNext()); without 'irel' and keys for
 * the latter, all rows.  The keys are those of the index scan: those on
 * simple index columns are converted to heap ones, the others are dropped,
 * as the executor rechecks the rows in memory against the original quals.
 */
TempcatScan
tempcat_begin_index_scan(Relation rel, Relation irel, Snapshot snapshot,
						 int nkeys, ScanKey keys)
{
	TempcatScan scan;
	ScanKey		hkeys;
	int			nhkeys = 0;
	int			i;

	Assert(irel != NULL || nkeys == 0);
	if (snapshot == NULL || !IsMVCCSnapshot(snapshot) ||
		!tempcat_has_rows(rel))
		return NULL;

	hkeys = palloc(sizeof(ScanKeyData) * Max(nkeys, 1));
	for (i = 0; i < nkeys; i++)
	{
		ScanKey		k = &keys[i];
		AttrNumber	attno;

		if (k->sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_SEARCHARRAY |
						   SK_SEARCHNULL | SK_SEARCHNOTNULL | SK_ORDER_BY))
			continue;
		if (k->sk_attno < 1 ||
			k->sk_attno > IndexRelationGetNumberOfKeyAttributes(irel))
			continue;
		attno = irel->rd_index->indkey.values[k->sk_attno - 1];
		if (attno <= 0)
			continue;
		hkeys[nhkeys] = *k;
		hkeys[nhkeys].sk_attno = attno;
		nhkeys++;
	}

	scan = tempcat_beginscan(rel, irel, snapshot, nhkeys, hkeys);
	pfree(hkeys);
	if (scan != NULL && scan->ntuples == 0)
	{
		tempcat_endscan(scan);
		scan = NULL;
	}
	return scan;
}

/* The next row in memory in index order, without taking it. */
static HeapTuple
tempcat_merge_peek(TempcatScan scan, bool forward)
{
	int			dir = forward ? 1 : -1;

	Assert(scan->irel != NULL);
	if (scan->direction == 0)
		scan->direction = dir;
	else if (scan->direction != dir)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change the direction of a catalog scan that includes rows of temporary objects kept in memory")));

	if (scan->next >= scan->ntuples)
		return NULL;
	return scan->tuples[forward ? scan->next : scan->ntuples - 1 - scan->next];
}

/* The position in the rows in memory of a scan through an index */
int
tempcat_scan_getpos(TempcatScan scan)
{
	return scan->next;
}

void
tempcat_scan_setpos(TempcatScan scan, int pos)
{
	Assert(pos >= 0 && pos <= scan->ntuples);
	scan->next = pos;
}

/*
 * The next row in memory of a scan through an index to return before the
 * on-disk row 'disk' (NULL if there are no more of those), or NULL if 'disk'
 * comes first.  Both come in index order, backward if !forward.
 */
HeapTuple
tempcat_merge_next(TempcatScan scan, bool forward, HeapTuple disk)
{
	HeapTuple	vtup = tempcat_merge_peek(scan, forward);

	if (vtup == NULL ||
		(disk != NULL && tempcat_compare(scan, vtup, disk) * (forward ? 1 : -1) > 0))
		return NULL;
	scan->next++;
	return vtup;
}

/* Likewise, before the index entry 'disk' of an index-only scan */
HeapTuple
tempcat_merge_next_itup(TempcatScan scan, bool forward,
						IndexTuple disk, TupleDesc itupdesc)
{
	HeapTuple	vtup = tempcat_merge_peek(scan, forward);

	if (vtup == NULL ||
		(disk != NULL &&
		 tempcat_compare_itup(scan, vtup, disk, itupdesc) * (forward ? 1 : -1) > 0))
		return NULL;
	scan->next++;
	return vtup;
}

/*
 * The columns of the scan's index of row 'tup', as an index-only scan
 * returns them: in the types of the catalog's columns, which those of the
 * index may differ from (btree name_ops store cstrings), with their row
 * type in *desc.
 */
HeapTuple
tempcat_index_columns(TempcatScan scan, HeapTuple tup, TupleDesc *desc)
{
	int			natts = IndexRelationGetNumberOfAttributes(scan->irel);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	int			i;

	if (scan->idxcoldesc == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(scan));

		scan->idxcoldesc = CreateTemplateTupleDesc(natts);
		for (i = 0; i < natts; i++)
		{
			AttrNumber	attno = scan->irel->rd_index->indkey.values[i];

			if (attno <= 0)
				elog(ERROR, "cannot return column %d of index \"%s\" of a row in memory",
					 i + 1, RelationGetRelationName(scan->irel));
			TupleDescCopyEntry(scan->idxcoldesc, i + 1, scan->heapdesc, attno);
		}
		MemoryContextSwitchTo(oldcxt);
	}

	for (i = 0; i < natts; i++)
		values[i] = heap_getattr(tup, scan->irel->rd_index->indkey.values[i],
								 scan->heapdesc, &isnull[i]);
	*desc = scan->idxcoldesc;
	return heap_form_tuple(scan->idxcoldesc, values, isnull);
}

/*
 * Return the next row: rows in memory merged with the on-disk rows that
 * fetch_disk() returns.  For scans through an index both come in index
 * order (backward if !forward, for ordered scans); otherwise the rows in
 * memory come first.
 */
HeapTuple
tempcat_getnext_dir(TempcatScan scan, bool forward,
					HeapTuple (*fetch_disk) (void *), void *arg)
{
	HeapTuple	vtup;

	if (scan->irel == NULL)
	{
		if ((vtup = tempcat_next_virtual(scan)) != NULL)
		{
			scan->on_virtual = true;
			return vtup;
		}
		scan->on_virtual = false;
		return fetch_disk(arg);
	}

	if (scan->disk_tuple == NULL && !scan->disk_done)
	{
		scan->disk_tuple = fetch_disk(arg);
		if (scan->disk_tuple == NULL)
			scan->disk_done = true;
	}

	vtup = tempcat_merge_next(scan, forward, scan->disk_tuple);
	if (vtup != NULL)
	{
		scan->on_virtual = true;
		return vtup;
	}

	scan->on_virtual = false;
	if (scan->disk_tuple != NULL)
	{
		HeapTuple	tup = scan->disk_tuple;

		scan->disk_tuple = NULL;
		return tup;
	}
	return NULL;
}

HeapTuple
tempcat_getnext(TempcatScan scan, HeapTuple (*fetch_disk) (void *), void *arg)
{
	return tempcat_getnext_dir(scan, true, fetch_disk, arg);
}

/* Does the catalog hold rows in memory that this process can see? */
bool
tempcat_has_rows(Relation rel)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));

	return catidx >= 0 && (tc_area != NULL || tempcat_attach()) &&
		tc_root->catalogs[catidx].nentries > 0;
}

/*
 * The next row in memory, in no particular order, or NULL: a copy, valid
 * until the next call (for a scan without an index; otherwise until the
 * end of the scan).  The rows were visible to the scan's snapshot when
 * it began, and cannot have been freed since (see tempcat_reclaim_dead());
 * one that was all the same is passed over.
 */
HeapTuple
tempcat_next_virtual(TempcatScan scan)
{
	bool		reader = tempcat_is_reader();

	/* (a bitmap heap scan's, begun with the keys of an index) */
	if (scan->irel != NULL)
		return scan->next < scan->ntuples ? scan->tuples[scan->next++] : NULL;

	if (scan->current != NULL)
	{
		pfree(scan->current);
		scan->current = NULL;
	}
	while (scan->next < scan->ntuples)
	{
		ItemPointerData tid = scan->tids[scan->next++];
		dsa_pointer ep;

		if (reader)
			LWLockAcquire(&tc_root->lock, LW_SHARED);
		ep = tempcat_find_tid(&tid);
		if (DsaPointerIsValid(ep))
		{
			/* (in the scan's context, which the caller's may outlive) */
			MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(scan));

			scan->current = tempcat_entry_copy(TC_ADDR(ep), scan->relid);
			MemoryContextSwitchTo(oldcxt);
		}
		if (reader)
			LWLockRelease(&tc_root->lock);
		if (scan->current != NULL)
			return scan->current;
	}
	return NULL;
}

bool
tempcat_scan_on_virtual(TempcatScan scan)
{
	return scan != NULL && scan->on_virtual;
}

void
tempcat_endscan(TempcatScan scan)
{
	int			i;

	if (scan->tuples != NULL)
	{
		for (i = 0; i < scan->ntuples; i++)
			pfree(scan->tuples[i]);
		pfree(scan->tuples);
	}
	if (scan->tids != NULL)
		pfree(scan->tids);
	if (scan->current != NULL)
		pfree(scan->current);
	if (scan->procs)
		pfree(scan->procs);
	if (scan->idxcoldesc)
		FreeTupleDesc(scan->idxcoldesc);
	pfree(scan);
}

/*
 * Fetch a row in memory by TID, if visible to the snapshot.  Returns a
 * palloc'd copy, or NULL.
 */
HeapTuple
tempcat_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	dsa_pointer ep;
	HeapTuple	result = NULL;
	bool		reader;

	if (tempcat_catalog_index(RelationGetRelid(rel)) < 0 ||
		(tc_area == NULL && !tempcat_attach()))
		return NULL;

	reader = tempcat_is_reader();
	if (reader)
		LWLockAcquire(&tc_root->lock, LW_SHARED);

	ep = tempcat_find_tid(tid);
	if (DsaPointerIsValid(ep))
	{
		TempcatEntry *e = TC_ADDR(ep);

		if (tempcat_catalogs[e->catidx].relid == RelationGetRelid(rel) &&
			tempcat_entry_visible(e, snapshot))
			result = tempcat_entry_copy(e, RelationGetRelid(rel));
	}

	if (reader)
		LWLockRelease(&tc_root->lock);
	return result;
}

/*
 * Does catalog 'rel' hold rows in memory that 'snapshot' sees?
 */
static bool
tempcat_has_visible_rows(Relation rel, Snapshot snapshot)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	bool		reader;
	bool		found = false;
	dsa_pointer ep;

	if (catidx < 0 || (tc_area == NULL && !tempcat_attach()) ||
		tc_root->catalogs[catidx].nentries == 0)
		return false;
	if (snapshot == NULL)
		return true;

	reader = tempcat_is_reader();
	if (reader)
		LWLockAcquire(&tc_root->lock, LW_SHARED);
	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep) && !found;)
	{
		TempcatEntry *e = TC_ADDR(ep);

		found = tempcat_entry_visible(e, snapshot);
		ep = e->next;
	}
	if (reader)
		LWLockRelease(&tc_root->lock);
	return found;
}

/*
 * Sample scans of a catalog do not see its rows in memory (they read pages
 * of the heap): refuse them rather than miss rows.
 */
void
tempcat_check_sample_scan(Relation rel, Snapshot snapshot)
{
	if (tempcat_has_visible_rows(rel, snapshot))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot sample catalog \"%s\", which holds rows of temporary objects kept in memory",
						RelationGetRelationName(rel))));
}

/*
 * Ordinary tables and functions with reserved OIDs (created before the
 * range was reserved, or carried over by pg_upgrade) dropped by the current
 * transaction, at subtransaction nesting levels.  pgstat_vacuum_stat() keeps
 * their statistics as long as a live session holds the slice of their OIDs
 * (tempcat_oid_maybe_live()): purge them at commit.
 */
typedef struct TempcatDiskDrop
{
	Oid			catalog;
	Oid			oid;
	int			level;
} TempcatDiskDrop;

static TempcatDiskDrop *tc_disk_drops = NULL;
static int	tc_ndisk_drops = 0;
static int	tc_max_disk_drops = 0;

/*
 * Those of the transaction this process prepared last, until it finishes it
 * (FinishPreparedTransaction(), on a QE in the same process): purge them
 * only if it commits.  If another process finishes it, e.g. after a
 * restart, they are not purged, as before this was done.
 */
static TempcatDiskDrop *tc_prepared_drops = NULL;
static int	tc_nprepared_drops = 0;
static TransactionId tc_prepared_drops_xid = InvalidTransactionId;
static TransactionId tc_preparing_xid = InvalidTransactionId;

static void
tempcat_purge_disk_drops(TempcatDiskDrop *drops, int n)
{
	int			i;

	for (i = 0; i < n; i++)
	{
		if (drops[i].catalog == RelationRelationId)
			pgstat_purge_relation(drops[i].oid);
		else
			pgstat_purge_function(drops[i].oid);
	}
}

static void
tempcat_disk_drop_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			tempcat_purge_disk_drops(tc_disk_drops, tc_ndisk_drops);
			tc_ndisk_drops = 0;
			break;
		case XACT_EVENT_ABORT:
			tc_ndisk_drops = 0;
			break;
		case XACT_EVENT_PRE_PREPARE:
			tc_preparing_xid = GetTopTransactionIdIfAny();
			break;
		case XACT_EVENT_PREPARE:
			/* (a transaction prepared earlier and not finished here: lost) */
			if (tc_prepared_drops != NULL)
				pfree(tc_prepared_drops);
			tc_prepared_drops = NULL;
			tc_nprepared_drops = 0;
			tc_prepared_drops_xid = InvalidTransactionId;
			if (tc_ndisk_drops > 0 && TransactionIdIsValid(tc_preparing_xid))
			{
				tc_prepared_drops = MemoryContextAlloc(TopMemoryContext,
													   sizeof(TempcatDiskDrop) * tc_ndisk_drops);
				memcpy(tc_prepared_drops, tc_disk_drops,
					   sizeof(TempcatDiskDrop) * tc_ndisk_drops);
				tc_nprepared_drops = tc_ndisk_drops;
				tc_prepared_drops_xid = tc_preparing_xid;
			}
			tc_ndisk_drops = 0;
			tc_preparing_xid = InvalidTransactionId;
			break;
		default:
			break;
	}
}

/*
 * The prepared transaction 'xid' is committed or rolled back, by this
 * process (on a QE, the writer that prepared it), or by another one.  Apply
 * the outcome to what this process noted for it at PREPARE: the statistics
 * of ordinary objects it dropped.  (The slices of the rows of a prepared
 * transaction that rolled back are let go of with the rows, see
 * tempcat_release_unused_slices().)
 */
void
tempcat_finish_prepared(TransactionId xid, bool isCommit)
{
	if (!TransactionIdIsValid(xid))
		return;

	if (tc_nprepared_drops == 0 || !TransactionIdEquals(xid, tc_prepared_drops_xid))
		return;
	if (isCommit)
		tempcat_purge_disk_drops(tc_prepared_drops, tc_nprepared_drops);
	pfree(tc_prepared_drops);
	tc_prepared_drops = NULL;
	tc_nprepared_drops = 0;
	tc_prepared_drops_xid = InvalidTransactionId;
}

static void
tempcat_disk_drop_subxact_callback(SubXactEvent event,
								   SubTransactionId mySubid,
								   SubTransactionId parentSubid, void *arg)
{
	int			level = GetCurrentTransactionNestLevel();
	int			i;

	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		while (tc_ndisk_drops > 0 &&
			   tc_disk_drops[tc_ndisk_drops - 1].level >= level)
			tc_ndisk_drops--;
	}
	else if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		for (i = 0; i < tc_ndisk_drops; i++)
		{
			if (tc_disk_drops[i].level >= level)
				tc_disk_drops[i].level = level - 1;
		}
	}
}

/* An on-disk pg_class or pg_proc row at 'tid' with OID 'oid' is deleted. */
void
tempcat_note_disk_drop(Oid catalog, Oid oid, ItemPointer tid)
{
	static bool registered = false;

	Assert(catalog == RelationRelationId || catalog == ProcedureRelationId);
	if (!IsTempcatOid(oid) || IsTempcatTid(tid) || IsBinaryUpgrade)
		return;

	if (!registered)
	{
		RegisterXactCallback(tempcat_disk_drop_xact_callback, NULL);
		RegisterSubXactCallback(tempcat_disk_drop_subxact_callback, NULL);
		registered = true;
	}
	if (tc_ndisk_drops >= tc_max_disk_drops)
	{
		int			newmax = Max(16, tc_max_disk_drops * 2);

		if (tc_disk_drops == NULL)
			tc_disk_drops = MemoryContextAlloc(TopMemoryContext,
											   sizeof(TempcatDiskDrop) * newmax);
		else
			tc_disk_drops = repalloc(tc_disk_drops,
									 sizeof(TempcatDiskDrop) * newmax);
		tc_max_disk_drops = newmax;
	}
	tc_disk_drops[tc_ndisk_drops].catalog = catalog;
	tc_disk_drops[tc_ndisk_drops].oid = oid;
	tc_disk_drops[tc_ndisk_drops].level = GetCurrentTransactionNestLevel();
	tc_ndisk_drops++;
}

/*
 * Readers: the slices that the writer held when we used a table or function
 * with an OID of them, see tempcat_stats_object_gone()
 */
static Bitmapset *tc_reader_slices_seen = NULL;

/*
 * A reader process uses a table or function with a reserved OID, and may
 * send statistics of it later (pgstat_initstats(),
 * pgstat_init_function_usage()).  If our writer holds the slice of the OID,
 * it is one of the writer's temporary objects kept in memory, whose counts
 * are not to come back once it is dropped.
 */
void
tempcat_stats_note_use(Oid oid)
{
	TempcatProcState *st;
	int			slice;
	MemoryContext oldcxt;

	if (TempcatShared == NULL || !tempcat_is_reader() || proc_exit_inprogress)
		return;
	slice = tempcat_slice_of(oid);
	if (bms_is_member(slice, tc_reader_slices_seen))
		return;
	st = tempcat_writer_state();
	if (st == NULL ||
		pg_atomic_read_u32(&TempcatShared->slices[slice].holder) !=
		(uint32) (st - TempcatShared->procs) + 1)
		return;
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	tc_reader_slices_seen = bms_add_member(tc_reader_slices_seen, slice);
	MemoryContextSwitchTo(oldcxt);
}

/* See tempcat_stats_object_gone(). */
static bool
tempcat_stats_object_gone_guts(int catidx, Oid oid)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	uint32		hash = DatumGetUInt32(hash_uint32(oid));
	bool		gone = true;
	int			k;

	LWLockAcquire(&tc_root->lock, LW_SHARED);
	for (k = 0; k < TEMPCAT_NKEYS && gone; k++)
	{
		TempcatHash *h = &tc_root->catalogs[catidx].keys[k];
		dsa_pointer np;

		/* the oid column is always attribute 1 */
		if (def->keyattr[k] != 1 || h->nbuckets == 0)
			continue;
		np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
		while (DsaPointerIsValid(np) && gone)
		{
			TempcatHashNode *node = TC_ADDR(np);
			TempcatEntry *e = TC_ADDR(node->entry);

			if (node->hash == hash && tempcat_entry_oid(e) == oid)
			{
#ifdef FAULT_INJECTOR
				/* Test hook: fail with the lock held (with 'error'). */
				SIMPLE_FAULT_INJECTOR("tempcat_stats_object_gone");
#endif
				if (!(TransactionIdIsValid(e->xmax) &&
					  tempcat_xid_status(e->xmax) == TCX_COMMITTED))
					gone = false;
			}
			np = node->next;
		}
	}
	LWLockRelease(&tc_root->lock);
	return gone;
}

/*
 * Is 'oid' that of a table or function (by 'catalog') of the writer's
 * temporary objects kept in memory, which a committed transaction dropped?
 * For a reader process sending statistics (pgstat_report_stat()): the
 * writer purged those of what it dropped (tempcat_forget_if_gone()), and a
 * reader's counts still pending, sent later, would bring them back.  It is
 * one of the writer's if the writer held its slice when we used it
 * (tempcat_stats_note_use()).  Out of a transaction: the rows in memory
 * tell, if the writer holds the slice still, with an OID that no live row
 * has; if it let go of the slice, or is gone, none of its objects has an OID
 * of it any more.  At exit, when the area may be detached already, our
 * counts of the writer's objects are dropped, also of live ones: temporary
 * tables need none for autovacuum.  (The counts of an ordinary object with
 * an OID of such a slice, which no row in memory has, are lost too.)
 *
 * Out of a transaction, an error would not release the locks (ours, and
 * the clog's): as in tempcat_idle(), log it instead, and keep the counts.
 */
bool
tempcat_stats_object_gone(Oid catalog, Oid oid)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	TempcatProcState *st;
	int			slice;
	int			catidx;
	volatile bool gone = false;

	if (!IsTempcatOid(oid) || TempcatShared == NULL || !tempcat_is_reader())
		return false;
	slice = tempcat_slice_of(oid);
	if (!bms_is_member(slice, tc_reader_slices_seen))
		return false;
	/* (a retrieve connection detaches from the area of a writer that is gone) */
	if (proc_exit_inprogress || tc_area == NULL)
		return true;
	st = tempcat_writer_state();
	if (st == NULL ||
		pg_atomic_read_u32(&TempcatShared->slices[slice].holder) !=
		(uint32) (st - TempcatShared->procs) + 1)
		return true;
	catidx = tempcat_catalog_index(catalog);
	if (catidx < 0)
		return false;

	PG_TRY();
	{
		gone = tempcat_stats_object_gone_guts(catidx, oid);
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		LWLockReleaseAll();
		pgstat_report_wait_end();
		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		ereport(LOG,
				(errmsg("could not check whether a temporary object kept in memory was dropped: %s",
						edata->message)));
		FreeErrorData(edata);
		gone = false;
	}
	PG_END_TRY();
	return gone;
}

/*
 * May 'oid' be that of a temporary object that a live session of database
 * 'dbid' keeps in memory?  For pgstat_vacuum_stat(), which cannot see them.
 */
bool
tempcat_oid_maybe_live(Oid oid, Oid dbid)
{
	TempcatSliceState *ss;

	if (TempcatShared == NULL || !IsTempcatOid(oid))
		return false;
	ss = &TempcatShared->slices[tempcat_slice_of(oid)];
	if (pg_atomic_read_u32(&ss->holder) == 0)
		return false;
	pg_read_barrier();
	return ss->databaseId == dbid;
}

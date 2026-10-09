/*-------------------------------------------------------------------------
 *
 * tempcat.c
 *	  In-memory catalog rows for temporary objects.
 *
 * Every temporary table leaves dead tuples in pg_class, pg_attribute,
 * pg_type, pg_depend and friends, on the coordinator and on every segment.
 * Applications that create and drop many temporary tables therefore bloat
 * the shared catalogs and keep autovacuum busy.  This module keeps the
 * catalog rows of temporary objects out of the on-disk catalogs.
 *
 * Which rows are virtual is decided by the data, not by the code path that
 * writes them:
 *
 * - When gp_enable_temp_memory_catalog is on at the moment the session's
 *   temporary namespace is created, the namespace gets an OID from the
 *   reserved range FirstTempcatObjectId..LastTempcatObjectId.  Relations and
 *   types created in such a namespace, and the attribute defaults,
 *   constraints, rules and triggers of such relations, get OIDs from the
 *   same range (see tempcat_want_temp_oid(), called from oid_dispatch.c).
 *   The coordinator assigns the OIDs and dispatches them to the segments as
 *   usual, so all nodes agree on them.  The range is split into one slice
 *   per PGPROC, so OIDs of concurrent sessions never collide and lock tags
 *   stay unique.
 *
 * - A row inserted into one of the catalogs listed in tempcat_catalogs[] is
 *   virtual if its owner column (pg_attribute.attrelid, pg_index.indrelid,
 *   ...) holds an OID that this session assigned to a temporary object.
 *   Ownership is tracked per (catalog, OID) in a backend-local set, filled
 *   when the OID is allocated (QD) or received from the QD (QE), so ordinary
 *   objects that happen to have OIDs in the reserved range (created before
 *   the range was reserved, or carried over by pg_upgrade) are never
 *   affected.  Rows that also reference another object (pg_depend,
 *   pg_inherits) are virtual only if that object is the session's
 *   temporary object too, so dependencies of temporary objects on ordinary
 *   objects stay on disk and keep protecting those objects from DROP in
 *   other sessions.
 *
 * Virtual rows are stored in a dynamic shared memory area owned by the
 * session's writer process (the QD backend, or the writer QE on a segment).
 * Its handle is published in the writer's SharedSnapshotSlot, so reader
 * gangs and entry-db readers of the same session attach to it and see the
 * same rows, including uncommitted ones, without any serialization.
 *
 * Every row carries xmin/xmax/cmin/cmax and is checked with the usual MVCC
 * rules, so subtransactions, aborts and two-phase commit behave like on-disk
 * rows: an aborted insert simply never becomes visible.  Rows whose fate is
 * known are cleaned up by the writer at the start of the next transaction.
 * Frozen inserts and in-place updates (gp_fastsequence, relpages, ...) stay
 * non-transactional, like on disk.
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

#include <sys/statvfs.h>

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/valid.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/gp_distribution_policy.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/gp_partition_template.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_statistic_ext_data.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_range.h"
#include "catalog/pg_proc_callback.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_database.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_index.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/tempcat.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbvars.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/dsm_impl.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/dsa.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/rel.h"
#include "utils/faultinjector.h"
#include "utils/sharedsnapshot.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/* GUCs */
bool		gp_enable_temp_memory_catalog = false;
int			gp_temp_memory_catalog_max_size = 16384;	/* kB, per writer process */
bool		gp_temp_memory_catalog_disk_only = false;	/* hide virtual rows from SQL */
bool		gp_temp_memory_catalog_hide_others = false; /* see tempcat_hide_disk_row() */

/*
 * Catalogs that can hold virtual rows.
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
	 * A row is virtual if its owner column holds an OID that this session
	 * assigned to a temporary object of catalog 'ownerclass' (or of the
	 * catalog named by column 'ownerclassattr'), and, if 'owner2' is set,
	 * the same holds for the second owner column.
	 */
	AttrNumber	owner;
	Oid			ownerclass;
	AttrNumber	ownerclassattr;
	AttrNumber	owner2;
	Oid			owner2class;
	AttrNumber	owner2classattr;

	AttrNumber	keyattr[TEMPCAT_NKEYS]; /* hashed lookup columns */
	TempcatKeyType keytype[TEMPCAT_NKEYS];
} TempcatCatalogDef;

#define REL RelationRelationId

static const TempcatCatalogDef tempcat_catalogs[] =
{
	{RelationRelationId, Anum_pg_class_oid, REL, 0, 0, 0, 0,
	{Anum_pg_class_oid, Anum_pg_class_relname}, {TCK_OID, TCK_NAME}},
	{AttributeRelationId, Anum_pg_attribute_attrelid, REL, 0, 0, 0, 0,
	{Anum_pg_attribute_attrelid, 0}, {TCK_OID, TCK_NONE}},
	{TypeRelationId, Anum_pg_type_oid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_type_oid, Anum_pg_type_typname}, {TCK_OID, TCK_NAME}},
	{NamespaceRelationId, Anum_pg_namespace_oid, NamespaceRelationId, 0, 0, 0, 0,
	{Anum_pg_namespace_oid, Anum_pg_namespace_nspname}, {TCK_OID, TCK_NAME}},
	{IndexRelationId, Anum_pg_index_indrelid, REL, 0, 0, 0, 0,
	{Anum_pg_index_indrelid, Anum_pg_index_indexrelid}, {TCK_OID, TCK_OID}},
	{AttrDefaultRelationId, Anum_pg_attrdef_adrelid, REL, 0, 0, 0, 0,
	{Anum_pg_attrdef_adrelid, Anum_pg_attrdef_oid}, {TCK_OID, TCK_OID}},
	/* conrelid, or contypid for domain constraints; see tempcat_route_insert() */
	{ConstraintRelationId, Anum_pg_constraint_conrelid, REL, 0, 0, 0, 0,
	{Anum_pg_constraint_conrelid, Anum_pg_constraint_oid}, {TCK_OID, TCK_OID}},
	{RewriteRelationId, Anum_pg_rewrite_ev_class, REL, 0, 0, 0, 0,
	{Anum_pg_rewrite_ev_class, Anum_pg_rewrite_oid}, {TCK_OID, TCK_OID}},
	{TriggerRelationId, Anum_pg_trigger_tgrelid, REL, 0, 0, 0, 0,
	{Anum_pg_trigger_tgrelid, Anum_pg_trigger_oid}, {TCK_OID, TCK_OID}},
	{StatisticRelationId, Anum_pg_statistic_starelid, REL, 0, 0, 0, 0,
	{Anum_pg_statistic_starelid, 0}, {TCK_OID, TCK_NONE}},
	{DescriptionRelationId, Anum_pg_description_objoid, InvalidOid,
		Anum_pg_description_classoid, 0, 0, 0,
	{Anum_pg_description_objoid, 0}, {TCK_OID, TCK_NONE}},
	{DependRelationId, Anum_pg_depend_objid, InvalidOid, Anum_pg_depend_classid,
		Anum_pg_depend_refobjid, InvalidOid, Anum_pg_depend_refclassid,
	{Anum_pg_depend_objid, Anum_pg_depend_refobjid}, {TCK_OID, TCK_OID}},
	{InheritsRelationId, Anum_pg_inherits_inhrelid, REL, 0,
		Anum_pg_inherits_inhparent, REL, 0,
	{Anum_pg_inherits_inhrelid, Anum_pg_inherits_inhparent}, {TCK_OID, TCK_OID}},
	{PartitionedRelationId, Anum_pg_partitioned_table_partrelid, REL, 0, 0, 0, 0,
	{Anum_pg_partitioned_table_partrelid, 0}, {TCK_OID, TCK_NONE}},
	{SequenceRelationId, Anum_pg_sequence_seqrelid, REL, 0, 0, 0, 0,
	{Anum_pg_sequence_seqrelid, 0}, {TCK_OID, TCK_NONE}},
	{GpPolicyRelationId, Anum_gp_distribution_policy_localoid, REL, 0, 0, 0, 0,
	{Anum_gp_distribution_policy_localoid, 0}, {TCK_OID, TCK_NONE}},
	{AppendOnlyRelationId, Anum_pg_appendonly_relid, REL, 0, 0, 0, 0,
	{Anum_pg_appendonly_relid, 0}, {TCK_OID, TCK_NONE}},
	{FastSequenceRelationId, Anum_gp_fastsequence_objid, REL, 0, 0, 0, 0,
	{Anum_gp_fastsequence_objid, 0}, {TCK_OID, TCK_NONE}},
	{AttributeEncodingRelationId, Anum_pg_attribute_encoding_attrelid, REL, 0, 0, 0, 0,
	{Anum_pg_attribute_encoding_attrelid, 0}, {TCK_OID, TCK_NONE}},
	{PartitionTemplateRelationId, Anum_gp_partition_template_relid, REL, 0, 0, 0, 0,
	{Anum_gp_partition_template_relid, 0}, {TCK_OID, TCK_NONE}},
	{PolicyRelationId, Anum_pg_policy_polrelid, REL, 0, 0, 0, 0,
	{Anum_pg_policy_polrelid, Anum_pg_policy_oid}, {TCK_OID, TCK_OID}},
	/* objects in a temporary namespace kept in memory, and their parts */
	{ProcedureRelationId, Anum_pg_proc_pronamespace, NamespaceRelationId, 0, 0, 0, 0,
	{Anum_pg_proc_oid, Anum_pg_proc_proname}, {TCK_OID, TCK_NAME}},
	{AggregateRelationId, Anum_pg_aggregate_aggfnoid, ProcedureRelationId, 0, 0, 0, 0,
	{Anum_pg_aggregate_aggfnoid, 0}, {TCK_OID, TCK_NONE}},
	{ProcCallbackRelationId, Anum_pg_proc_callback_profnoid, ProcedureRelationId, 0, 0, 0, 0,
	{Anum_pg_proc_callback_profnoid, 0}, {TCK_OID, TCK_NONE}},
	{OperatorRelationId, Anum_pg_operator_oprnamespace, NamespaceRelationId, 0, 0, 0, 0,
	{Anum_pg_operator_oid, Anum_pg_operator_oprname}, {TCK_OID, TCK_NAME}},
	{EnumRelationId, Anum_pg_enum_enumtypid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_enum_enumtypid, Anum_pg_enum_oid}, {TCK_OID, TCK_OID}},
	{RangeRelationId, Anum_pg_range_rngtypid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_range_rngtypid, 0}, {TCK_OID, TCK_NONE}},
	{StatisticExtRelationId, Anum_pg_statistic_ext_stxrelid, REL, 0, 0, 0, 0,
	{Anum_pg_statistic_ext_stxrelid, Anum_pg_statistic_ext_oid}, {TCK_OID, TCK_OID}},
	{StatisticExtDataRelationId, Anum_pg_statistic_ext_data_stxoid, StatisticExtRelationId, 0, 0, 0, 0,
	{Anum_pg_statistic_ext_data_stxoid, 0}, {TCK_OID, TCK_NONE}},
	/* owner and ACL rows only; see tempcat_route_insert() */
	{SharedDependRelationId, Anum_pg_shdepend_objid, InvalidOid,
		Anum_pg_shdepend_classid, 0, 0, 0,
	{Anum_pg_shdepend_objid, Anum_pg_shdepend_refobjid}, {TCK_OID, TCK_OID}},
};

#undef REL

#define TEMPCAT_NCATALOGS lengthof(tempcat_catalogs)

/*
 * Shared structures.  Everything below lives in the session's DSA area and
 * refers to other objects by dsa_pointer.  The writer modifies them under
 * the exclusive lock; readers take the lock in shared mode.
 */
typedef struct TempcatHash
{
	uint32		nbuckets;		/* power of two, or 0 if not allocated */
	uint32		nentries;
	dsa_pointer buckets;		/* dsa_pointer[nbuckets] */
} TempcatHash;

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

typedef struct TempcatRoot
{
	LWLock		lock;
	uint64		next_tid;
	TempcatHash tids;
	TempcatCatalog catalogs[TEMPCAT_NCATALOGS];
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

/*
 * Process-local state.
 */
static dsa_area *tc_area = NULL;
static TempcatRoot *tc_root = NULL;

/*
 * Writer only: entries inserted or deleted by transactions whose outcome
 * has not been applied yet.
 */
static dsa_pointer *tc_pending = NULL;
static int	tc_npending = 0;
static int	tc_maxpending = 0;

struct TempcatScanData
{
	HeapTuple  *tuples;
	int			ntuples;
	int			next;
	bool		on_virtual;		/* last tuple returned was virtual */

	/*
	 * For scans through an index: virtual rows are sorted by the index key
	 * and merged with the on-disk rows, so that callers relying on index
	 * order (e.g. catcache lists of pg_attribute by attnum) see it.
	 */
	Relation	irel;
	TupleDesc	heapdesc;
	int			nidxkeys;
	FmgrInfo  **procs;
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

static int
tempcat_catalog_index(Oid relid)
{
	int			i;

	for (i = 0; i < TEMPCAT_NCATALOGS; i++)
	{
		if (tempcat_catalogs[i].relid == relid)
			return i;
	}
	return -1;
}

/*
 * Space for dynamic shared memory.
 *
 * The session's area grows by creating DSM segments.  With the default
 * dynamic_shared_memory_type = posix they live in /dev/shm, which can be
 * small (64 MB in a default Docker container).  Allocations are made with
 * DSA_ALLOC_NO_OOM, so failing to create a segment for lack of space just
 * makes the row go to the on-disk catalog (see DSM_CREATE_NULL_IF_NOSPACE).
 * Other users of DSM (e.g. combo CIDs shared with reader gangs) are not that
 * tolerant, so the area does not grow into the last quarter of the space
 * (dsa_set_min_free_space()).  The area's first segment, created by
 * dsa_create(), is checked before creating the area.
 */
#define TEMPCAT_DSA_FIRST_SEGMENT	(1024 * 1024)
#define TEMPCAT_DSM_CHECK_ATTEMPTS	100

static bool tc_dsm_low = false;		/* could not create the area */
static int	tc_attempts_since_check = 0;

/* Leave this fraction of the DSM space to its other users. */
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

static inline size_t
tempcat_max_area_bytes(void)
{
	return (size_t) gp_temp_memory_catalog_max_size * 1024;
}

/*
 * Attach to the session's area, creating it if 'create' and we are the
 * writer.  Returns false if there is no area (yet), or if it could not be
 * created because DSM space is short.
 */
static bool
tempcat_attach(bool create)
{
	MemoryContext oldcxt;
	dsa_pointer rootptr;

	if (tc_area != NULL)
		return true;

	LWLockRegisterTranche(tempcat_tranche_id(), "tempcat");

	if (am_cursor_retrieve_handler)
	{
		/*
		 * A retrieve connection outputs the rows of another session's
		 * parallel retrieve cursor and must see that session's temporary
		 * types.  It runs its own transactions, so it sees their committed
		 * catalog rows only, like on disk.
		 */
		dsa_handle	handle;
		int			sessionId = RetrieveSessionId();

		/* (free slots have session ID -1, like an unauthenticated session) */
		if (sessionId < 0 ||
			!SharedSnapshotGetTempcatArea(sessionId, &handle, &rootptr))
			return false;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		tc_area = dsa_attach(handle);
		dsa_pin_mapping(tc_area);
		MemoryContextSwitchTo(oldcxt);

		tc_root = dsa_get_address(tc_area, rootptr);
		return true;
	}

	if (tempcat_is_reader())
	{
		dsa_handle	handle;

		if (SharedLocalSnapshotSlot == NULL)
			return false;
		handle = SharedLocalSnapshotSlot->tempcat_handle;
		if (handle == DSM_HANDLE_INVALID)
			return false;
		pg_read_barrier();
		rootptr = SharedLocalSnapshotSlot->tempcat_root;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		tc_area = dsa_attach(handle);
		dsa_pin_mapping(tc_area);
		MemoryContextSwitchTo(oldcxt);

		tc_root = dsa_get_address(tc_area, rootptr);
		return true;
	}

	if (!create)
		return false;

	/* Its first segment must fit; recheck only every so often. */
	if (tc_dsm_low && ++tc_attempts_since_check < TEMPCAT_DSM_CHECK_ATTEMPTS)
		return false;
	tc_attempts_since_check = 0;
	tc_dsm_low = tempcat_dsm_space_low(2 * TEMPCAT_DSA_FIRST_SEGMENT);
	if (tc_dsm_low)
		return false;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	tc_area = dsa_create(tempcat_tranche_id());
	dsa_pin_mapping(tc_area);
	MemoryContextSwitchTo(oldcxt);

	rootptr = dsa_allocate0(tc_area, sizeof(TempcatRoot));
	tc_root = dsa_get_address(tc_area, rootptr);
	LWLockInitialize(&tc_root->lock, tempcat_tranche_id());

	/* Rows that do not fit go to the on-disk catalog instead. */
	dsa_set_size_limit(tc_area, tempcat_max_area_bytes());
	{
		Size		free_bytes;
		Size		total_bytes;

		if (dsm_impl_free_space(&free_bytes, &total_bytes))
			dsa_set_min_free_space(tc_area, total_bytes / TEMPCAT_DSM_HEADROOM_DIV);
	}

	/* Publish the area to the readers of this session. */
	if (SharedLocalSnapshotSlot != NULL)
	{
		SharedLocalSnapshotSlot->tempcat_root = rootptr;
		pg_write_barrier();
		SharedLocalSnapshotSlot->tempcat_handle = dsa_get_handle(tc_area);
	}

	return true;
}

/* ----------------------------------------------------------------
 * OID assignment
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 * Shared memory
 *
 * Per PGPROC: the next OID of its slice of the reserved range, and the
 * oldest relfrozenxid/relminmxid of the in-memory temporary tables of the
 * session using it.  The latter stand in for the pg_class rows that
 * vac_update_datfrozenxid() cannot see.  Also the databases already swept
 * for leftovers of crashed sessions since the node started.
 * ----------------------------------------------------------------
 */
#define TEMPCAT_MAX_SWEPT_DBS 128
#define TEMPCAT_MAX_ROLES	8

typedef struct TempcatProcState
{
	Oid			nextOid;
	Oid			databaseId;		/* database of the published horizon */
	pg_atomic_uint32 frozenXid; /* or InvalidTransactionId */
	pg_atomic_uint32 minMulti;	/* or InvalidMultiXactId */

	/*
	 * Roles that in-memory pg_shdepend rows of the session refer to (owners
	 * and grantees of its temporary objects), standing in for the rows
	 * DROP ROLE cannot see.  Changed under TempcatShared->mutex.
	 */
	int			nroles;
	Oid			roles[TEMPCAT_MAX_ROLES];

	/*
	 * Bounds of the reserved OIDs the session assigned to (QD) or received
	 * for (QE) its temporary objects, or InvalidOid; for
	 * tempcat_hide_disk_row() in other sessions.  Written by the session
	 * only.
	 */
	pg_atomic_uint32 ownedMin;
	pg_atomic_uint32 ownedMax;
} TempcatProcState;

typedef struct TempcatSharedData
{
	int			tranche_id;		/* LWLock tranche of the session areas */
	slock_t		mutex;			/* protects the swept list */
	int			nswept;
	Oid			swept[TEMPCAT_MAX_SWEPT_DBS];
	int			nprocs;
	TempcatProcState procs[FLEXIBLE_ARRAY_MEMBER];
} TempcatSharedData;

static TempcatSharedData *TempcatShared = NULL;

/* Lock tag sub-ID serializing the leftover sweep of a database */
#define TEMPCAT_SWEEP_LOCK_SUBID	0x7EC

static inline int
tempcat_total_procs(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;
}

Size
TempcatShmemSize(void)
{
	return add_size(offsetof(TempcatSharedData, procs),
					mul_size(tempcat_total_procs(), sizeof(TempcatProcState)));
}

void
TempcatShmemInit(void)
{
	bool		found;
	int			i;

	TempcatShared = ShmemInitStruct("Temporary catalog data",
									TempcatShmemSize(), &found);
	if (found)
		return;

	/* A dynamic tranche, so that the builtin tranche IDs stay unchanged. */
	TempcatShared->tranche_id = LWLockNewTrancheId();
	SpinLockInit(&TempcatShared->mutex);
	TempcatShared->nswept = 0;
	TempcatShared->nprocs = tempcat_total_procs();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		st->nextOid = InvalidOid;
		st->databaseId = InvalidOid;
		st->nroles = 0;
		pg_atomic_init_u32(&st->frozenXid, InvalidTransactionId);
		pg_atomic_init_u32(&st->minMulti, InvalidMultiXactId);
		pg_atomic_init_u32(&st->ownedMin, InvalidOid);
		pg_atomic_init_u32(&st->ownedMax, InvalidOid);
	}
}

/* LWLock tranche of the session areas */
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

/* Has this session started to keep temporary objects in memory? */
static bool tc_activated = false;

/* The published frozen horizon may be too old and needs recomputing. */
static bool tc_horizon_dirty = false;

/* The published roles may include roles no longer used. */
static bool tc_roles_dirty = false;

static void tempcat_sweep_if_needed(bool allow_force);

/*
 * The (sub)transaction whose sweep marked tc_sweep_db as swept before
 * committing; checked at the start of later transactions.
 */
static TransactionId tc_sweep_xid = InvalidTransactionId;
static Oid	tc_sweep_db = InvalidOid;
static void tempcat_unmark_swept(Oid dbid);
static bool tempcat_area_has_live_rows(void);

/*
 * Runs after the temporary objects were dropped at session exit
 * (RemoveTempRelationsCallback(), registered later, runs earlier), while
 * the area is still attached.  If that cleanup did not finish (it failed,
 * or a prepared transaction kept it from seeing the objects), the on-disk
 * rows of the remaining objects are leftovers like those of a crashed
 * session, but no node restart will trigger their sweep: ask for one.
 */
static void
tempcat_before_exit(int code, Datum arg)
{
	TempcatProcState *st = tempcat_my_state();
	bool		leftovers;

	/* Without an area, rows went to disk only; assume the worst. */
	leftovers = tc_area == NULL || tempcat_area_has_live_rows();

	/* Stop protecting our rows from the sweep before asking for it. */
	pg_atomic_write_u32(&st->ownedMin, InvalidOid);
	pg_atomic_write_u32(&st->ownedMax, InvalidOid);

	if (leftovers)
		tempcat_unmark_swept(MyDatabaseId);
}

static void
tempcat_proc_exit(int code, Datum arg)
{
	TempcatProcState *st = tempcat_my_state();

	pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
	pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
	pg_atomic_write_u32(&st->ownedMin, InvalidOid);
	pg_atomic_write_u32(&st->ownedMax, InvalidOid);
	st->databaseId = InvalidOid;

	SpinLockAcquire(&TempcatShared->mutex);
	st->nroles = 0;
	SpinLockRelease(&TempcatShared->mutex);
}

/*
 * Publish that an in-memory pg_shdepend row of ours refers to a role.
 * Returns false if the list is full; the row then goes to disk.
 */
static bool
tempcat_publish_role(Oid roleid)
{
	TempcatProcState *st = tempcat_my_state();
	bool		ok = true;
	int			i;

	/* Only we change our own list, so it can be read without the lock. */
	for (i = 0; i < st->nroles; i++)
	{
		if (st->roles[i] == roleid)
			return true;
	}

	SpinLockAcquire(&TempcatShared->mutex);
	if (st->nroles < TEMPCAT_MAX_ROLES)
		st->roles[st->nroles++] = roleid;
	else
		ok = false;
	SpinLockRelease(&TempcatShared->mutex);

	return ok;
}

/*
 * Do in-memory temporary objects of another session refer to the role?
 * Called by DROP ROLE (checkSharedDependencies()), which holds a lock on the
 * role that keeps sessions from adding such references meanwhile: they lock
 * it too before recording a dependency on it.
 */
bool
tempcat_role_used_elsewhere(Oid roleid)
{
	bool		found = false;
	int			i;
	int			j;

	if (TempcatShared == NULL)
		return false;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nprocs && !found; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		if (MyProc != NULL && i == MyProc->pgprocno)
			continue;
		for (j = 0; j < st->nroles; j++)
		{
			if (st->roles[j] == roleid)
			{
				found = true;
				break;
			}
		}
	}
	SpinLockRelease(&TempcatShared->mutex);

	return found;
}

/*
 * Called when the session gets its first reserved OID, i.e. when its
 * temporary namespace is created in memory (on the QD and on each QE
 * writer).
 */
static void
tempcat_activate(void)
{
	TempcatProcState *st;

	if (tc_activated)
		return;
	tc_activated = true;

	st = tempcat_my_state();
	pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
	pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
	st->databaseId = MyDatabaseId;
	on_shmem_exit(tempcat_proc_exit, (Datum) 0);
	before_shmem_exit(tempcat_before_exit, (Datum) 0);

	/*
	 * From now on catalog queries must not use index-only or bitmap scans of
	 * catalog indexes (see tempcat_restrict_catalog_index_scans()); forget
	 * plans made before.
	 */
	ResetPlanCache();

	tempcat_sweep_if_needed(true);
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
	TransactionId xid = form->relfrozenxid;
	MultiXactId mxid = form->relminmxid;

	if (TransactionIdIsNormal(xid))
	{
		TransactionId cur = pg_atomic_read_u32(&st->frozenXid);

		if (!TransactionIdIsValid(cur) || TransactionIdPrecedes(xid, cur))
			pg_atomic_write_u32(&st->frozenXid, xid);
	}
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

/*
 * OIDs assigned by this session to its temporary objects, per catalog.
 */
typedef struct TempcatOwnedKey
{
	Oid			catalog;
	Oid			oid;
} TempcatOwnedKey;

static HTAB *tc_owned = NULL;

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

	/* Widen the published bounds; nobody else writes them. */
	if (IsTempcatOid(oid))
	{
		TempcatProcState *st = tempcat_my_state();
		Oid			lo = pg_atomic_read_u32(&st->ownedMin);
		Oid			hi = pg_atomic_read_u32(&st->ownedMax);

		if (!OidIsValid(lo) || oid < lo)
			pg_atomic_write_u32(&st->ownedMin, oid);
		if (!OidIsValid(hi) || oid > hi)
			pg_atomic_write_u32(&st->ownedMax, oid);
	}
}

static void
tempcat_forget_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL)
		return;
	key.catalog = catalog;
	key.oid = oid;
	(void) hash_search(tc_owned, &key, HASH_REMOVE, NULL);
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
 * Should a new object get an OID from the reserved range?  Called in the QD
 * by oid_dispatch.c with the same key it dispatches to the segments.
 */
bool
tempcat_want_temp_oid(Oid catalog, const char *objname, Oid namespaceOid,
					  Oid keyOid1, Oid keyOid2)
{
	return tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1, keyOid2,
							   gp_enable_temp_memory_catalog);
}

/*
 * A QE received a pre-assigned OID from the QD.  If the QD gave a temporary
 * object of this session a reserved OID, remember that we own it.
 */
void
tempcat_note_preassigned_oid(Oid catalog, const char *objname,
							 Oid namespaceOid, Oid keyOid1, Oid keyOid2,
							 Oid oid)
{
	if (IsBinaryUpgrade || !IsTempcatOid(oid))
		return;

	if (tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1, keyOid2,
							true))
	{
		tempcat_activate();
		tempcat_remember_oid(catalog, oid);
	}
}

/* The object is known to be a temporary object of this session. */
void
tempcat_note_temp_oid(Oid catalog, Oid oid)
{
	tempcat_activate();
	tempcat_remember_oid(catalog, oid);
}

/*
 * Only relations, types, functions and operators can be created in a
 * temporary namespace kept in memory: other objects (operator classes,
 * collations, text search objects, ...) would get on-disk catalog rows
 * pointing at the namespace that other sessions cannot see.  Called by
 * oid_dispatch.c in the QD.
 */
void
tempcat_check_namespace_object(Oid catalog, Oid namespaceOid)
{
	if (catalog == RelationRelationId || catalog == TypeRelationId ||
		catalog == ProcedureRelationId || catalog == OperatorRelationId)
		return;
	if (tempcat_owns_oid(NamespaceRelationId, namespaceOid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only relations, types, functions and operators can be created in a temporary schema kept in memory"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Some objects can be created for temporary tables or types, but are not
 * supported in memory (e.g. enum labels, read by ordered scans).
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

/*
 * Allocate an OID for a new temporary object in 'relation' from this
 * backend's slice of the reserved range.
 *
 * Like GetNewOidWithIndex(), make sure the OID is not used in the catalog
 * yet, neither on disk (ordinary objects may have OIDs in the range, see the
 * file header) nor in memory: the SnapshotAny scan sees both.
 *
 * The next value is kept in shared memory per PGPROC, so consecutive
 * sessions that reuse the same PGPROC keep moving forward instead of reusing
 * recent OIDs.
 */
Oid
tempcat_allocate_oid(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	TempcatProcState *st;
	uint64		span;
	Oid			base;
	int			tries;

	tempcat_activate();

	st = tempcat_my_state();
	span = ((uint64) LastTempcatObjectId - FirstTempcatObjectId + 1) /
		TempcatShared->nprocs;
	base = FirstTempcatObjectId + (Oid) (span * MyProc->pgprocno);

	for (tries = 0; tries < (int) Min(span, 1000000); tries++)
	{
		Oid			oid = st->nextOid;
		ScanKeyData key;
		SysScanDesc scan;
		bool		collides;

		if (oid < base || oid >= base + span)
			oid = base;
		st->nextOid = oid + 1;

		ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(oid));
		scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
		collides = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);

		if (!collides)
		{
			tempcat_remember_oid(RelationGetRelid(relation), oid);
			return oid;
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("out of OIDs for temporary objects in memory catalog")));
	return InvalidOid;			/* keep compiler quiet */
}

/* The first attribute of these catalogs is their oid column. */
static inline Oid
tempcat_entry_oid(TempcatEntry *e)
{
	HeapTupleHeader hdr = TCE_DATA(e);

	return *(Oid *) ((char *) hdr + hdr->t_hoff);
}

/* Is the entry's catalog one whose own oid column is attribute 1? */
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
 * Shared hash tables
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

	newb = dsa_allocate_extended(tc_area, sizeof(dsa_pointer) * newn,
								 DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(newb))
		return;

	oldarr = TC_ADDR(h->buckets);
	newarr = TC_ADDR(newb);
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

	dsa_free(tc_area, h->buckets);
	h->buckets = newb;
	h->nbuckets = newn;
}

/* Make sure the table has buckets.  Returns false if out of memory. */
static bool
tempcat_hash_prepare(TempcatHash *h)
{
	if (h->nbuckets == 0)
	{
		h->buckets = dsa_allocate_extended(tc_area, sizeof(dsa_pointer) * 64,
										   DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
		if (!DsaPointerIsValid(h->buckets))
			return false;
		h->nbuckets = 64;
	}
	else if (h->nentries >= h->nbuckets * 2)
		tempcat_hash_grow(h);
	return true;
}

/* Link a preallocated node.  The table must have been prepared. */
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
			dsa_pointer np = *link;

			*link = node->next;
			dsa_free(tc_area, np);
			h->nentries--;
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
	dsa_pointer nodes[TEMPCAT_NKEYS + 1];
	TempcatEntry *e;
	HeapTupleHeader hdr;
	int			nnodes = 0;
	int			k;

	/* Allocate everything up front, so that running out changes nothing. */
	ep = dsa_allocate_extended(tc_area,
							   MAXALIGN(sizeof(TempcatEntry)) + tup->t_len,
							   DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(ep))
		return InvalidDsaPointer;
	for (k = 0; k <= TEMPCAT_NKEYS; k++)
	{
		if (k < TEMPCAT_NKEYS && def->keytype[k] == TCK_NONE)
			continue;
		if ((k < TEMPCAT_NKEYS && !tempcat_hash_prepare(&cat->keys[k])) ||
			(k == TEMPCAT_NKEYS && !tempcat_hash_prepare(&tc_root->tids)))
			goto oom;
		nodes[nnodes] = dsa_allocate_extended(tc_area, sizeof(TempcatHashNode),
											  DSA_ALLOC_NO_OOM);
		if (!DsaPointerIsValid(nodes[nnodes]))
			goto oom;
		nnodes++;
	}

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

	nnodes = 0;
	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		if (def->keytype[k] != TCK_NONE)
			tempcat_hash_link(&cat->keys[k], nodes[nnodes++], e->keyhash[k], ep);
	}
	tempcat_hash_link(&tc_root->tids, nodes[nnodes], e->tidhash, ep);

	return ep;

oom:
	while (nnodes > 0)
		dsa_free(tc_area, nodes[--nnodes]);
	dsa_free(tc_area, ep);
	return InvalidDsaPointer;
}

/* Caller holds the exclusive lock. */
static void
tempcat_entry_remove(dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);
	const TempcatCatalogDef *def = &tempcat_catalogs[e->catidx];
	TempcatCatalog *cat = &tc_root->catalogs[e->catidx];
	int			k;

	if (def->relid == SharedDependRelationId)
		tc_roles_dirty = true;

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

	dsa_free(tc_area, ep);
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

	hdr->t_infomask &= ~HEAP_XACT_MASK;
	hdr->t_infomask |= HEAP_XMAX_INVALID;
	if (e->flags & TCE_XMIN_COMMITTED)
		hdr->t_infomask |= HEAP_XMIN_COMMITTED;
	HeapTupleHeaderSetXmin(hdr, e->xmin);
	HeapTupleHeaderSetXmax(hdr, InvalidTransactionId);
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

/* The current transaction changed virtual rows; see tempcat_check_prepare(). */
static bool tc_xact_changed = false;

static void
tempcat_add_pending(dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);

	tc_xact_changed = true;

	if (e->flags & TCE_PENDING)
		return;

	if (tc_npending >= tc_maxpending)
	{
		int			newmax = Max(64, tc_maxpending * 2);

		if (tc_pending == NULL)
			tc_pending = MemoryContextAlloc(TopMemoryContext,
											sizeof(dsa_pointer) * newmax);
		else
			tc_pending = repalloc(tc_pending, sizeof(dsa_pointer) * newmax);
		tc_maxpending = newmax;
	}
	tc_pending[tc_npending++] = ep;
	e->flags |= TCE_PENDING;
}

/* ----------------------------------------------------------------
 * Visibility
 * ----------------------------------------------------------------
 */

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
 * Apply the outcome of finished transactions to the pending entries: drop
 * aborted inserts and committed deletes, clear aborted deletes.  Entries of
 * a prepared transaction that has not been finished yet stay pending.
 *
 * Called by the writer at the start of each transaction, when none of the
 * pending XIDs can be the current one anymore.
 */
/*
 * After removing an entry, stop treating its object's OID as ours if no
 * other version of the object is left in memory.  Keeps the owned set from
 * growing in long sessions.
 */
static void
tempcat_forget_if_gone(int catidx, Oid objoid)
{
	if (tempcat_catalogs[catidx].relid == RelationRelationId)
		tc_horizon_dirty = true;

	if (OidIsValid(objoid) && !tempcat_memory_has_oid(catidx, objoid))
		tempcat_forget_oid(tempcat_catalogs[catidx].relid, objoid);
}

static void
tempcat_resolve_pending(void)
{
	int			i;
	int			keep = 0;

	if (tc_npending == 0)
		return;

	Assert(tc_area != NULL);

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);

	for (i = 0; i < tc_npending; i++)
	{
		dsa_pointer ep = tc_pending[i];
		TempcatEntry *e = TC_ADDR(ep);
		int			catidx = e->catidx;
		Oid			objoid = tempcat_catalog_has_oid(catidx) ? tempcat_entry_oid(e) : InvalidOid;
		bool		in_progress = false;

		if (!(e->flags & TCE_XMIN_COMMITTED))
		{
			switch (tempcat_xid_status(e->xmin))
			{
				case TCX_ABORTED:
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
			tc_pending[keep++] = ep;
		else
			e->flags &= ~TCE_PENDING;
	}
	tc_npending = keep;

	LWLockRelease(&tc_root->lock);
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

	tc_horizon_dirty = false;

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
 * Free rows that the current transaction inserted and then deleted in an
 * earlier command, before the transaction ends.  Called when the area is
 * full, so that a transaction creating and dropping many temporary tables
 * in a loop does not overflow to the on-disk catalog.  Caller holds the
 * exclusive lock.  Returns the number of rows freed.
 *
 * Such a row is dead whatever the transaction's fate, as long as its
 * deletion cannot be undone on its own: rows deleted by the top-level
 * transaction itself, or by the same (sub)transaction that inserted them
 * (any rollback undoing the deletion then undoes the insertion too, e.g. a
 * loop body with an exception block).  Not rows deleted by a subtransaction
 * that may roll back while their insertion stays.  New catalog snapshots do not see rows deleted in an earlier command
 * any more, and scans copy the rows they return when they start.  (A cursor
 * over a catalog opened before the deletion in the same transaction will
 * not see them, unlike on-disk rows.)
 */
static int
tempcat_reclaim_dead(void)
{
	TransactionId topxid = GetTopTransactionIdIfAny();
	CommandId	curcid = GetCurrentCommandId(false);
	int			keep = 0;
	int			nfreed = 0;
	int			i;

	if (!TransactionIdIsValid(topxid))
		return 0;

	for (i = 0; i < tc_npending; i++)
	{
		dsa_pointer ep = tc_pending[i];
		TempcatEntry *e = TC_ADDR(ep);

		if ((TransactionIdEquals(e->xmax, topxid) ||
			 TransactionIdEquals(e->xmax, e->xmin)) &&
			e->cmax < curcid &&
			!(e->flags & TCE_XMIN_COMMITTED) &&
			TransactionIdIsCurrentTransactionId(e->xmin))
		{
			int			catidx = e->catidx;
			Oid			objoid = tempcat_catalog_has_oid(catidx) ?
				tempcat_entry_oid(e) : InvalidOid;

			tempcat_entry_remove(ep);
			tempcat_forget_if_gone(catidx, objoid);
			nfreed++;
		}
		else
			tc_pending[keep++] = ep;
	}
	tc_npending = keep;

	return nfreed;
}

/*
 * Republish the roles that our in-memory pg_shdepend rows, of any
 * visibility, refer to, after some of the rows went away.
 */
static void
tempcat_recompute_roles(void)
{
	int			catidx = tempcat_catalog_index(SharedDependRelationId);
	TempcatProcState *st = tempcat_my_state();
	Oid			roles[TEMPCAT_MAX_ROLES];
	int			nroles = 0;
	dsa_pointer ep;
	int			i;

	tc_roles_dirty = false;

	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);
		Form_pg_shdepend form = (Form_pg_shdepend) ((char *) hdr + hdr->t_hoff);

		for (i = 0; i < nroles; i++)
		{
			if (roles[i] == form->refobjid)
				break;
		}
		/* every row's role was published, so they fit */
		if (i == nroles && nroles < TEMPCAT_MAX_ROLES)
			roles[nroles++] = form->refobjid;
		ep = e->next;
	}

	SpinLockAcquire(&TempcatShared->mutex);
	memcpy(st->roles, roles, sizeof(Oid) * nroles);
	st->nroles = nroles;
	SpinLockRelease(&TempcatShared->mutex);
}

/*
 * Does the area still hold rows that are, or may become, visible?  Used at
 * session exit, outside any transaction.
 */
static bool
tempcat_area_has_live_rows(void)
{
	int			i;

	for (i = 0; i < TEMPCAT_NCATALOGS; i++)
	{
		dsa_pointer ep = tc_root->catalogs[i].head;

		while (DsaPointerIsValid(ep))
		{
			TempcatEntry *e = TC_ADDR(ep);

			if (((e->flags & TCE_XMIN_COMMITTED) ||
				 tempcat_xid_status(e->xmin) != TCX_ABORTED) &&
				(!TransactionIdIsValid(e->xmax) ||
				 tempcat_xid_status(e->xmax) != TCX_COMMITTED))
				return true;
			ep = e->next;
		}
	}
	return false;
}

void
tempcat_start_transaction(void)
{
	tc_xact_changed = false;

	/* A sweep was rolled back: the leftovers are still there. */
	if (TransactionIdIsValid(tc_sweep_xid))
	{
		TempcatXidStatus status = tempcat_xid_status(tc_sweep_xid);

		if (status == TCX_ABORTED)
			tempcat_unmark_swept(tc_sweep_db);
		if (status != TCX_IN_PROGRESS)
			tc_sweep_xid = InvalidTransactionId;
	}

	tempcat_resolve_pending();
	if (tc_horizon_dirty && tc_area != NULL)
		tempcat_recompute_horizon();
	if (tc_roles_dirty && tc_area != NULL)
		tempcat_recompute_roles();
}

/*
 * PREPARE TRANSACTION by a user (possible in utility mode only) of a
 * transaction that changed virtual rows.  Upstream refuses to prepare
 * transactions that operated on temporary objects; Greengage cannot, as its
 * own two-phase commit prepares every distributed transaction on the
 * segments.  For in-memory rows it matters more: if the session ends before
 * the prepared transaction is finished, its changes to the session's rows
 * are lost, while its on-disk effects (relation files, dependencies on
 * ordinary objects) are committed later anyway.
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
 * After a subtransaction abort, free the rows it inserted and undo its
 * deletions right away, so that loops with exception blocks do not keep
 * dead rows around until the end of the top-level transaction.  Rows of the
 * still-running transaction are left alone (they are "in progress").
 */
void
tempcat_abort_subtransaction(void)
{
	tempcat_resolve_pending();
}

/* ----------------------------------------------------------------
 * Catalog DML
 * ----------------------------------------------------------------
 */

static Oid
tempcat_getoid(HeapTuple tup, TupleDesc desc, AttrNumber attno)
{
	bool		isnull;
	Datum		value = heap_getattr(tup, attno, desc, &isnull);

	return isnull ? InvalidOid : DatumGetObjectId(value);
}

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
	 * pg_shdepend: rows of this database recording that a temporary object
	 * of ours is owned by, or grants privileges to, a role.  The role is
	 * published instead, for DROP ROLE in other sessions to see.
	 */
	if (relid == SharedDependRelationId)
		return tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid) == MyDatabaseId &&
			tempcat_getoid(tup, desc, Anum_pg_shdepend_refclassid) == AuthIdRelationId &&
			tempcat_owner_is_temp(tup, desc, def->owner, def->ownerclass,
								  def->ownerclassattr) &&
			tempcat_publish_role(tempcat_getoid(tup, desc, Anum_pg_shdepend_refobjid));

	if (relid == ConstraintRelationId &&
		!OidIsValid(tempcat_getoid(tup, desc, Anum_pg_constraint_conrelid)))
	{
		/* domain constraint */
		if (!tempcat_owns_oid(TypeRelationId,
							  tempcat_getoid(tup, desc, Anum_pg_constraint_contypid)))
			return false;
	}
	else if (!tempcat_owner_is_temp(tup, desc, def->owner,
									def->ownerclass, def->ownerclassattr))
		return false;

	if (def->owner2 != 0 &&
		!tempcat_owner_is_temp(tup, desc, def->owner2,
							   def->owner2class, def->owner2classattr))
		return false;

	return true;
}

/*
 * Insert a virtual row.  Returns false, changing nothing, if the session's
 * area is full; the caller then stores the row on disk.  Scans merge both
 * sources, so where a row lives does not affect what anybody sees.
 */
bool
tempcat_insert(Relation rel, HeapTuple tup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	TransactionId xid;
	CommandId	cid;
	dsa_pointer ep;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader());

	/* No area (DSM space is short): the row goes to disk. */
	if (!tempcat_attach(true))
		return false;

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	ep = tempcat_entry_create(catidx, tup, RelationGetDescr(rel), xid, cid);
	if (!DsaPointerIsValid(ep) && tempcat_reclaim_dead() > 0)
		ep = tempcat_entry_create(catidx, tup, RelationGetDescr(rel), xid, cid);
	if (DsaPointerIsValid(ep))
		tempcat_add_pending(ep);
	LWLockRelease(&tc_root->lock);

	if (!DsaPointerIsValid(ep))
		return false;

	tempcat_set_caller_tuple(tup, rel, TC_ADDR(ep));
	if (RelationGetRelid(rel) == RelationRelationId)
		tempcat_cover_pg_class_row(tup->t_data);

	CacheInvalidateHeapTuple(rel, tup, NULL);
	return true;
}

static TempcatEntry *
tempcat_lookup_tid(Relation rel, ItemPointer tid, dsa_pointer *ep)
{
	if (!tempcat_attach(false))
		elog(ERROR, "tempcat: no in-memory catalog for virtual tuple (%u,%u)",
			 ItemPointerGetBlockNumberNoCheck(tid), tid->ip_posid);

	*ep = tempcat_find_tid(tid);
	if (!DsaPointerIsValid(*ep))
		elog(ERROR, "tempcat: virtual tuple (%u,%u) of \"%s\" not found",
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

/* Set xmax on an entry.  Caller holds the lock and has checked it. */
static void
tempcat_mark_deleted(TempcatEntry *e, dsa_pointer ep,
					 TransactionId xid, CommandId cid)
{
	e->xmax = xid;
	e->cmax = cid;
	tempcat_add_pending(ep);
}

/*
 * Replace a virtual row by a new version.  Returns false, changing nothing,
 * if the area is full; see tempcat_insert().
 */
bool
tempcat_update(Relation rel, ItemPointer otid, HeapTuple newtup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	dsa_pointer oldep;
	dsa_pointer newep;
	TempcatEntry *olde;
	HeapTuple	oldtup;
	TransactionId xid;
	CommandId	cid;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader());

	olde = tempcat_lookup_tid(rel, otid, &oldep);
	tempcat_check_deletable(olde);
	oldtup = tempcat_entry_copy(olde, RelationGetRelid(rel));

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	newep = tempcat_entry_create(catidx, newtup, RelationGetDescr(rel), xid, cid);
	if (!DsaPointerIsValid(newep) && tempcat_reclaim_dead() > 0)
		newep = tempcat_entry_create(catidx, newtup, RelationGetDescr(rel), xid, cid);
	if (!DsaPointerIsValid(newep))
	{
		LWLockRelease(&tc_root->lock);
		heap_freetuple(oldtup);
		return false;
	}
	tempcat_mark_deleted(olde, oldep, xid, cid);
	tempcat_add_pending(newep);
	LWLockRelease(&tc_root->lock);

	tempcat_set_caller_tuple(newtup, rel, TC_ADDR(newep));
	if (RelationGetRelid(rel) == RelationRelationId)
	{
		tempcat_cover_pg_class_row(newtup->t_data);
		tc_horizon_dirty = true;
	}

	CacheInvalidateHeapTuple(rel, oldtup, newtup);
	heap_freetuple(oldtup);
	return true;
}

void
tempcat_delete(Relation rel, ItemPointer tid)
{
	dsa_pointer ep;
	TempcatEntry *e;
	HeapTuple	oldtup;
	TransactionId xid;
	CommandId	cid;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, tid, &ep);
	tempcat_check_deletable(e);
	oldtup = tempcat_entry_copy(e, RelationGetRelid(rel));

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

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

/*
 * Collect the visible virtual rows matching the scan keys.  Must be called
 * while the keys still refer to heap attribute numbers, i.e. before
 * systable_beginscan() converts them to index column numbers.
 *
 * Returns NULL if there can be no virtual rows for this catalog.
 */
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
		int			cmp;

		if (anull || bnull)
		{
			if (anull && bnull)
				continue;
			cmp = anull ? 1 : -1;	/* NULLS LAST */
		}
		else
			cmp = DatumGetInt32(FunctionCall2Coll(scan->procs[i],
												  scan->irel->rd_indcollation[i],
												  ad, bd));
		if (scan->irel->rd_indoption[i] & INDOPTION_DESC)
			cmp = -cmp;
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

	catidx = tempcat_catalog_index(RelationGetRelid(rel));
	if (catidx < 0)
		return NULL;
	if (!tempcat_attach(false))
		return NULL;

	cat = &tc_root->catalogs[catidx];
	if (cat->nentries == 0)
		return NULL;

	def = &tempcat_catalogs[catidx];
	desc = RelationGetDescr(rel);

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
	scan->on_virtual = false;
	scan->irel = irel;
	scan->heapdesc = desc;
	scan->nidxkeys = 0;
	scan->procs = NULL;
	scan->disk_tuple = NULL;
	scan->disk_done = false;
	scan->direction = 0;
	maxtuples = 8;
	scan->tuples = palloc(sizeof(HeapTuple) * maxtuples);

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
			scan->tuples = repalloc(scan->tuples, sizeof(HeapTuple) * maxtuples);
		}
		scan->tuples[scan->ntuples++] = tempcat_entry_copy(e, RelationGetRelid(rel));
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
 * Return the next row: virtual rows merged with the on-disk rows that
 * fetch_disk() returns.  For scans through an index both come in index
 * order (backward if !forward, for ordered scans); otherwise the virtual
 * rows come first.
 */
HeapTuple
tempcat_getnext_dir(TempcatScan scan, bool forward,
					HeapTuple (*fetch_disk) (void *), void *arg)
{
	int			dir = forward ? 1 : -1;
	HeapTuple	vtup;

	if (scan->irel == NULL)
	{
		if (scan->next < scan->ntuples)
		{
			scan->on_virtual = true;
			return scan->tuples[scan->next++];
		}
		scan->on_virtual = false;
		return fetch_disk(arg);
	}

	if (scan->direction == 0)
		scan->direction = dir;
	else if (scan->direction != dir)
		elog(ERROR, "cannot change the direction of a catalog scan that includes in-memory temporary objects");

	if (scan->disk_tuple == NULL && !scan->disk_done)
	{
		scan->disk_tuple = fetch_disk(arg);
		if (scan->disk_tuple == NULL)
			scan->disk_done = true;
	}

	vtup = tempcat_scan_peek(scan, forward);
	if (vtup != NULL &&
		(scan->disk_tuple == NULL ||
		 tempcat_compare(scan, vtup, scan->disk_tuple) * dir <= 0))
	{
		scan->next++;
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

bool
tempcat_scan_on_virtual(TempcatScan scan)
{
	return scan != NULL && scan->on_virtual;
}

void
tempcat_endscan(TempcatScan scan)
{
	int			i;

	for (i = 0; i < scan->ntuples; i++)
		pfree(scan->tuples[i]);
	pfree(scan->tuples);
	if (scan->procs)
		pfree(scan->procs);
	pfree(scan);
}

/* ----------------------------------------------------------------
 * Objects of other sessions
 * ----------------------------------------------------------------
 */

typedef enum TempcatObjectLookup
{
	TCO_FOUND,
	TCO_MISSING,
	TCO_UNKNOWN					/* cannot look up objects of this class */
} TempcatObjectLookup;

/*
 * Look an object up by OID in its catalog, with the catalog snapshot (so
 * our own in-memory rows count, other sessions' do not).
 */
static TempcatObjectLookup
tempcat_object_lookup(Oid classId, Oid objectId)
{
	Relation	rel;
	HeapTuple	tup;
	bool		found;

	if (classId == AttrDefaultRelationId)
	{
		/* not in objectaddress.c's ObjectProperty in this version */
		SysScanDesc scan;
		ScanKeyData key;

		rel = table_open(AttrDefaultRelationId, AccessShareLock);
		ScanKeyInit(&key, Anum_pg_attrdef_oid, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(objectId));
		scan = systable_beginscan(rel, AttrDefaultOidIndexId, true, NULL, 1, &key);
		found = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);
		table_close(rel, AccessShareLock);
		return found ? TCO_FOUND : TCO_MISSING;
	}

	if (!is_objectclass_supported(classId))
		return TCO_UNKNOWN;

	rel = table_open(classId, AccessShareLock);
	tup = get_catalog_object_by_oid(rel, get_object_attnum_oid(classId), objectId);
	found = HeapTupleIsValid(tup);
	table_close(rel, AccessShareLock);
	return found ? TCO_FOUND : TCO_MISSING;
}

/*
 * Is this an in-memory temporary object we cannot see?
 *
 * On-disk dependency rows may point at temporary objects of other sessions
 * (e.g. a temporary table column of a regular type).  Their catalog rows
 * live in the other session's memory, so callers that want to describe or
 * drop them must handle their absence.
 */
bool
tempcat_object_missing(const ObjectAddress *object)
{
	if (!IsTempcatOid(object->objectId))
		return false;
	return tempcat_object_lookup(object->classId, object->objectId) == TCO_MISSING;
}

/*
 * On-disk rows of other sessions' temporary objects
 *
 * Rows of in-memory temporary objects can still be on disk: dependencies on
 * ordinary objects, and rows that went to disk when the session's area was
 * full.  To another session they point at objects that do not exist, which
 * is what catalog consistency checks look for.  gpcheckcat therefore sets
 * gp_temp_memory_catalog_hide_others, which hides from SQL scans the on-disk
 * rows whose owner column holds a reserved OID within the bounds published
 * by a live session of the row's database: such a catalog looks as if the
 * other sessions had no temporary objects.
 *
 * The bounds are approximate: ordinary objects with reserved OIDs (see the
 * file header) between them are hidden too.
 */
typedef struct TempcatLiveRange
{
	Oid			databaseId;
	Oid			lo;
	Oid			hi;
} TempcatLiveRange;

static TempcatLiveRange *tc_live = NULL;
static int	tc_nlive = 0;
static LocalTransactionId tc_live_lxid = InvalidLocalTransactionId;
static CommandId tc_live_cid = InvalidCommandId;

/* Collect the published bounds, once per command. */
static void
tempcat_collect_live_ranges(void)
{
	CommandId	cid = GetCurrentCommandId(false);
	int			i;

	if (tc_live != NULL && tc_live_lxid == MyProc->lxid && tc_live_cid == cid)
		return;

	if (tc_live == NULL)
		tc_live = MemoryContextAlloc(TopMemoryContext,
									 sizeof(TempcatLiveRange) * TempcatShared->nprocs);
	tc_nlive = 0;
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];
		Oid			lo;
		Oid			hi;
		Oid			db;

		if (i == MyProc->pgprocno)
			continue;
		lo = pg_atomic_read_u32(&st->ownedMin);
		hi = pg_atomic_read_u32(&st->ownedMax);
		db = st->databaseId;
		if (!OidIsValid(lo) || !OidIsValid(hi) || !OidIsValid(db))
			continue;
		tc_live[tc_nlive].databaseId = db;
		tc_live[tc_nlive].lo = lo;
		tc_live[tc_nlive].hi = hi;
		tc_nlive++;
	}
	tc_live_lxid = MyProc->lxid;
	tc_live_cid = cid;
}

/*
 * Is the OID within the bounds a live session of the database publishes
 * right now?  For the leftover sweep, which must decide with the bounds read
 * after it saw the row: a session publishes them before inserting rows, so
 * any committed row of a live session is covered.  (The sweep runs while
 * sessions keep creating objects, so bounds collected before it started
 * are too old.)
 */
static bool
tempcat_oid_live_now(Oid oid, Oid dbid)
{
	int			i;

	if (!IsTempcatOid(oid))
		return false;
	pg_read_barrier();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];
		Oid			lo;
		Oid			hi;

		/* (our own objects count: the sweep can run in a session with some) */
		if (st->databaseId != dbid)
			continue;
		lo = pg_atomic_read_u32(&st->ownedMin);
		hi = pg_atomic_read_u32(&st->ownedMax);
		if (OidIsValid(lo) && oid >= lo && oid <= hi)
			return true;
	}
	return false;
}

static bool
tempcat_oid_live_elsewhere(Oid oid, Oid dbid)
{
	int			i;

	if (!IsTempcatOid(oid))
		return false;
	for (i = 0; i < tc_nlive; i++)
	{
		if (tc_live[i].databaseId == dbid &&
			oid >= tc_live[i].lo && oid <= tc_live[i].hi)
			return true;
	}
	return false;
}

/*
 * Should a SQL scan skip this on-disk row of 'rel'?  Only when
 * gp_temp_memory_catalog_hide_others is set; see above.
 */
bool
tempcat_hide_disk_row(Relation rel, HeapTuple tup)
{
	Oid			relid = RelationGetRelid(rel);
	TupleDesc	desc = RelationGetDescr(rel);
	const TempcatCatalogDef *def;
	int			catidx;
	Oid			dbid = MyDatabaseId;
	Oid			owner;

	if (!gp_temp_memory_catalog_hide_others || TempcatShared == NULL)
		return false;
	catidx = tempcat_catalog_index(relid);
	if (catidx < 0)
		return false;
	def = &tempcat_catalogs[catidx];

	tempcat_collect_live_ranges();
	if (tc_nlive == 0)
		return false;

	if (relid == SharedDependRelationId)
		dbid = tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid);

	owner = tempcat_getoid(tup, desc, def->owner);
	if (relid == ConstraintRelationId && !OidIsValid(owner))
		owner = tempcat_getoid(tup, desc, Anum_pg_constraint_contypid);

	return tempcat_oid_live_elsewhere(owner, dbid) ||
		(def->owner2 != 0 &&
		 tempcat_oid_live_elsewhere(tempcat_getoid(tup, desc, def->owner2), dbid));
}

bool
tempcat_hide_disk_slot(Relation rel, TupleTableSlot *slot)
{
	return tempcat_hide_disk_row(rel, ExecFetchSlotHeapTuple(slot, false, NULL));
}

/* ----------------------------------------------------------------
 * Leftovers of crashed sessions
 *
 * A session that ends normally drops its temporary objects, removing their
 * on-disk rows too: dependencies on ordinary objects (pg_depend,
 * pg_shdepend) and rows that went to disk because the in-memory area was
 * full.  A crashed session leaves them behind, pointing at objects whose
 * main catalog rows were only in memory.  (Their files are removed when the
 * node restarts after the crash.)
 *
 * The first session of a database that starts keeping temporary objects in
 * memory after the node started removes such rows: at that point no live
 * session of the database has in-memory objects, so on-disk rows that refer
 * to reserved OIDs whose objects do not exist on disk are garbage.
 * Ordinary objects with reserved OIDs (see the file header) exist on disk
 * and are left alone.
 * ----------------------------------------------------------------
 */

static bool
tempcat_db_swept(Oid dbid)
{
	bool		found = false;
	int			i;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nswept; i++)
	{
		if (TempcatShared->swept[i] == dbid)
		{
			found = true;
			break;
		}
	}
	SpinLockRelease(&TempcatShared->mutex);
	return found;
}

/* Ask for another sweep of the database. */
static void
tempcat_unmark_swept(Oid dbid)
{
	int			i;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nswept; i++)
	{
		if (TempcatShared->swept[i] == dbid)
		{
			TempcatShared->swept[i] = TempcatShared->swept[--TempcatShared->nswept];
			break;
		}
	}
	SpinLockRelease(&TempcatShared->mutex);
}

static void
tempcat_mark_swept(Oid dbid)
{
	/* If the list is full, databases beyond it are swept every time. */
	SpinLockAcquire(&TempcatShared->mutex);
	if (TempcatShared->nswept < TEMPCAT_MAX_SWEPT_DBS)
		TempcatShared->swept[TempcatShared->nswept++] = dbid;
	SpinLockRelease(&TempcatShared->mutex);
}

/*
 * Delete an on-disk leftover row, unless somebody else deleted or updated it
 * meanwhile (its session at exit, or another sweep): the sweep runs inside
 * users' commands, which must not fail because of it.  Rows in our own
 * memory are never leftovers.
 */
static bool
tempcat_sweep_delete(Relation rel, HeapTuple tup)
{
	TM_Result	result;
	TM_FailureData tmfd;

	if (IsTempcatTid(&tup->t_self))
		return false;

	result = heap_delete(rel, &tup->t_self, GetCurrentCommandId(true),
						 InvalidSnapshot, true /* wait for commit */ ,
						 &tmfd, false /* changingPart */ );
	switch (result)
	{
		case TM_Ok:
			return true;
		case TM_SelfModified:
		case TM_Updated:
		case TM_Deleted:
			return false;
		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			return false;		/* keep compiler quiet */
	}
}

/* Does the object exist in the on-disk catalogs? */
static bool
tempcat_disk_object_exists(Oid classId, Oid objectId)
{
	switch (classId)
	{
		case RelationRelationId:
			return SearchSysCacheExists1(RELOID, ObjectIdGetDatum(objectId));
		case TypeRelationId:
			return SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(objectId));
		case NamespaceRelationId:
			return SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(objectId));
		default:
			break;
	}

	/* Keep rows of object classes we cannot look up. */
	return tempcat_object_lookup(classId, objectId) != TCO_MISSING;
}

/*
 * Delete the rows of 'catalog' whose column 'attr' holds a reserved OID of
 * an object (of class 'objclass', or of the class in column 'classattr')
 * that does not exist on disk.  For pg_shdepend, only rows of this
 * database.  For pg_constraint, 'domain' selects domain constraints
 * (conrelid = 0, owner in contypid).
 */
static int
tempcat_sweep_catalog(Oid catalog, AttrNumber attr, Oid objclass,
					  AttrNumber classattr, bool domain)
{
	Relation	rel;
	TupleDesc	desc;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;
	int			ndeleted = 0;

	rel = table_open(catalog, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	ScanKeyInit(&key, attr, BTGreaterEqualStrategyNumber, F_OIDGE,
				ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Oid			objid = tempcat_getoid(tup, desc, attr);
		Oid			cls = classattr != 0 ? tempcat_getoid(tup, desc, classattr) : objclass;

		if (!IsTempcatOid(objid))
			continue;
		if (catalog == SharedDependRelationId &&
			tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid) != MyDatabaseId)
			continue;
		if (catalog == ConstraintRelationId &&
			OidIsValid(tempcat_getoid(tup, desc, Anum_pg_constraint_conrelid)) == domain)
			continue;
		if (tempcat_oid_live_now(objid, MyDatabaseId) ||
			tempcat_disk_object_exists(cls, objid))
			continue;

		if (tempcat_sweep_delete(rel, tup))
			ndeleted++;
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	/* Let later passes (e.g. pg_depend by refobjid) see the deletions. */
	CommandCounterIncrement();

	return ndeleted;
}

/*
 * Delete objects that went to disk when the in-memory area was full but
 * whose namespace did not: temporary namespaces with reserved OIDs, and
 * relations and types with reserved OIDs in a namespace that is gone.
 */
static int
tempcat_sweep_objects(void)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;
	int			ndeleted = 0;

	rel = table_open(NamespaceRelationId, RowExclusiveLock);
	ScanKeyInit(&key, Anum_pg_namespace_oid, BTGreaterEqualStrategyNumber,
				F_OIDGE, ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_namespace form = (Form_pg_namespace) GETSTRUCT(tup);

		if (IsTempcatOid(form->oid) &&
			!tempcat_oid_live_now(form->oid, MyDatabaseId) &&
			(strncmp(NameStr(form->nspname), "pg_temp_", 8) == 0 ||
			 strncmp(NameStr(form->nspname), "pg_toast_temp_", 14) == 0))
		{
			if (tempcat_sweep_delete(rel, tup))
				ndeleted++;
		}
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
	CommandCounterIncrement();

	rel = table_open(RelationRelationId, RowExclusiveLock);
	ScanKeyInit(&key, Anum_pg_class_oid, BTGreaterEqualStrategyNumber,
				F_OIDGE, ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_class form = (Form_pg_class) GETSTRUCT(tup);

		if (IsTempcatOid(form->oid) &&
			!tempcat_oid_live_now(form->oid, MyDatabaseId) &&
			!tempcat_disk_object_exists(NamespaceRelationId, form->relnamespace))
		{
			if (tempcat_sweep_delete(rel, tup))
				ndeleted++;
		}
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	rel = table_open(TypeRelationId, RowExclusiveLock);
	ScanKeyInit(&key, Anum_pg_type_oid, BTGreaterEqualStrategyNumber,
				F_OIDGE, ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_type form = (Form_pg_type) GETSTRUCT(tup);

		if (IsTempcatOid(form->oid) &&
			!tempcat_oid_live_now(form->oid, MyDatabaseId) &&
			!tempcat_disk_object_exists(NamespaceRelationId, form->typnamespace))
		{
			if (tempcat_sweep_delete(rel, tup))
				ndeleted++;
		}
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
	CommandCounterIncrement();

	return ndeleted;
}

/* One pass over the catalogs whose rows are owned by another object. */
static int
tempcat_sweep_owned_rows(void)
{
	int			ndeleted = 0;
	int			i;

	for (i = 0; i < TEMPCAT_NCATALOGS; i++)
	{
		const TempcatCatalogDef *def = &tempcat_catalogs[i];

		switch (def->relid)
		{
			case RelationRelationId:
			case TypeRelationId:
			case NamespaceRelationId:
			case DependRelationId:
			case DescriptionRelationId:
			case SharedDependRelationId:
				continue;
			default:
				break;
		}
		ndeleted += tempcat_sweep_catalog(def->relid, def->owner, def->ownerclass,
										  def->ownerclassattr, false);
		if (def->relid == ConstraintRelationId)
			ndeleted += tempcat_sweep_catalog(ConstraintRelationId,
											  Anum_pg_constraint_contypid,
											  TypeRelationId, 0, true);
	}
	return ndeleted;
}

static void
tempcat_sweep_orphans(void)
{
	int			ndeleted;
	int			nbefore;
	int			pass = 0;

	ndeleted = tempcat_sweep_objects();

	/*
	 * Rows owned by objects that are gone, except dependency-like rows.
	 * Repeat while something goes away: owners can be such rows themselves
	 * (e.g. pg_aggregate rows of pg_proc rows of a namespace that is gone).
	 */
	do
	{
		nbefore = ndeleted;
		ndeleted += tempcat_sweep_owned_rows();
	} while (ndeleted != nbefore && ++pass < 4);

	/* Then rows pointing at any object that is gone. */
	ndeleted += tempcat_sweep_catalog(DependRelationId, Anum_pg_depend_objid,
									  InvalidOid, Anum_pg_depend_classid, false);
	ndeleted += tempcat_sweep_catalog(DependRelationId, Anum_pg_depend_refobjid,
									  InvalidOid, Anum_pg_depend_refclassid, false);
	ndeleted += tempcat_sweep_catalog(DescriptionRelationId, Anum_pg_description_objoid,
									  InvalidOid, Anum_pg_description_classoid, false);
	ndeleted += tempcat_sweep_catalog(SharedDependRelationId, Anum_pg_shdepend_objid,
									  InvalidOid, Anum_pg_shdepend_classid, false);
	CommandCounterIncrement();

	if (ndeleted > 0)
		ereport(LOG,
				(errmsg("removed %d catalog rows left by crashed sessions' temporary objects",
						ndeleted)));
}

static void
tempcat_sweep_if_needed(bool allow_force)
{
	bool		force = false;

#ifdef FAULT_INJECTOR
	/* Test hook: sweep even if this database was swept already. */
	if (allow_force &&
		SIMPLE_FAULT_INJECTOR("tempcat_force_sweep") == FaultInjectorTypeSkip)
		force = true;
#endif

	if (!force && tempcat_db_swept(MyDatabaseId))
		return;

	/*
	 * One sweeper at a time, and nobody waits for it: if another session is
	 * sweeping, go on (its sweep may not have removed everything yet, which
	 * at worst makes a DROP fail like before).  Waiting, with the lock held
	 * until commit, deadlocked distributed transactions that took it on the
	 * segments in different orders; so release it right after the sweep.
	 */
	if (!ConditionalLockDatabaseObject(DatabaseRelationId, MyDatabaseId,
									   TEMPCAT_SWEEP_LOCK_SUBID, ExclusiveLock))
		return;

	if (force || !tempcat_db_swept(MyDatabaseId))
	{
		tempcat_sweep_orphans();
		tempcat_mark_swept(MyDatabaseId);

		/* If the deletions do not commit, sweep again; see below. */
		tc_sweep_xid = GetCurrentTransactionIdIfAny();
		tc_sweep_db = MyDatabaseId;
	}

	UnlockDatabaseObject(DatabaseRelationId, MyDatabaseId,
						 TEMPCAT_SWEEP_LOCK_SUBID, ExclusiveLock);
}

/*
 * Called by autovacuum in each database it visits, so that leftovers of
 * crashed sessions do not wait for a session to keep temporary objects in
 * memory (they make pg_dump fail, and keep ordinary objects from being
 * dropped).  Must be called in a transaction.
 */
void
tempcat_autovacuum_sweep(void)
{
	if (TempcatShared == NULL || IsBinaryUpgrade)
		return;
#ifdef FAULT_INJECTOR
	/* Test hook: leave leftovers to other sweeps. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_skip_autovacuum_sweep") == FaultInjectorTypeSkip)
		return;
#endif
	tempcat_sweep_if_needed(false);
}

/*
 * Called before dropping objects (and roles): leftover dependencies of
 * temporary objects that no longer exist would otherwise block the drop
 * until the next sweep.  A no-op unless the database needs a sweep, i.e.
 * after the node started or a session could not clean up at exit.
 */
void
tempcat_sweep_before_drop(void)
{
	if (TempcatShared == NULL || IsBinaryUpgrade || !IsUnderPostmaster ||
		RecoveryInProgress())
		return;
	tempcat_sweep_if_needed(false);
}

/* ----------------------------------------------------------------
 * SQL-level catalog scans
 *
 * Catalog queries see the virtual rows through sequential scans, which
 * return them after the on-disk rows (nodeSeqscan.c), and plain index scans,
 * which merge them in index order (nodeIndexscan.c).  Index-only and bitmap
 * scans cannot return them, so once a session keeps temporary objects in
 * memory, the planner does not use those for the catalogs that can hold
 * virtual rows.  ORCA does not plan catalog queries.
 * ----------------------------------------------------------------
 */
bool
tempcat_restrict_catalog_index_scans(Oid relid)
{
	return (tc_activated || gp_temp_memory_catalog_hide_others) &&
		tempcat_catalog_index(relid) >= 0;
}

/* A scan without virtual rows, for scans that filter on-disk rows only. */
static TempcatScan
tempcat_empty_scan(void)
{
	TempcatScan scan = palloc0(sizeof(struct TempcatScanData));

	scan->tuples = palloc(sizeof(HeapTuple));
	return scan;
}

/* Virtual rows of 'rel' visible to the query snapshot, or NULL if none. */
TempcatScan
tempcat_begin_sql_scan(Relation rel, Snapshot snapshot)
{
	/* Developer option: look at the on-disk catalog only */
	if (gp_temp_memory_catalog_disk_only)
		return NULL;
	return tempcat_beginscan(rel, NULL, snapshot, 0, NULL);
}

HeapTuple
tempcat_next_virtual(TempcatScan scan)
{
	if (scan->next < scan->ntuples)
		return scan->tuples[scan->next++];
	return NULL;
}

/*
 * Virtual rows for an index scan, sorted in index order, or NULL if none.
 * Simple index scan keys are applied (translated to heap attributes); the
 * caller filters the rows by the complete index quals.
 */
TempcatScan
tempcat_begin_index_sql_scan(Relation rel, Relation irel, Snapshot snapshot,
							 ScanKey indexkeys, int nindexkeys)
{
	ScanKey		heapkeys;
	int			nheapkeys = 0;
	int			i;
	TempcatScan scan;

	if (tempcat_catalog_index(RelationGetRelid(rel)) < 0)
		return NULL;

	/*
	 * Developer options: look at the on-disk catalog only, and possibly
	 * filter it (the caller merges an empty scan, which does that).
	 */
	if (gp_temp_memory_catalog_disk_only)
		return gp_temp_memory_catalog_hide_others ? tempcat_empty_scan() : NULL;

	heapkeys = palloc(sizeof(ScanKeyData) * Max(nindexkeys, 1));
	for (i = 0; i < nindexkeys; i++)
	{
		ScanKey		k = &indexkeys[i];

		if (k->sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_ROW_MEMBER |
						   SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL |
						   SK_ORDER_BY))
			continue;
		if (k->sk_attno < 1 || k->sk_attno > IndexRelationGetNumberOfKeyAttributes(irel))
			continue;
		heapkeys[nheapkeys] = *k;
		heapkeys[nheapkeys].sk_attno = irel->rd_index->indkey.values[k->sk_attno - 1];
		nheapkeys++;
	}

	scan = tempcat_beginscan(rel, irel, snapshot, nheapkeys, heapkeys);
	if (scan == NULL && gp_temp_memory_catalog_hide_others)
		scan = tempcat_empty_scan();
	return scan;
}

/* Drop the rows for which keep() returns false. */
void
tempcat_scan_filter(TempcatScan scan, bool (*keep) (HeapTuple, void *), void *arg)
{
	int			i;
	int			n = 0;

	for (i = 0; i < scan->ntuples; i++)
	{
		if (keep(scan->tuples[i], arg))
			scan->tuples[n++] = scan->tuples[i];
		else
			pfree(scan->tuples[i]);
	}
	scan->ntuples = n;
}

/* Next row in the given direction without consuming it, or NULL. */
HeapTuple
tempcat_scan_peek(TempcatScan scan, bool forward)
{
	if (scan->next >= scan->ntuples)
		return NULL;
	return scan->tuples[forward ? scan->next : scan->ntuples - 1 - scan->next];
}

void
tempcat_scan_advance(TempcatScan scan)
{
	scan->next++;
}

int
tempcat_scan_compare(TempcatScan scan, HeapTuple a, HeapTuple b)
{
	return tempcat_compare(scan, a, b);
}

/*
 * Fetch a virtual row by TID for a TID scan, if visible to the snapshot.
 * Returns a palloc'd copy, or NULL.
 */
HeapTuple
tempcat_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	dsa_pointer ep;
	HeapTuple	result = NULL;
	bool		reader;

	if (gp_temp_memory_catalog_disk_only ||
		tempcat_catalog_index(RelationGetRelid(rel)) < 0 ||
		!tempcat_attach(false))
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
